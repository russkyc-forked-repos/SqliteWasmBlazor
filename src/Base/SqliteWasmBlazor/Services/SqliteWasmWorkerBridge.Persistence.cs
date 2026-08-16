// SqliteWasmBlazor - Minimal EF Core compatible provider
// MIT License

using System.Security.Cryptography;
using System.Text.Json;
using MessagePack;

namespace SqliteWasmBlazor;

// Persistence partial: single-DB import/export (opaque or plain, with REKEY/
// ENCRYPT and asymmetric verify+import variants) and bulk row import.
internal sealed partial class SqliteWasmWorkerBridge
{
    /// <summary>
    /// Import a raw .db file into OPFS SAHPool storage.
    ///
    /// Auto-detects ciphertext vs plaintext by inspecting the first 16 bytes:
    /// if they are <c>"SQLite format 3\0"</c>, the input is treated as a plain
    /// SQLite file (normal path with byte-18 WAL-mode patch). Otherwise the
    /// input is treated as opaque ciphertext of a PRF-VFS-encrypted DB —
    /// both the header validation and the byte-18 patch are skipped because
    /// they would corrupt the AEAD tag on slot 0.
    ///
    /// Opaque imports are subject to refuse-on-existing + verify-on-write:
    /// the worker rejects writes over an existing DB at this path
    /// (<see cref="DiskImportResult.EXISTING_DB_REFUSED"/>) and, when an
    /// encryption key is registered, AEAD-tests slot 0 of the freshly written
    /// DB. A failed verify rolls back the import (unlinks the file) and
    /// returns <see cref="DiskImportResult.WRONG_KEY"/>.
    /// </summary>
    public async Task<DiskImportResult> ImportDatabaseAsync(
        string databaseName,
        byte[] data,
        CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        var opaque = data.Length < 16 || !data.AsSpan(0, 16).SequenceEqual(SqliteHeaderMagic);

        var requestId = Interlocked.Increment(ref _nextRequestId);
        var tcs = new TaskCompletionSource<SqlQueryResult>();

        _pendingRequests[requestId] = tcs;

        try
        {
            await using var registration = cancellationToken.Register(() =>
            {
                _pendingRequests.TryRemove(requestId, out _);
                tcs.TrySetCanceled();
            });

            var metadataJson = JsonSerializer.Serialize(new
            {
                id = requestId,
                data = new
                {
                    type = "importDb",
                    database = databaseName,
                    opaque,
                }
            });

            SendBinaryToWorker(data.AsSpan(), metadataJson);

            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutCts.CancelAfter(60000);

            SqlQueryResult result;
            try
            {
                result = await tcs.Task.WaitAsync(timeoutCts.Token);
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                throw new TimeoutException("Import database operation timed out after 60 seconds.");
            }

            // Worker closes the DB during import (no-op when the DB wasn't
            // open or when the import was refused before close).
            _openDatabases.Remove(databaseName);

            // Worker encodes the import outcome in rowsAffected (same
            // tri-state channel SetEncryptionKeyAsync uses):
            // 0 = OK, 1 = WRONG_KEY (rolled back), 2 = EXISTING_DB_REFUSED.
            return result.RowsAffected switch
            {
                0 => DiskImportResult.OK,
                1 => DiskImportResult.WRONG_KEY,
                2 => DiskImportResult.EXISTING_DB_REFUSED,
                var other => throw new InvalidOperationException(
                    $"Worker returned unexpected import outcome code {other}"),
            };
        }
        catch
        {
            _pendingRequests.TryRemove(requestId, out _);
            throw;
        }
    }

    /// <inheritdoc />
    public Task<byte[]> ExportDatabaseAsync(
        string databaseName,
        CancellationToken cancellationToken = default)
        => SendRawBinaryRequestAsync(
            databaseName,
            new { type = "exportDb", database = databaseName, mode = "verbatim" },
            "Export verbatim",
            cancellationToken);

    /// <inheritdoc />
    public async Task ExportDatabaseToDownloadAsync(
        string databaseName,
        string filename,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(databaseName))
        {
            throw new ArgumentException(
                "databaseName must be non-empty.", nameof(databaseName));
        }
        if (string.IsNullOrWhiteSpace(filename))
        {
            throw new ArgumentException(
                "filename must be non-empty.", nameof(filename));
        }

        await EnsureInitializedAsync(cancellationToken);
        // On an Encrypted+Locked disk the worker has no globalKey and can't
        // decrypt slots to plain pages — refuse up front with the same
        // guard the SQL path uses instead of surfacing a worker slot-size
        // error.
        ThrowIfDiskLocked($"ExportDatabaseToDownload('{databaseName}')");

        var request = new { type = "exportDbToStaging", database = databaseName };
        var result = await SendRequestAsync(request, cancellationToken);
        if (string.IsNullOrEmpty(result.StagingFile))
        {
            throw new InvalidOperationException(
                "exportDbToStaging returned no staging file name.");
        }

        // Worker closed the DB for a consistent snapshot — mirror that in
        // the C#-side open set so the next use re-opens cleanly.
        _openDatabases.Remove(databaseName);

        var ok = await DownloadStagedExportAsync(result.StagingFile, filename);
        if (!ok)
        {
            throw new InvalidOperationException(
                "downloadStagedExport reported failure.");
        }
    }

    // Promoted from private → internal in plane-split Phase 1 so plane 2's
    // EncryptedSqliteWasmWorkerBridge in SqliteWasmBlazor.Crypto can drive
    // binary-payload round-trips through the same _pendingBinaryRequests map.
    internal async Task<byte[]> SendRawBinaryRequestAsync(
        string databaseName,
        object request,
        string opName,
        CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);

        var requestId = Interlocked.Increment(ref _nextRequestId);
        var tcs = new TaskCompletionSource<byte[]>();
        _pendingBinaryRequests[requestId] = tcs;

        try
        {
            await using var registration = cancellationToken.Register(() =>
            {
                _pendingBinaryRequests.TryRemove(requestId, out _);
                tcs.TrySetCanceled();
            });

            var requestJson = JsonSerializer.Serialize(new { id = requestId, data = request });
            SendToWorker(requestJson);

            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutCts.CancelAfter(60000);

            try
            {
                var result = await tcs.Task.WaitAsync(timeoutCts.Token);
                // Worker closes the DB during export for consistent snapshot.
                _openDatabases.Remove(databaseName);
                return result;
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                throw new TimeoutException($"{opName} operation timed out after 60 seconds.");
            }
        }
        catch
        {
            _pendingBinaryRequests.TryRemove(requestId, out _);
            throw;
        }
    }

    public async Task<int> ImportRowsAsync(
        string databaseName, byte[] data,
        CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        var requestId = Interlocked.Increment(ref _nextRequestId);
        var tcs = new TaskCompletionSource<SqlQueryResult>();
        _pendingRequests[requestId] = tcs;

        try
        {
            await using var registration = cancellationToken.Register(() =>
            {
                _pendingRequests.TryRemove(requestId, out _);
                tcs.TrySetCanceled();
            });

            var metadataJson = JsonSerializer.Serialize(new
            {
                id = requestId,
                data = new
                {
                    type = "importRows",
                    database = databaseName
                }
            });

            SendBinaryToWorker(data.AsSpan(), metadataJson);

            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutCts.CancelAfter(300_000);

            var result = await tcs.Task.WaitAsync(timeoutCts.Token);
            return result.RowsAffected;
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException("Row import timed out.");
        }
        finally
        {
            _pendingRequests.TryRemove(requestId, out _);
        }
    }
}
