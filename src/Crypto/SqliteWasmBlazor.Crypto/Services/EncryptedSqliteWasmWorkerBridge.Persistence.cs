// SqliteWasmBlazor.Crypto — encrypted plain-source import bridge
// MIT License

namespace SqliteWasmBlazor.Crypto.Services;

// Persistence partial: the plain-source-on-encrypted-disk import path used
// by the state-aware ImportAllDatabasesAsync (Encrypted+Unlocked branch).
// Rekey-on-write is owned by the worker's importDbPlain handler.
internal sealed partial class EncryptedSqliteWasmWorkerBridge
{
    /// <summary>
    /// Plain-source import onto an Encrypted+Unlocked disk. Ships raw plain
    /// SQLite bytes to the worker's <c>importDbPlain</c> handler, which
    /// re-encrypts every page under the registered globalKey via the
    /// chunked rekey path before atomic-promoting the temp slot.
    /// </summary>
    internal async Task<DiskImportResult> ImportPlainDatabaseAsync(
        string databaseName,
        byte[] plainBytes,
        CancellationToken cancellationToken = default)
    {
        SqlQueryResult result;
        try
        {
            result = await _bridge.PostBinaryAsync(
                new { type = "importDbPlain", database = databaseName },
                plainBytes,
                cancellationToken);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException("Import-plain database operation timed out.");
        }

        _bridge.MarkDatabaseClosed(databaseName);

        return result.RowsAffected switch
        {
            0 => DiskImportResult.OK,
            1 => DiskImportResult.WRONG_KEY,
            2 => DiskImportResult.EXISTING_DB_REFUSED,
            var other => throw new InvalidOperationException(
                $"Worker returned unexpected import-plain outcome code {other}"),
        };
    }
}
