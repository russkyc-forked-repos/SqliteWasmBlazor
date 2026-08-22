using Microsoft.EntityFrameworkCore;

namespace SqliteWasmBlazor.TestApp.TestInfrastructure.VfsEncryption;

/// <summary>
/// Streaming multi-DB <c>.dbs</c> envelope round-trip. The page uses the
/// envelope when ≥ 2 DBs are picked in the checkbox export selector;
/// this test exercises the import side end-to-end (worker pool wipe +
/// MessagePack envelope parse + chunked SAH writes per entry).
///
/// <para>
/// Building the envelope in C# avoids the export-side anchor-click
/// download which isn't reachable from in-page test code. Bytes for both
/// entries come from a single EF-populated DB exported through
/// <c>ExportDatabaseBytesAsync</c>; the
/// envelope packs them under two distinct names. After import, both
/// names must appear in the pool with byte-for-byte equal contents.
/// </para>
/// </summary>
internal sealed class DbsEnvelopeRoundTripTest
{
    private const int RowCount = 8;
    private const string EntryA = "DbsTest-A.db";
    private const string EntryB = "DbsTest-B.db";

    private readonly IDbContextFactory<PrfVfsTestContext> _factory;
    private readonly ISqliteWasmDatabaseService _databaseService;
    private readonly IEncryptedSqliteWasmDatabaseService _session;

    public string Name => "Plain_DbsEnvelope_StreamingImport_RoundTrip";

    public DbsEnvelopeRoundTripTest(
        IDbContextFactory<PrfVfsTestContext> factory,
        ISqliteWasmDatabaseService databaseService,
        IEncryptedSqliteWasmDatabaseService session)
    {
        _factory = factory;
        _databaseService = databaseService;
        _session = session;
    }

    public async ValueTask<string?> RunAsync()
    {
        await CleanupAsync();

        // ---- Populate the EF context, snapshot its plain bytes ------------
        await using (var ctx = await _factory.CreateDbContextAsync())
        {
            await ctx.Database.EnsureCreatedAsync();
            for (var i = 0; i < RowCount; i++)
            {
                ctx.Items.Add(new VfsTestItem
                {
                    Marker = $"dbs-{i}",
                    Payload = $"payload-{i}-{Guid.NewGuid():N}",
                });
            }
            await ctx.SaveChangesAsync();
        }

        byte[] plainBytes;
        try
        {
            plainBytes = await _databaseService.ExportDatabaseBytesAsync(PrfVfsTestContext.DatabaseName);
        }
        catch (Exception ex)
        {
            return $"FAIL[ExportDatabaseBytesAsync]: {ex.GetType().Name}: {ex.Message}";
        }
        if (plainBytes.Length == 0)
        {
            return "FAIL: ExportDatabaseBytesAsync returned empty bytes";
        }

        // ---- Wipe the pool so the importer has no pre-existing files ------
        await CleanupAsync();

        // ---- Build a .dbs envelope: array<2>[[name, bytes], [name, bytes]]
        var envelope = DbsEnvelopeWriter.Build(new[]
        {
            (EntryA, plainBytes),
            (EntryB, plainBytes),
        });

        // ---- Import via the streaming entry point -------------------------
        try
        {
            using var stream = new MemoryStream(envelope, writable: false);
            await _databaseService.ImportDatabasesFromStreamAsync(stream, envelope.Length);
        }
        catch (Exception ex)
        {
            return $"FAIL[ImportDatabasesFromStreamAsync]: {ex.GetType().Name}: {ex.Message}";
        }

        // ---- Verify both entries landed -----------------------------------
        var names = await _databaseService.ListDatabasesAsync();
        if (!names.Contains(EntryA))
        {
            return $"FAIL: pool missing '{EntryA}' after import (got [{string.Join(", ", names)}])";
        }
        if (!names.Contains(EntryB))
        {
            return $"FAIL: pool missing '{EntryB}' after import (got [{string.Join(", ", names)}])";
        }

        var a = await _databaseService.ExportDatabaseBytesAsync(EntryA);
        var b = await _databaseService.ExportDatabaseBytesAsync(EntryB);
        if (!a.AsSpan().SequenceEqual(plainBytes))
        {
            return $"FAIL[{EntryA}]: bytes differ ({a.Length} vs {plainBytes.Length})";
        }
        if (!b.AsSpan().SequenceEqual(plainBytes))
        {
            return $"FAIL[{EntryB}]: bytes differ ({b.Length} vs {plainBytes.Length})";
        }

        await CleanupAsync();
        return "OK";
    }

    private async Task CleanupAsync()
    {
        try { await _session.ResetPoolAsync(); } catch { }
        try
        {
            var names = await _databaseService.ListDatabasesAsync();
            foreach (var n in names)
            {
                try { await _databaseService.DeleteDatabaseAsync(n); } catch { }
            }
        }
        catch { }
    }
}
