using System.Security.Cryptography;
using Microsoft.EntityFrameworkCore;

namespace SqliteWasmBlazor.TestApp.TestInfrastructure.VfsEncryption;

/// <summary>
/// Streaming single-DB plain import round-trip — exercises the
/// IBrowserFile-shaped path that powers the page's <c>.db</c> picker
/// and the per-DB export affordance.
///
/// <para>
/// Two scenarios in one test:
/// </para>
/// <list type="number">
///   <item><b>Plain → Plain</b>: populate, export plain bytes, delete,
///         re-import via <see cref="ISqliteWasmDatabaseService.ImportDatabaseFromStreamAsync"/>,
///         verify rows.</item>
///   <item><b>Plain → Encrypted+Unlocked</b>: take the same plain bytes,
///         <see cref="IEncryptedSqliteWasmDatabaseService.EnterEncryptedAsync"/>
///         on an empty pool, then <see cref="ISqliteWasmDatabaseService.ImportDatabaseFromStreamAsync"/>
///         (worker rekey-on-write under the registered globalKey),
///         verify rows readable through the encrypted I/O path.</item>
/// </list>
///
/// <para>
/// The streaming download counterpart isn't exercised here — it goes
/// through anchor-click <c>&lt;a download&gt;</c> which isn't reachable
/// from in-page test code. The same chunked SAH primitives + bridge
/// streamHandler glue are shared with the import side, so the import
/// round-trip covers the worker / bridge / SAH plumbing end-to-end.
/// </para>
/// </summary>
internal sealed class SingleDbStreamingRoundTripTest
{
    private const int RowCount = 10;

    private readonly IDbContextFactory<PrfVfsTestContext> _factory;
    private readonly ISqliteWasmDatabaseService _databaseService;
    private readonly IEncryptedSqliteWasmDatabaseService _session;

    public string Name => "SingleDb_StreamingImport_PlainAndEncryptedRoundTrip";

    public SingleDbStreamingRoundTripTest(
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
        var dbName = PrfVfsTestContext.DatabaseName;
        await CleanupAsync(dbName);

        // ---- Phase 1: populate a Plain DB, snapshot its bytes ------------
        await using (var ctx = await _factory.CreateDbContextAsync())
        {
            await ctx.Database.EnsureCreatedAsync();
            for (var i = 0; i < RowCount; i++)
            {
                ctx.Items.Add(new VfsTestItem
                {
                    Marker = $"single-{i}",
                    Payload = $"payload-{i}-{Guid.NewGuid():N}",
                });
            }
            await ctx.SaveChangesAsync();
        }

        byte[] plainBytes;
        try
        {
            plainBytes = await _databaseService.ExportDatabaseAsync(dbName);
        }
        catch (Exception ex)
        {
            return $"FAIL: ExportDatabaseAsync threw {ex.GetType().Name}: {ex.Message}";
        }
        if (plainBytes.Length == 0)
        {
            return "FAIL: ExportDatabaseAsync returned empty bytes";
        }

        // ---- Phase 2: Plain → Plain streaming import round-trip ----------
        try
        {
            await _databaseService.DeleteDatabaseAsync(dbName);
        }
        catch (Exception ex)
        {
            return $"FAIL[plain:Delete]: {ex.GetType().Name}: {ex.Message}";
        }

        try
        {
            using var stream = new MemoryStream(plainBytes, writable: false);
            await _databaseService.ImportDatabaseFromStreamAsync(
                dbName, stream, plainBytes.Length);
        }
        catch (Exception ex)
        {
            return $"FAIL[plain:Import]: {ex.GetType().Name}: {ex.Message}";
        }

        var plainRows = await ReadRowsAsync();
        if (plainRows.Count != RowCount)
        {
            return $"FAIL[plain:Verify]: expected {RowCount} rows after streaming import on Plain disk, got {plainRows.Count}";
        }
        for (var i = 0; i < RowCount; i++)
        {
            if (plainRows[i].Marker != $"single-{i}")
            {
                return $"FAIL[plain:Verify]: row {i} Marker mismatch (got '{plainRows[i].Marker}')";
            }
        }

        // ---- Phase 3: Plain → Encrypted+Unlocked streaming import --------
        // Wipe everything, EnterEncrypted on an empty pool, then stream the
        // same plain bytes in. Worker should rekey-on-write under globalKey
        // and the rows should be readable through the encrypted I/O path.
        try
        {
            await _databaseService.DeleteDatabaseAsync(dbName);
        }
        catch { /* best-effort */ }

        var k = new byte[32];
        for (var i = 0; i < 32; i++) { k[i] = (byte)(0xa0 + i); }

        try
        {
            await _session.EnterEncryptedAsync(k, "test-credential-id-single-db-streaming");
        }
        catch (Exception ex)
        {
            CryptographicOperations.ZeroMemory(k);
            return $"FAIL[encrypted:EnterEncrypted]: {ex.GetType().Name}: {ex.Message}";
        }

        try
        {
            using var stream = new MemoryStream(plainBytes, writable: false);
            await _databaseService.ImportDatabaseFromStreamAsync(
                dbName, stream, plainBytes.Length);
        }
        catch (Exception ex)
        {
            CryptographicOperations.ZeroMemory(k);
            return $"FAIL[encrypted:Import]: {ex.GetType().Name}: {ex.Message}";
        }
        finally
        {
            CryptographicOperations.ZeroMemory(k);
        }

        var encryptedRows = await ReadRowsAsync();
        if (encryptedRows.Count != RowCount)
        {
            return $"FAIL[encrypted:Verify]: expected {RowCount} rows after streaming import on Encrypted+Unlocked disk, got {encryptedRows.Count}";
        }
        for (var i = 0; i < RowCount; i++)
        {
            if (encryptedRows[i].Marker != $"single-{i}")
            {
                return $"FAIL[encrypted:Verify]: row {i} Marker mismatch (got '{encryptedRows[i].Marker}')";
            }
        }

        await CleanupAsync(dbName);
        return "OK";
    }

    private async Task<List<VfsTestItem>> ReadRowsAsync()
    {
        await using var ctx = await _factory.CreateDbContextAsync();
        return await ctx.Items.OrderBy(x => x.Id).ToListAsync();
    }

    private async Task CleanupAsync(string dbName)
    {
        _ = dbName; // pool-wipe covers it; explicit name kept for callsite clarity
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
