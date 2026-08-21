using System.Security.Cryptography;
using Microsoft.EntityFrameworkCore;

namespace SqliteWasmBlazor.TestApp.TestInfrastructure.VfsEncryption;

/// <summary>
/// Validated single-DB import that the validator accepts, on an
/// Encrypted+Unlocked pool — the half of the flow a rejection test cannot
/// reach, and the one where the page AAD is at stake.
///
/// <para>
/// Page ciphertext is bound to the database path
/// (<c>prf-vfs-v1|/databases/{name}|{slot}</c>), so an import written under
/// a temporary name and moved afterwards would stop decrypting the moment
/// it arrived. The import therefore writes under the database's own name
/// and parks what was there. This test would fail at the validator's own
/// read if that were ever turned around.
/// </para>
///
/// <para>
/// Sequence: populate v1 → snapshot plain bytes → overwrite with v2 →
/// EnterEncrypted → import the v1 snapshot with the real schema check →
/// the rows read back as v1 through the encrypted I/O path, and no park is
/// left in the pool.
/// </para>
/// </summary>
internal sealed class SingleDbValidatedImportAcceptedTest
{
    private const int RowCount = 6;

    private readonly IDbContextFactory<PrfVfsTestContext> _factory;
    private readonly ISqliteWasmDatabaseService _databaseService;
    private readonly IEncryptedSqliteWasmDatabaseService _session;

    public string Name => "SingleDb_ValidatedImport_AcceptedOnEncryptedPool";

    public SingleDbValidatedImportAcceptedTest(
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
        await CleanupAsync();

        // ---- Phase 1: v1 rows, snapshotted as plain bytes ----------------
        await using (var ctx = await _factory.CreateDbContextAsync())
        {
            await ctx.Database.EnsureCreatedAsync();
            for (var i = 0; i < RowCount; i++)
            {
                ctx.Items.Add(new VfsTestItem { Marker = $"v1-{i}", Payload = $"payload-{i}" });
            }
            await ctx.SaveChangesAsync();
        }

        byte[] snapshot;
        try
        {
            snapshot = await _databaseService.ExportDatabaseAsync(dbName);
        }
        catch (Exception ex)
        {
            return $"FAIL[Export]: {ex.GetType().Name}: {ex.Message}";
        }

        // ---- Phase 2: v2 rows, so a no-op import would be invisible ------
        await using (var ctx = await _factory.CreateDbContextAsync())
        {
            ctx.Items.RemoveRange(await ctx.Items.ToListAsync());
            for (var i = 0; i < RowCount; i++)
            {
                ctx.Items.Add(new VfsTestItem { Marker = $"v2-{i}", Payload = $"payload-{i}" });
            }
            await ctx.SaveChangesAsync();
        }

        // ---- Phase 3: encrypt the pool -----------------------------------
        var key = new byte[32];
        for (var i = 0; i < 32; i++) { key[i] = (byte)(0x30 + i); }
        try
        {
            await _session.EnterEncryptedAsync(key, "test-credential-id-validated-import");
        }
        catch (Exception ex)
        {
            return $"FAIL[EnterEncrypted]: {ex.GetType().Name}: {ex.Message}";
        }
        finally
        {
            CryptographicOperations.ZeroMemory(key);
        }

        // ---- Phase 4: import v1 back, with the schema check in the way ---
        try
        {
            using var stream = new MemoryStream(snapshot, writable: false);
            await _session.ImportDatabaseFromStreamAsync(
                dbName, stream, snapshot.Length, ValidateImportedAsync);
        }
        catch (Exception ex)
        {
            return $"FAIL[Import]: {ex.GetType().Name}: {ex.Message}";
        }

        // ---- Phase 5: v1 rows read back through the encrypted path -------
        var rows = await ReadRowsAsync();
        if (rows.Count != RowCount)
        {
            return $"FAIL[Verify]: expected {RowCount} imported rows, got {rows.Count}";
        }
        for (var i = 0; i < RowCount; i++)
        {
            if (rows[i].Marker != $"v1-{i}")
            {
                return $"FAIL[Verify]: row {i} Marker mismatch (got '{rows[i].Marker}', expected 'v1-{i}')";
            }
        }

        var strays = (await _databaseService.ListDatabasesAsync())
            .Where(PoolNaming.IsImportPark)
            .ToArray();
        if (strays.Length > 0)
        {
            return $"FAIL[Verify]: parked entries left in the pool: {string.Join(", ", strays)}";
        }

        await CleanupAsync();
        return "OK";
    }

    // Reads the imported database under the encryption key installed by
    // EnterEncrypted — a page whose AAD named a different path would fail
    // here, before the validator ever got to the table list.
    private async ValueTask ValidateImportedAsync(string imported, CancellationToken cancellationToken)
    {
        await using var probe = new PrfVfsTestContext(
            new DbContextOptionsBuilder<PrfVfsTestContext>()
                .UseSqliteWasm(new SqliteWasmConnection($"Data Source={imported}"))
                .Options);
        await probe.ValidateImportedSchemaAsync(imported);
    }

    private async Task<List<VfsTestItem>> ReadRowsAsync()
    {
        await using var ctx = await _factory.CreateDbContextAsync();
        return await ctx.Items.OrderBy(x => x.Id).ToListAsync();
    }

    private ValueTask CleanupAsync() => new(_session.ResetPoolAsync());
}
