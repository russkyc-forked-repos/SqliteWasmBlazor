using Microsoft.EntityFrameworkCore;

namespace SqliteWasmBlazor.TestApp.TestInfrastructure.VfsEncryption;

/// <summary>
/// Validated multi-DB <c>.dbs</c> import whose validator rejects one entry.
/// A bundle replaces every database in the pool, so a file that carries the
/// right names with the wrong contents is the most destructive way to pick
/// the wrong file — and the pool has to survive it intact.
///
/// <para>
/// The envelope here names the database the app opens but carries a
/// foreign schema, which is what a bundle exported from a different app
/// looks like. Asserted: the rejection propagates, the pool still holds the
/// original rows (restored from their parks, byte-for-byte), and no park is
/// left behind.
/// </para>
/// </summary>
internal sealed class DbsValidatedImportRejectedTest
{
    private const int RowCount = 6;
    private const string ForeignDbName = "ForeignBundleSource.db";

    private readonly IDbContextFactory<PrfVfsTestContext> _factory;
    private readonly ISqliteWasmDatabaseService _databaseService;
    private readonly IEncryptedSqliteWasmDatabaseService _session;

    public string Name => "Dbs_ValidatedImport_RejectedBySchemaCheck";

    public DbsValidatedImportRejectedTest(
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

        // ---- Phase 1: the pool's real content ----------------------------
        await using (var ctx = await _factory.CreateDbContextAsync())
        {
            await ctx.Database.EnsureCreatedAsync();
            for (var i = 0; i < RowCount; i++)
            {
                ctx.Items.Add(new VfsTestItem { Marker = $"keep-{i}", Payload = $"payload-{i}" });
            }
            await ctx.SaveChangesAsync();
        }

        // ---- Phase 2: a bundle carrying the right name, wrong tables -----
        await using (var connection = new SqliteWasmConnection($"Data Source={ForeignDbName}"))
        {
            await connection.OpenAsync();
            await using var create = connection.CreateCommand();
            create.CommandText = "CREATE TABLE Strangers (Id INTEGER PRIMARY KEY, Note TEXT)";
            await create.ExecuteNonQueryAsync();
        }

        byte[] foreignBytes;
        try
        {
            foreignBytes = await _databaseService.ExportDatabaseAsync(ForeignDbName);
        }
        catch (Exception ex)
        {
            return $"FAIL[Export]: {ex.GetType().Name}: {ex.Message}";
        }
        await _databaseService.DeleteDatabaseAsync(ForeignDbName);

        var envelope = DbsEnvelopeWriter.Build([(dbName, foreignBytes)]);

        // ---- Phase 3: import the bundle, validated -----------------------
        var rejected = false;
        try
        {
            using var stream = new MemoryStream(envelope, writable: false);
            await _databaseService.ImportDatabasesFromStreamAsync(
                stream, envelope.Length, ValidateImportedAsync);
        }
        catch (InvalidOperationException)
        {
            rejected = true;
        }
        catch (Exception ex)
        {
            return $"FAIL[Import]: unexpected {ex.GetType().Name}: {ex.Message}";
        }

        if (!rejected)
        {
            return "FAIL[Import]: a bundle with a foreign schema replaced the pool without complaint";
        }

        // ---- Phase 4: the pool is what it was ----------------------------
        var rows = await ReadRowsAsync();
        if (rows.Count != RowCount)
        {
            return $"FAIL[Verify]: expected the original {RowCount} rows after a rejected bundle, got {rows.Count}";
        }
        for (var i = 0; i < RowCount; i++)
        {
            if (rows[i].Marker != $"keep-{i}")
            {
                return $"FAIL[Verify]: row {i} Marker mismatch (got '{rows[i].Marker}')";
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
