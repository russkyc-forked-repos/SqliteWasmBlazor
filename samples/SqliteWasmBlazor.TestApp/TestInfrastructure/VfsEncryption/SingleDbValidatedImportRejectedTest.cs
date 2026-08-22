using Microsoft.EntityFrameworkCore;

namespace SqliteWasmBlazor.TestApp.TestInfrastructure.VfsEncryption;

/// <summary>
/// Validated single-DB import whose validator rejects the file — the shape
/// the encryption panel produces when a database file is picked on the
/// wrong row (a TodoDb backup offered to NotesDb). File names say nothing
/// about what is inside, so the tables have to, and the refusal has to
/// leave the database it was aimed at untouched.
///
/// <para>
/// The validator here is the real one the Demo host uses:
/// <c>DbContext.ValidateImportedSchemaAsync</c> against a context bound to
/// the imported database. The file is a database whose schema is
/// deliberately foreign — one table, not the model's — so the check fails
/// for the same reason a mismatched pick does.
/// </para>
///
/// <para>
/// Asserted: the rejection propagates, the database still holds its
/// original rows (restored from the park, byte-for-byte), and no park is
/// left behind as a stray.
/// </para>
/// </summary>
internal sealed class SingleDbValidatedImportRejectedTest
{
    private const int RowCount = 5;
    private const string ForeignDbName = "ForeignSchema.db";

    private readonly IDbContextFactory<PrfVfsTestContext> _factory;
    private readonly ISqliteWasmDatabaseService _databaseService;
    private readonly IEncryptedSqliteWasmDatabaseService _session;

    public string Name => "SingleDb_ValidatedImport_RejectedBySchemaCheck";

    public SingleDbValidatedImportRejectedTest(
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

        // ---- Phase 1: the target, with rows that must survive ------------
        await using (var ctx = await _factory.CreateDbContextAsync())
        {
            await ctx.Database.EnsureCreatedAsync();
            for (var i = 0; i < RowCount; i++)
            {
                ctx.Items.Add(new VfsTestItem
                {
                    Marker = $"keep-{i}",
                    Payload = $"payload-{i}",
                });
            }
            await ctx.SaveChangesAsync();
        }

        // ---- Phase 2: a database with a schema the model doesn't know ----
        // Built through the ADO layer so it is a genuine SQLite file the
        // import path accepts — it just isn't a PrfVfsTestDb.
        await using (var connection = new SqliteWasmConnection($"Data Source={ForeignDbName}"))
        {
            await connection.OpenAsync();
            await using var create = connection.CreateCommand();
            create.CommandText = "CREATE TABLE Strangers (Id INTEGER PRIMARY KEY, Note TEXT)";
            await create.ExecuteNonQueryAsync();
            await using var insert = connection.CreateCommand();
            insert.CommandText = "INSERT INTO Strangers (Note) VALUES ('not a VfsTestItem')";
            await insert.ExecuteNonQueryAsync();
        }

        byte[] foreignBytes;
        try
        {
            foreignBytes = await _databaseService.ExportDatabaseBytesAsync(ForeignDbName);
        }
        catch (Exception ex)
        {
            return $"FAIL[Export]: {ex.GetType().Name}: {ex.Message}";
        }
        await _databaseService.DeleteDatabaseAsync(ForeignDbName);

        // ---- Phase 3: import it into the target, validated ---------------
        var rejected = false;
        try
        {
            using var stream = new MemoryStream(foreignBytes, writable: false);
            await _databaseService.ImportDatabaseFromStreamAsync(
                dbName,
                stream,
                foreignBytes.Length,
                ValidateImportedAsync);
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
            return "FAIL[Import]: a database with a foreign schema was imported without complaint";
        }

        // ---- Phase 4: the target is untouched ----------------------------
        var rows = await ReadRowsAsync();
        if (rows.Count != RowCount)
        {
            return $"FAIL[Verify]: expected the original {RowCount} rows after a rejected import, got {rows.Count}";
        }
        for (var i = 0; i < RowCount; i++)
        {
            if (rows[i].Marker != $"keep-{i}")
            {
                return $"FAIL[Verify]: row {i} Marker mismatch (got '{rows[i].Marker}')";
            }
        }

        // ---- Phase 5: no park left in the pool ---------------------------
        var pool = await _databaseService.ListDatabasesAsync();
        var strays = pool.Where(PoolNaming.IsImportPark).ToArray();
        if (strays.Length > 0)
        {
            return $"FAIL[Verify]: parked entries left in the pool: {string.Join(", ", strays)}";
        }

        await CleanupAsync();
        return "OK";
    }

    // The Demo host's check, inlined: open the imported database with the
    // model that is supposed to fit it and let ValidateImportedSchemaAsync
    // decide.
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
