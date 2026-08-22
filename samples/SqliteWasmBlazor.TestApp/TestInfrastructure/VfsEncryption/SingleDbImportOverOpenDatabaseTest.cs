using Microsoft.EntityFrameworkCore;

namespace SqliteWasmBlazor.TestApp.TestInfrastructure.VfsEncryption;

/// <summary>
/// Streaming single-DB import into a database that is currently OPEN — the
/// shape the encryption page produces, where a user picks a <c>.db</c> file
/// on a row while the app's <c>DbContext</c> has been querying that same
/// database all along.
///
/// <para>
/// The commit promotes a temp slot over the target via
/// <c>atomicReplaceFile</c>, which hands the target's SAH back to the pool's
/// free list. An <c>OFile</c> captures its SAH at <c>xOpen</c> time, so a
/// handle that survives the swap keeps serving the pre-import pages — and
/// writes into a slot the pool can hand to the next file. The import path
/// therefore closes the database first; this test fails if that close is
/// ever dropped again.
/// </para>
///
/// <para>
/// Discriminating step: the snapshot is taken BEFORE the rows are rewritten,
/// so a stale handle answers with the post-snapshot rows the import was
/// supposed to undo. Both the row markers and the row count are asserted.
/// </para>
/// </summary>
internal sealed class SingleDbImportOverOpenDatabaseTest
{
    private const int RowCount = 10;

    private readonly IDbContextFactory<PrfVfsTestContext> _factory;
    private readonly ISqliteWasmDatabaseService _databaseService;
    private readonly IEncryptedSqliteWasmDatabaseService _session;

    public string Name => "SingleDb_StreamingImport_OverOpenDatabase";

    public SingleDbImportOverOpenDatabaseTest(
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

        // ---- Phase 1: populate with the rows the snapshot will carry -----
        await using (var ctx = await _factory.CreateDbContextAsync())
        {
            await ctx.Database.EnsureCreatedAsync();
            for (var i = 0; i < RowCount; i++)
            {
                ctx.Items.Add(new VfsTestItem
                {
                    Marker = $"snapshot-{i}",
                    Payload = $"payload-{i}",
                });
            }
            await ctx.SaveChangesAsync();
        }

        byte[] snapshot;
        try
        {
            // Closes the DB for a consistent read — the reopen below is what
            // puts the handle back into the worker's cache.
            snapshot = await _databaseService.ExportDatabaseBytesAsync(dbName);
        }
        catch (Exception ex)
        {
            return $"FAIL[Export]: {ex.GetType().Name}: {ex.Message}";
        }
        if (snapshot.Length == 0)
        {
            return "FAIL[Export]: ExportDatabaseBytesAsync returned empty bytes";
        }

        // ---- Phase 2: rewrite the rows, leaving the DB open --------------
        // After this the worker holds an open handle on the target and its
        // pages differ from the snapshot's, so a stale handle is visible in
        // the verification below.
        await using (var ctx = await _factory.CreateDbContextAsync())
        {
            var existing = await ctx.Items.ToListAsync();
            ctx.Items.RemoveRange(existing);
            for (var i = 0; i < RowCount * 2; i++)
            {
                ctx.Items.Add(new VfsTestItem
                {
                    Marker = $"post-snapshot-{i}",
                    Payload = $"payload-{i}",
                });
            }
            await ctx.SaveChangesAsync();
        }

        // ---- Phase 3: import the snapshot over the open database ---------
        try
        {
            using var stream = new MemoryStream(snapshot, writable: false);
            await _databaseService.ImportDatabaseFromStreamAsync(
                dbName, stream, snapshot.Length);
        }
        catch (Exception ex)
        {
            return $"FAIL[Import]: {ex.GetType().Name}: {ex.Message}";
        }

        // ---- Phase 4: the snapshot's rows are what a query now sees ------
        var rows = await ReadRowsAsync();
        if (rows.Count != RowCount)
        {
            return $"FAIL[Verify]: expected {RowCount} rows from the imported snapshot, got {rows.Count} " +
                   $"(a stale open handle would report {RowCount * 2})";
        }
        for (var i = 0; i < RowCount; i++)
        {
            if (rows[i].Marker != $"snapshot-{i}")
            {
                return $"FAIL[Verify]: row {i} Marker mismatch (got '{rows[i].Marker}', " +
                       $"expected 'snapshot-{i}')";
            }
        }

        await CleanupAsync();
        return "OK";
    }

    private async Task<List<VfsTestItem>> ReadRowsAsync()
    {
        await using var ctx = await _factory.CreateDbContextAsync();
        return await ctx.Items.OrderBy(x => x.Id).ToListAsync();
    }

    // ResetPoolAsync closes every open DB and unlinks every file, which is
    // exactly the pre- and post-condition this test needs. A failure here is
    // a genuine failure — the runner reports it rather than a swallowed
    // cleanup leaving the next assertion to explain it badly.
    private ValueTask CleanupAsync() => new(_session.ResetPoolAsync());
}
