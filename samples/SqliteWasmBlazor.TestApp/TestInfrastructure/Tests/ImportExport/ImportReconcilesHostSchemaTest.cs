using Microsoft.EntityFrameworkCore;
using SqliteWasmBlazor.Models;
using SqliteWasmBlazor.Models.Models;

namespace SqliteWasmBlazor.TestApp.TestInfrastructure.Tests.ImportExport;

/// <summary>
/// Every successful import re-runs the host's migrations. The invariant used
/// to live in the drop-in encryption panel, which meant a consumer driving
/// the service directly got a pool with a possibly-older schema and no
/// re-created owned databases, silently. It now lives in the import paths, so
/// this asserts it where a headless consumer would see it: no UI in sight,
/// just the service and a counting host seam.
///
/// <para>
/// Both single-database exits are covered — the plain import and the
/// validated one, which reach the reconcile from different branches. The
/// multi-database paths call the same helper.
/// </para>
/// </summary>
internal class ImportReconcilesHostSchemaTest(
    IDbContextFactory<TodoDbContext> factory,
    ISqliteWasmDatabaseService databaseService,
    TestHostDatabaseService host)
    : SqliteWasmTest(factory, databaseService)
{
    public override string Name => "ImportRawDatabase_ReconcilesHostSchema";

    private const string DbName = "TestDb.db";

    public override async ValueTask<string?> RunTestAsync()
    {
        if (DatabaseService is null)
        {
            throw new InvalidOperationException("ISqliteWasmDatabaseService not available");
        }

        await using (var context = await Factory.CreateDbContextAsync())
        {
            context.TodoItems.Add(new TodoItem
            {
                Id = Guid.NewGuid(), Title = "Reconcile Test", Description = "Test data",
                IsCompleted = false, UpdatedAt = DateTime.UtcNow
            });
            await context.SaveChangesAsync();
        }

        var exported = await DatabaseService.ExportDatabaseBytesAsync(DbName);

        var before = host.MigrateCount;
        await DatabaseService.ImportDatabaseBytesAsync(DbName, exported);
        if (host.MigrateCount != before + 1)
        {
            throw new InvalidOperationException(
                $"Plain import did not reconcile the host schema " +
                $"(MigrateCount {before} → {host.MigrateCount}).");
        }

        before = host.MigrateCount;
        var validated = 0;
        using (var source = new MemoryStream(exported, writable: false))
        {
            await DatabaseService.ImportDatabaseFromStreamAsync(
                DbName,
                source,
                exported.Length,
                (_, _) =>
                {
                    validated++;
                    return ValueTask.CompletedTask;
                });
        }

        if (validated != 1)
        {
            throw new InvalidOperationException(
                $"Expected the validator to run once, ran {validated} times.");
        }
        if (host.MigrateCount != before + 1)
        {
            throw new InvalidOperationException(
                $"Validated import did not reconcile the host schema " +
                $"(MigrateCount {before} → {host.MigrateCount}).");
        }

        // The reconcile runs after the import commits, so the database is
        // still the one that landed.
        await using (var context = await Factory.CreateDbContextAsync())
        {
            var count = await context.TodoItems.CountAsync();
            if (count != 1)
            {
                throw new InvalidOperationException(
                    $"Expected 1 item after import, got {count}");
            }
        }

        return "OK";
    }
}
