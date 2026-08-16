using Microsoft.EntityFrameworkCore;
using SqliteWasmBlazor.Models;
using SqliteWasmBlazor.Models.Models;

namespace SqliteWasmBlazor.TestApp.TestInfrastructure.Tests.ImportExport;

/// <summary>
/// Tests the staged download export: the worker copies the DB into an OPFS
/// staging file and the bridge fires an anchor download from the disk-backed
/// File — no bytes enter managed memory. Verifies the operation completes,
/// the DB re-opens afterwards (export closes it for a consistent snapshot),
/// and existing data plus new writes survive.
/// </summary>
internal class RawDatabaseExportToDownloadTest(IDbContextFactory<TodoDbContext> factory, ISqliteWasmDatabaseService databaseService)
    : SqliteWasmTest(factory, databaseService)
{
    public override string Name => "ExportRawDatabase_StagedDownload";

    private const string DbName = "TestDb.db";

    public override async ValueTask<string?> RunTestAsync()
    {
        if (DatabaseService is null)
        {
            throw new InvalidOperationException("ISqliteWasmDatabaseService not available");
        }

        // Create initial data
        await using (var context = await Factory.CreateDbContextAsync())
        {
            context.TodoItems.Add(new TodoItem
            {
                Id = Guid.NewGuid(), Title = "Before Staged Export", Description = "Exists before export",
                IsCompleted = false, UpdatedAt = DateTime.UtcNow
            });
            await context.SaveChangesAsync();
        }

        // Staged export → anchor download (closes DB for a consistent snapshot)
        await DatabaseService.ExportDatabaseToDownloadAsync(DbName, "staged-export-test.db");

        // Re-open via EF Core (simulates what the page does)
        await using (var context = await Factory.CreateDbContextAsync())
        {
            await context.Database.EnsureCreatedAsync();
        }

        // Verify existing data is still accessible
        await using (var context = await Factory.CreateDbContextAsync())
        {
            var item = await context.TodoItems.FirstOrDefaultAsync(t => t.Title == "Before Staged Export");
            if (item is null)
            {
                throw new InvalidOperationException("Existing data not accessible after staged export + re-open");
            }
        }

        // Verify we can write new data after re-open
        await using (var context = await Factory.CreateDbContextAsync())
        {
            context.TodoItems.Add(new TodoItem
            {
                Id = Guid.NewGuid(), Title = "After Staged Export", Description = "Added after re-open",
                IsCompleted = false, UpdatedAt = DateTime.UtcNow
            });
            await context.SaveChangesAsync();
        }

        await using (var context = await Factory.CreateDbContextAsync())
        {
            var count = await context.TodoItems.CountAsync();
            if (count != 2)
            {
                throw new InvalidOperationException($"Expected 2 items after write, got {count}");
            }
        }

        return "OK";
    }
}
