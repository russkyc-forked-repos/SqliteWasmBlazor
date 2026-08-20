using Microsoft.EntityFrameworkCore;
using SqliteWasmBlazor.Models;
using SqliteWasmBlazor.Models.Models;

namespace SqliteWasmBlazor.TestApp.TestInfrastructure.Tests.CRUD;

/// <summary>
/// Mirrors the Demo's <c>TodoListModel.DeleteTodoAsync</c> soft-delete path
/// exactly: migrated schema (so the FTS5 triggers are live), a detached
/// entity from an earlier context, <c>FindAsync</c> in a fresh context, a
/// property mutation on the tracked instance, then <c>SaveChangesAsync</c>.
///
/// Guards the two silent-failure modes the demo can hit — <c>FindAsync</c>
/// returning null, and the mutation not being persisted — both of which
/// report success to the user while the row stays in the list.
/// </summary>
internal class SoftDeleteViaFindTest(IDbContextFactory<TodoDbContext> factory)
    : SqliteWasmTest(factory)
{
    public override string Name => "SoftDelete_ViaFind";

    // Migrations (not EnsureCreated) so the FTS5 triggers fire on UPDATE,
    // matching the Demo app's schema.
    protected override bool AutoCreateDatabase => false;

    public override async ValueTask<string?> RunTestAsync()
    {
        await using (var migrateContext = await Factory.CreateDbContextAsync())
        {
            await migrateContext.Database.MigrateAsync();
        }

        var items = new[]
        {
            NewItem("Keep One", "stays active"),
            NewItem("Delete Me", "soft-deleted below"),
            NewItem("Keep Two", "stays active"),
        };

        await using (var writeContext = await Factory.CreateDbContextAsync())
        {
            writeContext.TodoItems.AddRange(items);
            await writeContext.SaveChangesAsync();
        }

        // The list the UI renders — detached instances from a disposed context.
        List<TodoItem> listed;
        await using (var listContext = await Factory.CreateDbContextAsync())
        {
            listed = await listContext.TodoItems
                .Where(t => !t.IsDeleted)
                .OrderByDescending(t => t.UpdatedAt)
                .ToListAsync();
        }

        if (listed.Count != 3)
        {
            throw new InvalidOperationException($"Expected 3 active items before delete, got {listed.Count}");
        }

        var target = listed.Single(t => t.Title == "Delete Me");
        Console.WriteLine($"[{Name}] Target id from list: {target.Id}");

        int saved;
        await using (var deleteContext = await Factory.CreateDbContextAsync())
        {
            var tracked = await deleteContext.TodoItems.FindAsync([target.Id], CancellationToken.None);
            if (tracked is null)
            {
                throw new InvalidOperationException(
                    $"FindAsync returned null for id {target.Id} — the delete would silently no-op.");
            }

            tracked.IsDeleted = true;
            tracked.DeletedAt = DateTime.UtcNow;

            var stateBeforeSave = deleteContext.Entry(tracked).State;
            Console.WriteLine($"[{Name}] Entity state before SaveChanges: {stateBeforeSave}");

            saved = await deleteContext.SaveChangesAsync();
            Console.WriteLine($"[{Name}] SaveChangesAsync returned {saved}");
        }

        if (saved != 1)
        {
            throw new InvalidOperationException($"Expected SaveChangesAsync to write 1 row, got {saved}");
        }

        await using (var verifyContext = await Factory.CreateDbContextAsync())
        {
            var remaining = await verifyContext.TodoItems.CountAsync(t => !t.IsDeleted);
            if (remaining != 2)
            {
                throw new InvalidOperationException($"Expected 2 active items after delete, got {remaining}");
            }

            var reloaded = await verifyContext.TodoItems.FindAsync([target.Id], CancellationToken.None);
            if (reloaded is null)
            {
                throw new InvalidOperationException("Soft-deleted row disappeared entirely");
            }

            if (!reloaded.IsDeleted)
            {
                throw new InvalidOperationException("IsDeleted was not persisted");
            }
        }

        return "OK";
    }

    private static TodoItem NewItem(string title, string description) => new()
    {
        Id = Guid.NewGuid(),
        Title = title,
        Description = description,
        IsCompleted = false,
        IsDeleted = false,
        UpdatedAt = DateTime.UtcNow,
    };
}
