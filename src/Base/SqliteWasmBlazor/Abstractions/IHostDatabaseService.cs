// SqliteWasmBlazor - Minimal EF Core compatible provider
// MIT License

namespace SqliteWasmBlazor;

/// <summary>
/// Host-supplied seam for the two things the library cannot know: which
/// databases the app actually owns, and what their schema is supposed to
/// look like. Only the host can answer either — it owns the
/// <see cref="Microsoft.EntityFrameworkCore.DbContext"/> types whose models
/// define both.
///
/// <para>
/// Consumed by the import paths on <see cref="ISqliteWasmDatabaseService"/>:
/// a validated import calls <see cref="ValidateSchemaAsync"/> while the
/// content it would replace is parked, and re-runs
/// <see cref="MigrateAsync"/> once it commits. Hosts that declare nothing
/// register <see cref="NullHostDatabaseService.Instance"/>, or simply do not
/// register the seam at all.
/// </para>
/// </summary>
public interface IHostDatabaseService
{
    /// <summary>
    /// Names of the databases this app opens by connection string, e.g.
    /// <c>["TodoDb.db", "NotesDb.db"]</c>. The pool can hold more entries
    /// than this — imports and retired features leave rows behind — so a UI
    /// listing pool content uses the list to tell "this app reads this one"
    /// apart from "this is just stored here", and to keep an import from
    /// landing on a name nothing will ever open.
    ///
    /// <para>
    /// Empty when the host doesn't declare its databases; every pool entry
    /// is then treated as unowned.
    /// </para>
    /// </summary>
    IReadOnlyList<string> OwnedDatabases { get; }

    /// <summary>
    /// Re-run migrations for every owned database and promote the boot
    /// status back to <see cref="DbInitState.READY"/>, without destroying
    /// anything. Called after an import has replaced pool content: the
    /// bytes that just landed may carry an older schema than the app's
    /// model, and an owned database the import didn't include has to be
    /// re-created before the next query hits it.
    /// </summary>
    ValueTask MigrateAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Decide whether what now sits at <paramref name="probeDatabaseName"/>
    /// is a valid <paramref name="ownedDatabaseName"/>, and throw
    /// <see cref="SchemaMismatchException"/> if it is not. Called on a
    /// validated import, while the content it would replace is parked: the
    /// import survives only if this returns.
    ///
    /// <para>
    /// <c>DbContext.ValidateImportedSchemaAsync</c> implements the check;
    /// open a context bound to <paramref name="probeDatabaseName"/> and call
    /// it. Names outside <see cref="OwnedDatabases"/> have no model to check
    /// against — return without throwing.
    /// </para>
    /// </summary>
    /// <param name="ownedDatabaseName">The database the import is destined for.</param>
    /// <param name="probeDatabaseName">
    /// Pool name to open and inspect. The import writes under the database's
    /// own name, so the two are the same today; the parameter stays separate
    /// because it is the one a host must connect to, not the one it reports.
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask ValidateSchemaAsync(
        string ownedDatabaseName,
        string probeDatabaseName,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// No-op <see cref="IHostDatabaseService"/> for hosts that declare no
/// databases and want no schema gate. Use <see cref="Instance"/> to avoid
/// allocations.
/// </summary>
public sealed class NullHostDatabaseService : IHostDatabaseService
{
    /// <summary>Shared instance — the type carries no state.</summary>
    public static NullHostDatabaseService Instance { get; } = new();

    /// <inheritdoc />
    public IReadOnlyList<string> OwnedDatabases => [];

    /// <inheritdoc />
    public ValueTask MigrateAsync(CancellationToken cancellationToken = default)
        => ValueTask.CompletedTask;

    /// <inheritdoc />
    public ValueTask ValidateSchemaAsync(
        string ownedDatabaseName,
        string probeDatabaseName,
        CancellationToken cancellationToken = default)
        => ValueTask.CompletedTask;
}
