namespace SqliteWasmBlazor.Crypto.UI.Services;

/// <summary>
/// Host-supplied seam for the two things the library cannot know: which
/// databases the app actually owns, and how to bring their schema up to
/// date. Invoked by <see cref="Components.Shared.DatabaseErrorAlert"/> on a
/// recoverable boot failure (<see cref="SchemaIncompatibleFailure"/>,
/// <see cref="GenericInitFailure"/>, or any unmapped
/// <see cref="IDbInitFailure"/>), and by the encryption panel after every
/// operation that replaces pool content.
///
/// <para>
/// The library intentionally does not own the recovery path because the
/// CryptoSync.UI panels are reusable across consumer apps with different
/// <c>DbContext</c> types and database names. Hosts that ship without
/// recovery (read-only deployments, etc.) register
/// <see cref="NullHostDatabaseService.Instance"/> — the panel will hide
/// the reset button and only offer the reload path.
/// </para>
/// </summary>
public interface IHostDatabaseService
{
    /// <summary>
    /// True when the implementation can actually perform a reset. The
    /// <see cref="NullHostDatabaseService"/> default returns <c>false</c>,
    /// which the alert panel uses to hide the reset button.
    /// </summary>
    bool IsAvailable { get; }

    /// <summary>
    /// Names of the databases this app opens by connection string, e.g.
    /// <c>["TodoDb.db", "NotesDb.db"]</c>. The pool can hold more entries
    /// than this — imports and retired features leave rows behind — so the
    /// encryption panel uses the list to tell "this app reads this one"
    /// apart from "this is just stored here", and to keep an import from
    /// landing on a name nothing will ever open.
    ///
    /// <para>
    /// Empty when the host doesn't declare its databases; the panel then
    /// treats every pool entry as unowned.
    /// </para>
    /// </summary>
    IReadOnlyList<string> OwnedDatabases { get; }

    /// <summary>
    /// Perform the host-defined recovery: wipe the pool, re-migrate every
    /// owned database, then promote the boot status back to
    /// <see cref="DbInitState.READY"/>.
    /// </summary>
    ValueTask ResetAsync(CancellationToken cancellationToken = default);

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
    /// Decide whether the database at <paramref name="probeDatabaseName"/>
    /// is a valid stand-in for <paramref name="ownedDatabaseName"/>, and
    /// throw if it is not. Called on a staged import — the probe database
    /// holds the picked file's content under a temporary pool name, and the
    /// import is only promoted if this returns.
    ///
    /// <para>
    /// The host is the only layer that can answer this: it owns the
    /// <c>DbContext</c> whose model says which tables the database must
    /// have. <c>DbContext.ValidateImportedSchemaAsync</c> implements the
    /// check; open a context bound to <paramref name="probeDatabaseName"/>
    /// and call it. Names outside <see cref="OwnedDatabases"/> have no
    /// model to check against — return without throwing.
    /// </para>
    /// </summary>
    /// <param name="ownedDatabaseName">The database the import is destined for.</param>
    /// <param name="probeDatabaseName">Temporary pool name holding the imported content.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask ValidateSchemaAsync(
        string ownedDatabaseName,
        string probeDatabaseName,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// No-op <see cref="IHostDatabaseService"/> for hosts that don't ship
/// recovery. Use <see cref="Instance"/> to avoid allocations.
/// </summary>
public sealed class NullHostDatabaseService : IHostDatabaseService
{
    /// <summary>Shared instance — the type carries no state.</summary>
    public static NullHostDatabaseService Instance { get; } = new();

    /// <inheritdoc />
    public bool IsAvailable => false;

    /// <inheritdoc />
    public IReadOnlyList<string> OwnedDatabases => [];

    /// <inheritdoc />
    public ValueTask ResetAsync(CancellationToken cancellationToken = default)
        => ValueTask.CompletedTask;

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
