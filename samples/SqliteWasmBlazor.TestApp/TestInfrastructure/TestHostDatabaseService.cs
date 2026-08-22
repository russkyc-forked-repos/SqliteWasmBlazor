namespace SqliteWasmBlazor.TestApp.TestInfrastructure;

/// <summary>
/// Counting stand-in for the host seam, so a test can assert that the import
/// paths reconcile the host's schema themselves rather than relying on the
/// drop-in UI to do it. Declares no owned databases and gates nothing — the
/// TestApp's own tests pass their <c>validateImported</c> delegates directly,
/// so the only thing this observes is <see cref="MigrateAsync"/>.
/// </summary>
internal sealed class TestHostDatabaseService : IHostDatabaseService
{
    /// <summary>How many times an import has reconciled through this seam.</summary>
    public int MigrateCount { get; private set; }

    /// <inheritdoc />
    public IReadOnlyList<string> OwnedDatabases => [];

    /// <inheritdoc />
    public ValueTask MigrateAsync(CancellationToken cancellationToken = default)
    {
        MigrateCount++;
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask ValidateSchemaAsync(
        string ownedDatabaseName,
        string probeDatabaseName,
        CancellationToken cancellationToken = default)
        => ValueTask.CompletedTask;
}
