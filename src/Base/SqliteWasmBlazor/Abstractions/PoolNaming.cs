// SqliteWasmBlazor - Minimal EF Core compatible provider
// MIT License

namespace SqliteWasmBlazor;

/// <summary>
/// Names the SAH pool holds that are not databases an app opens. Today
/// that is one shape: the parked copy a validated import leaves behind
/// while it decides whether to keep what arrived.
/// </summary>
public static class PoolNaming
{
    /// <summary>
    /// Suffix of the entry an import parks the previous content under
    /// (<c>TodoDb.db</c> → <c>TodoDb.db.import-park</c>).
    ///
    /// <para>
    /// A validated import writes the incoming file under the database's
    /// real name — page AAD binds ciphertext to the database path, so a
    /// file written anywhere else would not decrypt once moved — and parks
    /// what was there under this suffix. Renames are metadata-only, so the
    /// parked bytes are untouched and go back exactly as they were if the
    /// validator refuses. On success the park is dropped.
    /// </para>
    /// <para>
    /// A park outlives the import only if the tab dies mid-flight or the
    /// platform closes the pool's access handles under it. Whichever end
    /// finds it first puts it back: the worker restores an orphan at init,
    /// and <c>SweepImportParksAsync</c> restores one before the next import
    /// starts. A park is only ever dropped while the database it was taken
    /// from is present — until then it <em>is</em> that database. UI
    /// listing pool content should hide these entries —
    /// <see cref="IsImportPark"/> identifies them — since nothing opens
    /// them and they normally exist for seconds at a time.
    /// </para>
    /// </summary>
    public const string ImportParkSuffix = ".import-park";

    /// <summary>
    /// True when <paramref name="databaseName"/> is a parked import copy
    /// rather than a database of its own.
    /// </summary>
    /// <param name="databaseName">Pool entry name, e.g. <c>TodoDb.db</c>.</param>
    public static bool IsImportPark(string databaseName)
    {
        ArgumentNullException.ThrowIfNull(databaseName);
        return databaseName.EndsWith(ImportParkSuffix, StringComparison.Ordinal);
    }

    /// <summary>
    /// The park name for <paramref name="databaseName"/>.
    /// </summary>
    /// <param name="databaseName">Database whose content is being parked.</param>
    public static string ImportParkFor(string databaseName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(databaseName);
        return $"{databaseName}{ImportParkSuffix}";
    }

    /// <summary>
    /// The database <paramref name="parkName"/> holds the previous content
    /// of — the inverse of <see cref="ImportParkFor"/>.
    /// </summary>
    /// <param name="parkName">A park name, e.g. <c>TodoDb.db.import-park</c>.</param>
    /// <exception cref="ArgumentException">
    /// <paramref name="parkName"/> is not a park name.
    /// </exception>
    public static string DatabaseNameForPark(string parkName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(parkName);
        if (!IsImportPark(parkName))
        {
            throw new ArgumentException(
                $"'{parkName}' is not a park name.", nameof(parkName));
        }

        return parkName[..^ImportParkSuffix.Length];
    }
}
