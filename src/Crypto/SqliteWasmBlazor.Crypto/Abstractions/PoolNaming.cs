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
    /// A park outlives the import only if the tab dies mid-flight; the next
    /// import sweeps whatever it finds. UI listing pool content should hide
    /// these entries — <see cref="IsImportPark"/> identifies them — since
    /// nothing opens them and they exist for seconds at a time.
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
}
