// SqliteWasmBlazor - Minimal EF Core compatible provider
// MIT License

namespace SqliteWasmBlazor;

/// <summary>
/// Outcome returned by <see cref="ISqliteWasmDatabaseService.ImportDatabaseAsync"/>
/// and the streaming import paths on
/// <c>IEncryptedSqliteWasmDatabaseService</c>. Plain (non-opaque) imports
/// always return <see cref="OK"/> on success and throw on byte-level
/// failures. Opaque (encrypted) imports go through the refuse-on-existing
/// + verify-on-write policy: a fresh-path import that AEAD-verifies under
/// the registered key returns <see cref="OK"/>; an import refused because
/// a DB already exists at the path returns <see cref="EXISTING_DB_REFUSED"/>;
/// an import whose slot 0 fails AEAD under the registered key returns
/// <see cref="WRONG_KEY"/> after the worker has rolled back (unlinked) the
/// partial file.
/// </summary>
public enum DiskImportResult
{
    /// <summary>
    /// Bytes written. For opaque imports with a registered key, slot 0 also
    /// AEAD-verified.
    /// </summary>
    OK = 0,

    /// <summary>
    /// Opaque import only: slot 0 failed AEAD authentication under the
    /// registered key. The worker has unlinked the half-written file so no
    /// state survives the failed import.
    /// </summary>
    WRONG_KEY = 1,

    /// <summary>
    /// Opaque import only: a DB file already exists at this path. Caller
    /// must call <see cref="ISqliteWasmDatabaseService.DeleteDatabaseAsync"/>
    /// first. Plain imports keep their overwrite semantics and never return
    /// this code.
    /// </summary>
    EXISTING_DB_REFUSED = 2,
}

/// <summary>
/// Plain SQLite database management on OPFS. Single-DB ops (Exists / Delete
/// / Rename / Close / Import / Export native <c>.db</c>), the pool-wide
/// <see cref="ListDatabasesAsync"/>, plain bulk row insert
/// (<see cref="ImportRowsAsync"/>).
///
/// <para>
/// <b>Audience.</b> Anyone using SQLite-on-OPFS — encryption-aware apps
/// (which also use <see cref="IEncryptedSqliteWasmDatabaseService"/>) and
/// pure plain apps. Per-DB <c>.db</c> bytes from
/// <see cref="ExportDatabaseAsync"/> open in <c>sqlite3</c>; multi-DB
/// transfers go through the streaming <c>.dbs</c> envelope on the
/// encrypted plane (<c>ExportDatabasesToDownloadAsync</c> /
/// <c>ImportDatabasesFromStreamAsync</c>).
/// </para>
///
/// <para>
/// <b>What this is NOT.</b> The encryption lifecycle (Enter/Leave/Lock/
/// Unlock/Reset, ExportDisk envelope, ImportDisk envelope) lives on
/// <see cref="IEncryptedSqliteWasmDatabaseService"/>. The CryptoSync
/// delta-bulk surface (DeltaExport/DeltaImport/DeltaRotate) lives on
/// <c>ICryptoSyncDeltaService</c> in the CryptoSync package. Both are
/// separately registered; consumers who don't need them never see them.
/// </para>
///
/// <para>
/// All single-DB operations refuse to write or read while the encrypted
/// disk is locked — they throw <see cref="DiskLockedException"/> via the
/// bridge gate. Wrap DB-touching code in
/// <c>&lt;AuthorizeView Policy="DatabaseOpen"&gt;</c> to avoid that path.
/// </para>
/// </summary>
public interface ISqliteWasmDatabaseService
{
    /// <summary>
    /// Bare main-DB names currently in the SAH pool — no journal / WAL /
    /// SHM siblings. Cheap pool metadata read; safe to call regardless of
    /// disk lock state.
    /// </summary>
    Task<IReadOnlyList<string>> ListDatabasesAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Checks if a database exists in OPFS.
    /// </summary>
    /// <param name="databaseName">The database filename (e.g., "mydb.db")</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>True if the database exists, false otherwise</returns>
    Task<bool> ExistsDatabaseAsync(string databaseName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes a database from OPFS.
    /// </summary>
    /// <param name="databaseName">The database filename to delete</param>
    /// <param name="cancellationToken">Cancellation token</param>
    Task DeleteDatabaseAsync(string databaseName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Renames a database in OPFS.
    /// </summary>
    /// <param name="oldName">The current database filename</param>
    /// <param name="newName">The new database filename</param>
    /// <param name="cancellationToken">Cancellation token</param>
    Task RenameDatabaseAsync(string oldName, string newName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Closes a database connection in the worker.
    /// Note: This closes the worker-side connection, not the C# DbConnection.
    /// </summary>
    /// <param name="databaseName">The database filename to close</param>
    /// <param name="cancellationToken">Cancellation token</param>
    Task CloseDatabaseAsync(string databaseName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Imports a raw <c>.db</c> file into OPFS. The database is not opened
    /// after import — caller must re-open when ready (e.g., after cleaning
    /// up backup files to avoid SAH pool exhaustion).
    ///
    /// <para>
    /// Auto-detects ciphertext vs plaintext via the SQLite-format-3 magic
    /// bytes. Plain imports allow overwriting an existing DB and always
    /// return <see cref="DiskImportResult.OK"/> on success. Opaque
    /// (encrypted) imports are subject to the refuse-on-existing +
    /// verify-on-write policy and may return
    /// <see cref="DiskImportResult.EXISTING_DB_REFUSED"/> or
    /// <see cref="DiskImportResult.WRONG_KEY"/>.
    /// </para>
    /// </summary>
    /// <param name="databaseName">The database filename (e.g., "mydb.db")</param>
    /// <param name="data">Raw SQLite database bytes (plaintext .db file or
    /// PRF-VFS slot-format ciphertext)</param>
    /// <param name="cancellationToken">Cancellation token</param>
    Task<DiskImportResult> ImportDatabaseAsync(string databaseName, byte[] data,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Exports a single database as raw native SQLite bytes — equivalent
    /// to dumping the on-disk file. Plain DBs return standard SQLite pages
    /// (<c>sqlite3 file.db</c> opens them); encrypted DBs return slot-format
    /// ciphertext under the active globalKey (only re-importable on a disk
    /// holding the same key). The worker closes the DB before exporting
    /// for a consistent snapshot — caller must re-open afterwards.
    ///
    /// <para>
    /// For multi-DB plain export use the encrypted plane's
    /// <c>IEncryptedSqliteWasmDatabaseService.ExportDatabasesToDownloadAsync</c>
    /// (a streamed <c>.dbs</c> envelope) — that path avoids the managed-byte[]
    /// allocation this byte[]-returning per-DB primitive still incurs.
    /// </para>
    /// </summary>
    /// <param name="databaseName">The database filename (e.g., "mydb.db").</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Raw native SQLite bytes (plain pages or slot-format ciphertext).</returns>
    Task<byte[]> ExportDatabaseAsync(string databaseName,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Plain (non-encrypted) row import from a V2 MessagePack payload built
    /// via <c>MessagePackFileHeaderV2</c>. Worker streams rows into the
    /// named target table using a single prepared INSERT inside a
    /// transaction.
    ///
    /// <para>
    /// DB-agnostic: column metadata (name, SQL type, C# type) is read from
    /// the payload header itself — no dependency on a CryptoSync
    /// <c>_column_registry</c>. Suitable for plain SQLite databases
    /// (test-data generation, seeding) as well as CryptoSync-bootstrapped
    /// DBs.
    /// </para>
    /// </summary>
    /// <param name="databaseName">Target database filename.</param>
    /// <param name="data">V2 MessagePack bytes: header + row arrays.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Number of rows imported.</returns>
    Task<int> ImportRowsAsync(string databaseName, byte[] data,
        CancellationToken cancellationToken = default);
}
