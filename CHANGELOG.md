# Changelog

All notable changes to SqliteWasmBlazor are documented in this file.

## Version 0.9.3-pre

### A Note on the Development Delay
> **A quick update from the maintainer:** You might have noticed a lack of updates over the past few weeks. My development pipeline was hit hard when Anthropic made their services more or less unusable for my workflow. That situation has since been resolved — development is back on **Claude (Opus 5 / Fable 5)** and fully on track again!

### Import: One Scope Per Affordance, and a Fix for Imports Over Open Databases

Importing went through a single file picker whose extension silently decided how much it replaced — one database (`.db`), the whole pool (`.dbs`), or the whole pool plus its passkey binding (`.eds`) — and offered `.eds` in a state where the primitive refuses it, so the refusal arrived as an internal diagnostic naming a method. The panel is now shaped like the operations it performs.

- **Data-loss fix:** the streaming imports never closed the database they replaced. `atomicReplaceFile` hands the replaced file's SAH back to the pool's free list while an open `OFile` still holds it, so a `DbContext` that had the database open kept reading pre-import pages and writing into a slot the pool could hand to the next file. Both the worker and the bridge now close first. New coverage: `SingleDb_StreamingImport_OverOpenDatabase`, which fails with the exact symptom if the close is dropped again.
- **Per-database rows.** Each database has a row that owns everything scoped to it: pick it for export, replace it from a `.db` file, empty it. Whole-pool operations moved to their own card with the warning above the picker. The free-text "import into database" field is gone — it could only produce a pool entry no connection string points at.
- **Locking is done for you.** A `.eds` import rebinds the pool to the envelope's passkey and is refused while a session is open. The command now ends the session itself after a confirmation that says so, instead of reporting "Lock or Reset first".
- **Refusals are typed.** Disk-state preconditions throw `PoolOperationRejectedException` with a `Reason` (`ENTER_NEEDS_PLAIN`, `LEAVE_NEEDS_UNLOCK`, `EXPORT_NEEDS_UNLOCK`, `PLAIN_IMPORT_NEEDS_UNLOCK`, `GUIDED_IMPORT_NEEDS_LOCK`), so a UI can say what the pool needs rather than print a primitive's diagnostic. Nothing is written when one is thrown.
- **A `.db` file has to fit the database it is picked for.** Nothing checked, so a TodoDb backup could be imported into NotesDb and the app would query tables that were no longer there. Single-DB imports are now staged: the file lands under a temporary pool name, the host checks it against the target's model (`IHostDatabaseService.ValidateSchemaAsync`, implemented with the existing `DbContext.ValidateImportedSchemaAsync`), and only then is it promoted in one worker message. A rejected file leaves the target exactly as it was. `IEncryptedSqliteWasmDatabaseService.ImportDatabaseFromStreamAsync` takes the check as a `validateStaged` delegate; new coverage: `SingleDb_StagedImport_RejectedBySchemaCheck`.
- **Export is per row.** Saving one database is the row's own save button (a plain `.db` any SQLite tool opens); the tick boxes are for the bundle, whose button appears only once two or more are ticked and now says how many it will write. Emptying a database is a plain trash icon on its row.
- **Imports reconcile the schema.** `IHostDatabaseService` gains `MigrateAsync` and `OwnedDatabases`. Every successful import re-runs the host's migrations, so a file carrying an older schema is brought up to date and an owned database the import omitted is re-created before the next query opens a schema-less one in its place. Deleting a database the app owns now empties it rather than leaving a hole.
- The state × operation matrix, the rollback guarantees behind each format, and the close-before-replace contract are documented in `docs/crypto-vfs.md`.

**Breaking (`SqliteWasmBlazor.Crypto.UI`):** `IHostDatabaseService` implementations must add `OwnedDatabases`, `MigrateAsync` and `ValidateSchemaAsync`. `EncryptionModel.DatabaseNames` is replaced by `Databases` (rows carrying `Owned` / `Present`), `ExportDatabase` (per row) joins `ExportDatabases` (bundle, two or more), and `ProposeDatabaseName` is gone with the free-text import target.

**Breaking (`SqliteWasmBlazor.Crypto`):** `ImportDatabaseFromStreamAsync` gains a `validateStaged` parameter before `cancellationToken`. Callers passing the token positionally must name it.

### Memory-Flat Exports — OPFS Staging Replaces `byte[]` Downloads

Exporting used to materialise the entire database as a `byte[]`, hand it across the JS boundary, and wrap it in a Blob. That is fine for a few megabytes and fatal on mobile, where Safari kills the page rather than serve a large one. Exports now stage through OPFS instead: the worker writes the bytes into a staging file via a synchronous access handle — the same primitive the import path already uses for rekey-on-write — and the browser saves from that disk-backed `File`. Blobs built from `ArrayBuffer`s are held in process memory by WebKit; a `File` backed by an OPFS entry is disk-backed in every engine, so peak memory stays flat regardless of database size.

- Covers every export shape: single `.db`, the multi-DB `.dbs` envelope, and the encrypted-disk `.eds` envelope in `SqliteWasmBlazor.Crypto`.
- **New on the plain plane:** `ISqliteWasmDatabaseService.ExportDatabaseToDownloadAsync(databaseName, filename)` — a memory-flat `.db` download without the Crypto package. The worker closes the database first for a consistent snapshot, so the next context re-opens it.
- **Critical fix (export data loss):** uncheckpointed WAL data was silently omitted from encrypted disk exports. The worker now forces a proper VFS checkpoint before exporting.
- **Breaking:** `SqliteWasmBlazor.Components` no longer exposes `FileOperationsInterop.DownloadMessagePackFile` — the byte-array download it provided is exactly the memory profile this release removes. Use the staged export instead.
- Staging files are swept on worker start rather than after the click: an anchor download drains its `File` lazily, so deleting the entry at click time would corrupt the download. Retention is bounded to one session.

### Installed Web Apps: Saying What iOS Is About to Do

Installed on a Home Screen, an export arrives on a full-screen OS intermediate screen — an icon, a size, and "Open in &lt;app&gt;" — rather than as a saved file. Such a container has no downloads folder, so WebKit drops the `download` attribute and navigates to the file instead. Nothing is wrong with the export: WebKit streams the disk-backed staging file onto that screen, so it holds at any size, and its share menu saves to Files. It simply looks like a failure to anyone who was not told, so the demo's encryption page now says so above the export buttons — after an export the OS screen covers anything the page could add.

Handing the file to `navigator.share` was tried and rejected: the share sheet must materialise the file to pass it to another process, which dies on a large database, while the intermediate-screen path streams and does not. The presentation is the platform's; only the explanation was ours to fix.

### Demo: a Real Web-App Manifest and an Icon That Says What the App Is

The demo ships a `manifest.webmanifest` that was never linked from `index.html` and pointed at an `icon-512.png` that did not exist. It is now linked, complete, and correct: explicit `scope`, a `short_name` that fits under a Home Screen icon, a splash `background_color` matching the theme, and both icon sizes declared `any maskable`.

The stock Blazor template icon is replaced by a mark drawn for this app — a database cylinder for storage that outlives the session, a keyhole for the fact that what it stores is encrypted at rest, on the manifest's theme colour so the icon, splash screen and status bar are one colour. Source art is checked in as `icon.svg` (full detail) and `favicon.svg` (redrawn for 16-32px, where a disc seam is one shape too many); the PNGs are rendered from them with `rsvg-convert`. Everything sits inside the 80% safe circle, so cropping to a circle or a squircle loses nothing. `.svg` was added to the service worker's offline asset filter.

### Bulk Import: Guid Keys Now Land Where EF Core Can Find Them

Rows written through `ImportRowsAsync` listed and displayed correctly but could not be reached by their primary key: `FindAsync` returned null and `UPDATE ... WHERE Id = @p` affected zero rows, so a delete or an edit would report success and change nothing.

The ADO layer binds every `Guid` as uppercase TEXT, and EF Core generates its literals the same way, but the import path wrote the lowercase string MessagePack-CSharp produces — or a 16-byte BLOB when a caller passed a `sqlTypeOverrides` entry for the key column. SQLite's default BINARY collation makes `=` sensitive to both case and storage class, so those rows were unreachable while still reading back fine, because list queries carry no `Id` predicate and the reader decodes every form.

- The worker now writes uppercase TEXT for every `Guid`, whatever the declared column type. A `[Column(TypeName = "BLOB")]` annotation is not a reason to override: SQLite's BLOB affinity is "none", so a TEXT value stays TEXT in a BLOB column.
- **Action required:** databases populated by an earlier bulk import still hold the old representation. Those rows have to be re-imported; queries that don't filter on the key keep working, key-addressed ones don't.
- New TestApp coverage reaches an imported row by key (`BulkImport_RowsAreEfAddressable`) — the gap that let this ship.

### Naming: the Encryption Unit Is a Pool of Databases, Not a "Disk"

The API called its unit of encryption a *disk*, and the UI said the same in
English and German. Neither was accurate. What the library stores into is an
OPFS SAHPool: a fixed set of pre-allocated, opaquely-named OPFS files, each
carrying the logical path SQLite thinks it is using in a plaintext header
sector. There is no directory tree and no file-system semantics — the database
names a user sees exist only as strings inside those headers. The unit that
gets encrypted, locked and bound to a passkey is all of them together, which
is what needed a noun.

Every `Disk`-named symbol becomes `Pool`. `Databases` was not available: the
per-selection `ExportDatabases` / `ImportDatabases` commands already mean
something different.

- **Breaking, public API:** `EncryptedDiskState` → `EncryptedPoolState`,
  `DiskImportResult` → `PoolImportResult`, `DiskLockedException` →
  `PoolLockedException`, `ResetDiskAsync` → `ResetPoolAsync`,
  `ImportDiskGuidedFromStreamAsync` → `ImportPoolGuidedFromStreamAsync`,
  `ExportDiskToPubkeyAndDownloadAsync` → `ExportPoolToPubkeyAndDownloadAsync`.
- **Breaking, host registration:** `UseEncryptedDiskLifecycle()` →
  `UseEncryptedPoolLifecycle()`. One call site in a typical `Program.cs`.
- **Breaking, `EncryptionModel` commands:** `ImportDisk` → `ImportPool`,
  `ExportDiskBackup` → `ExportPoolBackup`, `ExportDiskForRecipient` →
  `ExportPoolForRecipient`, with their `CanX` companions.
- The worker protocol moves with it — the `[JSImport]` names and the
  `readDiskManifest` / `writeDiskManifest` / `clearDiskManifest` message types
  are renamed on both sides at once. Consumers who only use the C# API are
  unaffected; anyone driving the worker directly is not.
- The `.db` / `.dbs` / `.eds` file extensions are **unchanged**. Renaming
  `.eds` would orphan every backup already exported.

**UI copy no longer names the container at all.** It says what the user has —
"all databases" / "alle Datenbanken" — because a pool is an implementation
detail and a user has databases, not a storage substrate. Only the file
extensions survive as technical terms. German copy dropped *Festplatte*
outright, which described a hard drive.

### Demo: One Reset, Reachable When It Is Actually Needed

The demo had two resets. The Administration page's sat inside
`<AuthorizeView Policy="DatabaseOpen">` and fired on a single unconfirmed
click; the encryption page had a second one, worded differently. The gated one
was unreachable in precisely the situation a reset exists for: an encrypted
pool whose passkey is gone can never satisfy that policy.

Both collapse into one command on the Administration page, outside the policy
gate, in its own card away from the row of todo-maintenance buttons it used to
sit in, behind the destructive-confirm dialog. Its label, hint and confirmation
follow the encryption state — an encrypted pool loses its key along with its
data, and the button now says so before it is clicked.

### Formal Verification (Tamarin)

100% formal verification of the cryptographic state transitions and key lifecycle invariants using the Tamarin Prover.

### Other Fixes

- **Bug Fix (#20):** Fixed a documentation error in the Quick Start guide that erroneously instructed users to register a non-existent `IDBInitializationService`.
- **Feature (#18):** Made SQL command logging strictly opt-in via `SqliteWasmOptions.EnableCommandSqlLogging` to prevent sensitive schema/data leakage in production (Reported & suggested by @bearyung).

### Dependencies & Tooling

- .NET / EF Core `10.0.11`, MudBlazor `9.8.0`, MessagePack `3.1.8`, R3 `1.3.1`, Playwright `1.62.0`, Test SDK `18.9.0`, xunit.runner.visualstudio `4.0.0`, PolySharp `1.16.0`, BouncyCastle `2.7.0`, SourceLink `10.0.400`.
- Build now targets the .NET SDK `10.0.400` band (`global.json`), on runtime `10.0.11`.
- Roslyn (`Microsoft.CodeAnalysis.*`) moves to `5.6.0` — the newest published Roslyn, and below the `5.9.0` compiler the SDK ships, so generators and analyzers never ask for a Roslyn newer than the one loading them.
- TypeScript `6.0.3`, ESLint `10.8.1` + typescript-eslint `8.67.0`, msgpackr `2.0.5`, esbuild `0.28.2`, vitest `4.1.10`.
- TypeScript stays on the 6.0 line: typescript-eslint 8.x peer-caps `typescript <6.1.0`, so TS 7 (the native port) waits on typescript-eslint support.
- The `@sqlite.org/sqlite-wasm` patch is ported to `3.53.0-build1` and adds `getFileSize` / `exportFileSlice` to the vendor SAHPool VFS, which is what lets the plain plane export in slices.
- The native stub now reports SQLite `3.53.0`, matching the worker engine that actually answers — `Microsoft.Data.Sqlite` gates features on `sqlite3_libversion_number`.
- `SQLitePCLRaw.lib.e_sqlite3` moves to the SQLite-versioned `3.53.3` package. Its `.a` is excluded and replaced by the stub, so only the provider's P/Invoke surface matters.
- `build_stub.sh` falls back to the .NET wasm-tools workload's Emscripten pack when no standalone emsdk is present — the same toolchain the Blazor native relink uses.

## Version 0.9.0-pre

### CSP Hardening + Options-Driven Configuration

Removes the `data:text/javascript` JS import previously used to auto-probe `<base href>` from the DOM — blocked under a strict `script-src` Content-Security-Policy — and replaces it with explicit, DI-resolved options.

**New configuration shape:**

```csharp
// Root-path deployment — no change needed
builder.Services.AddSqliteWasm();

// Sub-path deployment — derive BaseHref from <base href>
builder.Services.AddSqliteWasm(o => o.BaseHref = new Uri(builder.HostEnvironment.BaseAddress).AbsolutePath);

// Browser-extension build — override the static-asset root
builder.Services.AddSqliteWasm(o => o.AssetRoot = "content/SqliteWasmBlazor/");
```

**`SqliteWasmOptions`:**
- `BaseHref` — origin-side path prefix, default `"/"`. For sub-path deployments set to `new Uri(builder.HostEnvironment.BaseAddress).AbsolutePath`.
- `AssetRoot` — segment between `BaseHref` and package files, default `"_content/SqliteWasmBlazor/"`

**Companion packages** gained the same pattern:
- `SqliteWasmBlazor.Components`: `FileOperationsInterop.InitializeAsync(Action<SqliteWasmComponentsOptions>?)`
- `SqliteWasmBlazor.FloatingWindow`: `services.AddFloatingWindow(Action<FloatingWindowOptions>?)`

**Behaviour change:** calling a database operation before `InitializeSqliteWasmAsync` / `InitializeSqliteWasmDatabaseAsync<T>` now throws `InvalidOperationException` with a clear message, instead of silently lazy-initialising with default settings (which 404'd on sub-path / extension builds).

**CSP samples:** `AdoNetSample`, `Demo`, and `TestApp` `index.html` now ship a strict `Content-Security-Policy` meta tag (`script-src 'self' 'wasm-unsafe-eval'`, `object-src 'none'`, `base-uri 'self'`) to validate the hardened init path.

**Tests:** new `SubPathTests` Playwright E2E coverage runs the full TestApp under `/myapp/`, asserting the `<base href>` → `HostEnvironment.BaseAddress` → `BaseHref` chain works end-to-end and that no CSP violations fire.

**Acknowledgement:** the design was sparked by [@astrema](https://github.com/astrema)'s [PR #16](https://github.com/b-straub/SqliteWasmBlazor/pull/16), which raised the CSP and sub-path issues and proposed the original `baseHref` parameter. The sub-path E2E test infrastructure is ported from that PR.

## Version 0.8.3-pre

### V2 Worker-Side Bulk Import/Export

Replaced per-statement SQL round-trips with worker-side prepared statement loops for dramatically faster bulk operations.

**Key improvements:**
- **10-50x faster import** — worker does `db.prepare()` + `stmt.bind()/step()/reset()` loop instead of ~800 individual `ExecuteSqlRawAsync` calls
- **Worker-side export** — SELECT + pack happens entirely in the worker, one round-trip
- **Memory-safe streaming** — import streams raw MessagePack bytes without C# deserialization, bounded memory per batch
- **Self-describing V2 format** — header carries column metadata, table name, primary key, and C# type info so the worker builds SQL autonomously
- **Multi-part export** — large databases split into delta-sized parts with a meta file, adaptive part sizing from configurable MB limit
- **Full type coverage** — Guid (TEXT/BLOB), DateTime, TimeSpan, DateTimeOffset, decimal, char, enum, JSON collections, BigInt-safe int64
- **Cancellation support** — all operations cancellable via CancellationToken

**New API methods:**
- `BulkImportAsync` — send V2 payload to worker for prepared statement insertion
- `BulkExportAsync` — worker queries SQLite and returns V2 MessagePack binary
- `BulkExportMetadata` — typed record ensuring all export fields are defined
- `ConflictResolutionStrategy` — enum (None/LastWriteWins/LocalWins/DeltaWins)

**Bug fixes:**
- Fix int64 precision loss: `long` values > `Number.MAX_SAFE_INTEGER` sent as text in EF Core parameters
- Fix .NET Guid byte order (little-endian groups 1-3) in both import and export
- Fix `sqlite3_column_int64` boundary errors via SQLITE_TEXT workaround
- Fix `AllTypesRoundTripTest` using change tracker cache instead of actual SQLite read

### Seed Server

PHP REST API + Blazor UI component for cloud-based database provisioning.

- Upload current database to server (multi-part, with progress)
- Download seed from server and import (with progress and cancellation)
- Adaptive cloud part sizing synced from server PHP limits at build time
- Server connectivity check with setup instructions

See [Seed Server docs](docs/seed-server.md) for setup instructions.

## Version 0.7.2-pre

### Breaking Change: Stable Public API

`SqliteWasmWorkerBridge` is now `internal`. Database management operations are exposed through the new `ISqliteWasmDatabaseService` interface via dependency injection.

**Migration steps:**

1. Add service registration in `Program.cs`:
   ```csharp
   builder.Services.AddSqliteWasm();
   ```

2. Inject the interface in components:
   ```csharp
   @inject ISqliteWasmDatabaseService DatabaseService
   ```

3. Replace direct calls:
   ```csharp
   // Before
   await SqliteWasmWorkerBridge.Instance.DeleteDatabaseAsync("MyDb.db");

   // After
   await DatabaseService.DeleteDatabaseAsync("MyDb.db");
   ```

**Available methods on `ISqliteWasmDatabaseService`:**
- `ExistsDatabaseAsync(string databaseName)` - Check if database exists in OPFS
- `DeleteDatabaseAsync(string databaseName)` - Delete database from OPFS
- `RenameDatabaseAsync(string oldName, string newName)` - Rename database (atomic)
- `CloseDatabaseAsync(string databaseName)` - Close database connection in worker
- `ImportDatabaseAsync(string databaseName, byte[] data)` - Import raw .db file into OPFS
- `ExportDatabaseAsync(string databaseName)` - Export raw .db file from OPFS

This change encapsulates internal implementation details and provides a stable API surface for future versions.

---

## Raw Database Import/Export

Export and import complete SQLite .db files directly from/to OPFS. Unlike the MessagePack-based import/export (which serializes individual records), this transfers the raw database file as-is — preserving all tables, indexes, FTS5 virtual tables, triggers, and migration history.

### API

```csharp
@inject ISqliteWasmDatabaseService DatabaseService

// Export: closes DB for consistent snapshot, returns raw bytes
byte[] data = await DatabaseService.ExportDatabaseAsync("TodoDb.db");

// Import: writes raw .db file to OPFS (validates SQLite header)
await DatabaseService.ImportDatabaseAsync("TodoDb.db", data);
```

**Important:** Both operations close the database in the worker. The connection state tracking (`IsDatabaseOpen`) ensures subsequent EF Core queries automatically re-open the database — no manual re-open needed.

### Schema Validation

After importing a raw .db file, validate that it has the correct schema before use:

```csharp
using SqliteWasmBlazor.Models.Extensions;

await using var ctx = await DbContextFactory.CreateDbContextAsync();
await ctx.ValidateSchemaAsync(); // throws InvalidOperationException if tables are missing
```

`ValidateSchemaAsync` reads expected table names from the EF model metadata (`GetEntityTypes()` + `GetTableName()`) and checks them against `sqlite_master`. This catches incompatible databases (e.g., importing a file from a different application) with a clear error message listing the missing tables.

### Safe Import with Backup/Restore

The demo app implements a safe import pattern with transient backup:

```csharp
// 1. Backup existing database
await DatabaseService.CloseDatabaseAsync("TodoDb.db");
await DatabaseService.RenameDatabaseAsync("TodoDb.db", "TodoDb.backup.db");

// 2. Import new file
await DatabaseService.ImportDatabaseAsync("TodoDb.db", data);

// 3. Validate schema
try
{
    await using var ctx = await DbContextFactory.CreateDbContextAsync();
    await ctx.ValidateSchemaAsync();

    // 4. Success — delete backup
    await DatabaseService.DeleteDatabaseAsync("TodoDb.backup.db");
}
catch (InvalidOperationException)
{
    // 5. Failed — restore from backup
    await DatabaseService.DeleteDatabaseAsync("TodoDb.db");
    await DatabaseService.RenameDatabaseAsync("TodoDb.backup.db", "TodoDb.db");
    throw;
}
```

### Connection State Tracking

The worker bridge tracks which databases are open on the worker side. `SqliteWasmConnection.State` reflects the actual worker state, not just the C#-side `_state` field. This prevents stale connection issues after import/export/delete/rename operations:

```
Operation Flow:
├─ ExportDatabaseAsync("TodoDb.db")     → worker closes DB → bridge marks as not open
├─ EF Core query via DbContextFactory   → State returns Closed (bridge says not open)
│  └─ EF Core calls OpenAsync           → bridge sends open to worker → DB reopened
├─ Query executes successfully           → worker has DB open
```

Without this tracking, EF Core would see `State == Open` (stale from before export), skip `OpenAsync`, and send SQL to a worker that has the DB closed.

### SAH Pool Capacity

The OPFS SAH pool `initialCapacity` only applies on first creation. For existing pools, `reserveMinimumCapacity(10)` grows the pool to handle backup files during import:

```
Capacity math: 2 DBs × 3 files (db + shm + wal) = 6 normal + backup + journal headroom = 10
```

### Demo App

The `DatabaseImportExport.razor` page in the demo app provides a complete UI with:
- Export button (downloads timestamped .db file)
- Import with file picker (.db filter)
- Confirmation dialog for destructive replace (red "Replace Database" button)
- Schema validation with automatic backup/restore on failure
- Snackbar notifications for success/error states

---

## Incremental Database Export/Import (Delta Sync)

File-based incremental export/import for large databases in offline-first PWAs. Export only changed items since last checkpoint, transfer the file manually (USB, cloud storage, etc.), and import with conflict resolution:

```csharp
// Export only changes since last checkpoint (delta export)
<MessagePackFileDownload T="TodoItemDto"
    GetPageAsync="@GetDeltaTodoItemsPageAsync"  // Only items modified since checkpoint
    GetTotalCountAsync="@GetDeltaCountAsync"
    FileName="@($"delta-{DateTime.Now:yyyyMMdd}.msgpack")"
    Mode="ExportMode.Delta" />  // Delta mode includes UpdatedAt/DeletedAt ranges

// Import with conflict resolution strategy
<MessagePackFileUpload T="TodoItemDto"
    OnBulkInsertAsync="@DeltaMergeTodoItemsAsync"  // Smart merge instead of replace
    Mode="ImportMode.Delta"
    ConflictResolution="ConflictResolutionStrategy.LastWriteWins" />  // Or LocalWins/DeltaWins
```

### Key Features

**Automatic Checkpoint Management**
- Auto checkpoints created after every import/export operation
- Manual checkpoints with tombstone cleanup
- Checkpoint history with timestamp, description, and item counts
- Restore to any checkpoint with optional delta reapply

**Efficient Delta Tracking**
- Only exports items modified since last checkpoint (`UpdatedAt > lastCheckpointTime`)
- Includes soft-deleted items (tombstones) for proper sync
- Pending delta count shows items awaiting export
- Significantly reduces data transfer for large databases

**Three Conflict Resolution Strategies**
- **LastWriteWins** (default): Most recent `UpdatedAt` timestamp wins
- **LocalWins**: Local changes always preserved, imports only add new items
- **DeltaWins**: Imported changes always win, local items overwritten

**Soft Delete (Tombstones)**
- Items marked with `IsDeleted` flag instead of hard deletion
- `DeletedAt` timestamp tracks deletion time for delta sync
- Tombstones included in delta export for proper deletion propagation
- Manual tombstone cleanup before creating manual checkpoints

### Architecture

```
Database Timeline:
├─ Checkpoint 1 (Manual)     ← Baseline: 100 active items, 0 tombstones
│  └─ Created 10 items       ← UpdatedAt = 2025-11-17 10:00
│  └─ Deleted 2 items        ← DeletedAt = 2025-11-17 10:05
├─ Delta Export              ← Exports 12 items (10 new + 2 deleted)
├─ Checkpoint 2 (Auto)       ← Auto checkpoint: 108 active, 2 tombstones
│  └─ Import 5 items         ← Conflict resolution applied
├─ Checkpoint 3 (Auto)       ← Auto checkpoint after import
│  └─ Created 3 items        ← UpdatedAt = 2025-11-17 10:30
├─ Pending Delta: 3 items    ← Awaiting next export
```

### Conflict Resolution Examples

```csharp
// LastWriteWins: Compare timestamps
Local:    UpdatedAt = 2025-11-17 10:00, Title = "Local Edit"
Imported: UpdatedAt = 2025-11-17 10:05, Title = "Remote Edit"
Result:   Title = "Remote Edit" (newer timestamp wins)

// LocalWins: Keep local changes
Local:    Title = "My Local Changes"
Imported: Title = "Remote Changes"
Result:   Title = "My Local Changes" (local always wins)

// DeltaWins: Always accept imported
Local:    Title = "Local Changes", UpdatedAt = 2025-11-17 10:05
Imported: Title = "Remote Changes", UpdatedAt = 2025-11-17 09:00 (older!)
Result:   Title = "Remote Changes" (delta wins despite older timestamp)
```

### Database Schema Requirements

```csharp
public class TodoItem
{
    public Guid Id { get; set; }
    public string Title { get; set; }
    public DateTime UpdatedAt { get; set; }        // Required for delta sync
    public bool IsDeleted { get; set; }            // Soft delete flag
    public DateTime? DeletedAt { get; set; }       // Deletion timestamp
}

public class SyncState  // Checkpoint tracking
{
    public int Id { get; set; }
    public DateTime CreatedAt { get; set; }        // Checkpoint timestamp
    public string Description { get; set; }
    public int ActiveItemCount { get; set; }
    public int TombstoneCount { get; set; }
    public string CheckpointType { get; set; }     // "Auto" or "Manual"
}
```

### Implementation Pattern

```csharp
// Delta export query
private async Task<(List<TodoItemDto> Items, int TotalCount)> GetDeltaTodoItemsPageAsync(
    int skip, int take)
{
    await using var context = await DbContextFactory.CreateDbContextAsync();

    // Get last checkpoint timestamp
    var lastCheckpoint = await context.SyncState
        .OrderByDescending(s => s.CreatedAt)
        .FirstOrDefaultAsync();

    var lastCheckpointTime = lastCheckpoint?.CreatedAt ?? DateTime.MinValue;

    // Query items modified since checkpoint (including soft-deleted)
    var query = context.TodoItems
        .Where(t =>
            (t.UpdatedAt > lastCheckpointTime && !t.IsDeleted) ||  // Modified items
            (t.IsDeleted && t.DeletedAt.HasValue && t.DeletedAt.Value > lastCheckpointTime))  // Deletions
        .OrderBy(t => t.UpdatedAt);

    var totalCount = await query.CountAsync();
    var items = await query
        .Skip(skip)
        .Take(take)
        .Select(t => t.ToDto())
        .ToListAsync();

    return (items, totalCount);
}

// Delta import with conflict resolution
private async Task DeltaMergeTodoItemsAsync(List<TodoItemDto> dtos)
{
    await using var context = await DbContextFactory.CreateDbContextAsync();

    foreach (var dto in dtos)
    {
        var existingItem = await context.TodoItems
            .FirstOrDefaultAsync(t => t.Id == dto.Id);

        if (existingItem is not null)
        {
            // Apply conflict resolution strategy
            var shouldUpdate = _conflictResolution switch
            {
                ConflictResolutionStrategy.LastWriteWins => dto.UpdatedAt > existingItem.UpdatedAt,
                ConflictResolutionStrategy.LocalWins => false,  // Never update
                ConflictResolutionStrategy.DeltaWins => true,   // Always update
                _ => throw new InvalidOperationException($"Unknown strategy: {_conflictResolution}")
            };

            if (shouldUpdate)
            {
                // Update existing item
                existingItem.Title = dto.Title;
                existingItem.UpdatedAt = dto.UpdatedAt;
                existingItem.IsDeleted = dto.IsDeleted;
                existingItem.DeletedAt = dto.DeletedAt;
            }
        }
        else
        {
            // Add new item
            context.TodoItems.Add(dto.ToEntity());
        }
    }

    await context.SaveChangesAsync();

    // Create auto checkpoint after import
    await context.CreateCheckpointAsync(
        $"Auto checkpoint after delta import ({dtos.Count} items)",
        "Auto");
}

// Checkpoint creation extension method
public static async Task<SyncState> CreateCheckpointAsync(
    this TodoDbContext context,
    string description,
    string checkpointType = "Auto",
    CancellationToken cancellationToken = default)
{
    var activeCount = await context.TodoItems
        .CountAsync(t => !t.IsDeleted, cancellationToken);

    var tombstoneCount = await context.TodoItems
        .CountAsync(t => t.IsDeleted, cancellationToken);

    var checkpoint = new SyncState
    {
        CreatedAt = DateTime.UtcNow,
        Description = description,
        ActiveItemCount = activeCount,
        TombstoneCount = tombstoneCount,
        CheckpointType = checkpointType
    };

    context.SyncState.Add(checkpoint);
    await context.SaveChangesAsync(cancellationToken);

    return checkpoint;
}
```

### What This Is

A file-based incremental backup/restore system for large databases. Useful when you need to:
- Transfer only changes between devices (vs. transferring entire database)
- Keep incremental backups with restore points
- Reduce file transfer size for large databases (100k+ records)
- Handle conflicts when merging changes from different sources

### What This Is NOT

This is **not** a real-time sync solution. It requires:
- Manual file transfer (download delta → copy file → upload delta on other device)
- No automatic sync between devices/users

For real-time/automatic sync, see the [Datasync TodoApp](https://github.com/b-straub/Datasync/tree/main/samples/todoapp-blazor-wasm-offline) sample which demonstrates proper offline-first synchronization patterns.

### Use Cases

- **Offline-First PWAs**: Export changes before going offline, import when back online
- **Multi-Device Transfer**: Manually share database state via file transfer
- **Incremental Backups**: Keep checkpoint history with smaller backup files
- **Data Migration**: Move data between environments with conflict handling

### Best Practices

1. Always store timestamps in UTC (`DateTime.UtcNow`)
2. Display timestamps in local time (`ToLocalTime()`)
3. Set `UpdatedAt` on every entity modification
4. Use soft delete for entities that need sync
5. Clean tombstones before manual checkpoints
6. Choose conflict resolution strategy based on use case:
   - **LastWriteWins**: Most recent edit wins (general purpose)
   - **LocalWins**: User's local edits are sacred (offline-first apps)
   - **DeltaWins**: Server/remote is source of truth (cloud sync)

### Future Direction

This foundation could be extended toward decentralized sync solutions, but currently it's a building block for offline-first scenarios, not a complete sync system.

See the Demo app's Administration and TodoImportExport components for complete implementation examples.

---

## Database Import/Export

Export and import your entire database with schema validation and efficient binary serialization:

```csharp
// Export database to MessagePack file
<MessagePackFileDownload T="TodoItemDto"
    GetPageAsync="@GetTodoItemsPageAsync"
    GetTotalCountAsync="@GetTodoItemCountAsync"
    FileName="@($"backup-{DateTime.Now:yyyyMMdd}.msgpack")"
    SchemaVersion="1.0"
    AppIdentifier="MyApp" />

// Import database with validation
<MessagePackFileUpload T="TodoItemDto"
    OnBulkInsertAsync="@BulkInsertTodoItemsAsync"
    ExpectedSchemaVersion="1.0"
    ExpectedAppIdentifier="MyApp" />
```

### Features

- **Schema Validation** - Prevents importing incompatible data with version and app identifier checks
- **Efficient Serialization** - MessagePack binary format (60% smaller than JSON)
- **Streaming Export** - Handles large datasets with pagination (tested with 100k+ records)
- **Bulk Import** - Optimized SQL batching respects SQLite's 999 parameter limit
- **Progress Tracking** - Real-time progress updates during import/export operations
- **Type Safety** - Full DTO validation ensures data integrity

Perfect for:
- Database backups and restores
- Data migration between environments
- Sharing datasets between users
- Offline-first PWA scenarios

### How it works

Export streams data in MessagePack format with a file header (magic number "SWBMP", schema version, type info, record count) followed by serialized items. Import deserializes the stream in batches, validates the header, and uses raw SQL INSERT statements to preserve entity IDs while respecting SQLite's 999 parameter limit (166 rows per batch for 6-column entities). The header-first approach ensures schema compatibility before processing begins, preventing partial imports of incompatible data.

### Why sqlite-wasm needed patching

The official sqlite-wasm OPFS SAHPool VFS lacked a `renameFile()` implementation. The patch (`patches/@sqlite.org+sqlite-wasm+3.50.4-build1.patch`) adds this method to enable efficient database renaming by updating the SAH (Synchronous Access Handle) metadata mapping with the new path while keeping the physical file intact - avoiding expensive file copying for large databases.

See the Demo app's TodoImportExport component for a complete implementation example.

---

## Version 0.6.7-pre (2025-11-14)

### Log Level Configuration Change

The `SqliteWasmConnection` constructor now uses the standard `Microsoft.Extensions.Logging.LogLevel` enum instead of the custom `SqliteWasmLogLevel`:

```csharp
// Old (0.6.6-pre and earlier)
var connection = new SqliteWasmConnection("Data Source=MyDb.db", SqliteWasmLogLevel.Warning);

// New (0.6.7-pre and later)
using Microsoft.Extensions.Logging; // Add this using

// Default is LogLevel.Warning, so you can omit it:
var connection = new SqliteWasmConnection("Data Source=MyDb.db");

// Or specify a different level:
var connection = new SqliteWasmConnection("Data Source=MyDb.db", LogLevel.Error);
```

**Migration:** Simply add `using Microsoft.Extensions.Logging;` and change `SqliteWasmLogLevel` to `LogLevel`. If you were using the default `Warning` level, you can omit the parameter entirely.

Available log levels: `Trace`, `Debug`, `Information`, `Warning` (default), `Error`, `Critical`, `None`
