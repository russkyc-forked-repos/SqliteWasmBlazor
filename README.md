# SqliteWasmBlazor

**The first known solution providing true filesystem-backed SQLite database with full EF Core support for Blazor WebAssembly.**

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![.NET](https://img.shields.io/badge/.NET-10.0-purple.svg)](https://dotnet.microsoft.com/)
[![NuGet](https://img.shields.io/nuget/vpre/SqliteWasmBlazor)](https://www.nuget.org/packages/SqliteWasmBlazor)
[![GitHub Repo stars](https://img.shields.io/github/stars/b-straub/SqliteWasmBlazor)](https://github.com/b-straub/SqliteWasmBlazor/stargazers)

**[Try the Live Demo](https://b-straub.github.io/SqliteWasmBlazor/)** - Experience persistent SQLite database in your browser! Can be installed as a Progressive Web App (PWA) for offline use.

## Related Projects

SqliteWasmBlazor is part of a family of libraries for building offline-first Blazor applications:

| Project | Description |
|---------|-------------|
| **[RxBlazorV2](https://github.com/b-straub/RxBlazorV2)** | Reactive programming framework for Blazor built on [R3](https://github.com/Cysharp/R3). Uses Roslyn source generators for observable models with reactive property bindings, command patterns, and automatic component generation. |
| **[BlazorPRF](https://github.com/b-straub/BlazorPRF)** | _Absorbed into this repo as `SqliteWasmBlazor.Crypto`._ Originally a standalone library for PRF-based deterministic encryption in Blazor WebAssembly via the WebAuthn PRF extension; the primitives + key-derivation flow now ship as the `SqliteWasmBlazor.Crypto` package and back the encryption VFS directly. |

Together these enable a complete offline-first stack: persistent local storage with optional at-rest encryption (`SqliteWasmBlazor` + `SqliteWasmBlazor.Crypto`) + reactive state management (`RxBlazorV2`).

**Coming soon:** **CryptoSync** — end-to-end encrypted multi-device delta sync, built on top of `SqliteWasmBlazor.Crypto`. Per-row delta sync with permission enforcement, per-group keys, an admin invitation flow, and a whitelist-authenticated PHP relay that never sees plaintext.

## About This Project

This is a non-commercial hobby project maintained in my spare time - no fixed update cycle or roadmap. However, "hobby" refers to time and commitment, not craftsmanship: the project is developed with professional standards including proper test coverage and attention to code quality.

Open source thrives on community involvement. The project grows through bug reports, feature requests, pull requests, and real-world feedback. If you're considering this for production use, I'd encourage you to contribute back - that's how open source stays alive, not through promises from a single maintainer, but through shared ownership.

### Stability & Status

The public API surface is intentionally kept minimal to reduce the risk of breaking changes. While the API has been stable in practice, this project is pre-1.0: broader real-world feedback is needed before committing to long-term API guarantees. Contributions and usage reports help move toward that goal.

## What's New

### 0.9.3-pre — Memory-flat file paths, validated imports, `Disk` → `Pool`

- **Every file path is memory-flat, on the plain plane** —
  `ISqliteWasmDatabaseService` now carries the whole set: one database or many,
  in or out, to a `Stream` or straight to a download. None of them holds the
  file in managed memory, so a 250 MB database transfers on a phone without the
  Crypto package.

  ```csharp
  await DatabaseService.ExportDatabaseToDownloadAsync("TodoDb.db", "backup.db");
  await DatabaseService.ExportDatabasesToDownloadAsync(names, "backup.dbs");
  await DatabaseService.ExportDatabaseToStreamAsync("TodoDb.db", destination);
  await DatabaseService.ImportDatabaseFromStreamAsync("TodoDb.db", src, size, validate);
  await DatabaseService.ImportDatabasesFromStreamAsync(src, size, validate);
  ```

  The worker behind them is state-aware on its own: with the Crypto package
  loaded it rekeys on the way in and decrypts on the way out, so the same calls
  serve a plain pool and an encrypted one.
- **Exports stage through OPFS** — the worker writes the bytes into a staging
  file through a synchronous access handle and the browser saves from that
  disk-backed `File`, so peak memory stays flat whatever the database weighs.
  Covers `.db`, `.dbs` and `.eds`.
- **Large imports are pushed into the worker** — C# streams the picked file
  one chunk at a time instead of assembling it as a `Blob` first, so neither
  heap ever holds the whole database. That assembly is what made large `.db`
  imports fail on iPadOS with `AccessHandle is closed`.
- **What an import replaces is parked, never overwritten** — park and restore
  are one pool-level replace instead of delete-then-rename, and a park whose
  database is missing is restored (at the next import, and at worker init)
  rather than swept.
- **Imports are validated before they count** — the incoming file is written
  under the database's real name, opened, and checked by the host
  (`IHostDatabaseService.ValidateSchemaAsync`); a refusal renames the parks
  back, metadata-only, so what returns is byte-identical. Every successful
  import re-runs the host's migrations (`MigrateAsync`) and re-creates owned
  databases the file omitted (`OwnedDatabases`) — inside the import path, so it
  holds whether or not you use the drop-in UI.
- **One scope per affordance** — the drop-in UI gives each database its own
  row (save it, replace it from a `.db`, empty it) and moves whole-pool
  operations to their own card. Pool-state preconditions now throw
  `PoolOperationRejectedException` with a typed `Reason`, so a host can say
  what the pool needs instead of printing a primitive's diagnostic.
- **Bulk-imported `Guid` keys are EF-addressable** — `ImportRowsAsync` writes
  uppercase TEXT for every `Guid`, whatever the declared column type.
  **Action required:** rows written by an earlier bulk import cannot be
  reached by primary key and have to be re-imported.
- **SQL command logging is opt-in** via `SqliteWasmOptions.EnableCommandSqlLogging`,
  so schema and parameter values no longer reach the browser console in
  production ([#18](https://github.com/b-straub/SqliteWasmBlazor/issues/18)).
- **XML documentation ships with the packages** — the public API is documented
  end to end and `CS1591` is an error, so IntelliSense works against the NuGet
  packages.
- **`Disk` → `Pool` across the public API** — what gets encrypted, locked and
  bound to a passkey is an OPFS SAHPool of databases, not a disk. The `.db` /
  `.dbs` / `.eds` extensions are unchanged. See [Breaking Changes](#breaking-changes).

### Passkey-derived encryption (Plane 2)

Optional at-rest encryption for OPFS-backed SQLite databases. The host opts
in by registering a 32-byte key with the worker; without that the same VFS
falls through to byte-for-byte vendor SAHPool behavior. With the key
registered the entire encryption layer engages:

- **Page-level AEAD** — every 4 096-byte SQLite slot is sealed with
  ChaCha20-Poly1305 before it reaches OPFS. The physical slot is
  `[ciphertext(4096) | nonce(12) | tag(16)]`; the nonce is fresh CSPRNG
  bytes per write.
- **AAD-bound to slot identity** — the AEAD's associated data binds
  `(versionTag, dbPath, slotIndex)`, so a tampered or relocated page fails
  authentication. Cross-database and cross-slot page swaps are rejected on
  read; legacy or wrong-version ciphertext is rejected outright.
- **WebAuthn-PRF key derivation** — the global key is derived from a
  passkey via the WebAuthn PRF extension, using the absorbed
  [BlazorPRF](https://github.com/b-straub/BlazorPRF) primitives that now ship
  inside `SqliteWasmBlazor.Crypto`. No password, no client-side key file; the
  authenticator does the unlock.
- **Verified unlock, not silent** — a slot-0 AEAD probe gates unlock, and a
  manifest MAC binds the stored pool state to the credential. Wrong key, wrong
  credential, or tampered manifest fail loudly before any decryption hits
  the page cache.
- **Whole-pool envelope export (`.eds`)** — encrypted dumps wrap the slot
  key under a recipient X25519 pubkey (ECIES). Carries a `credentialId`
  hint so the receiver's UI can pick the right passkey automatically.
- **Guided import primitive** — a single click on a `.eds` runs the whole
  ritual: preflight, wipe, EnterEncrypted, rekey-import, manifest rebind.
  Works from a Plain or Locked pool state; mistargeted envelopes are
  rejected before the current pool is touched.
- **Plain-ZIP import on an encrypted pool** — state-aware: a Locked pool
  breaks to plain (recovery path), an Unlocked pool re-encrypts on write
  (passkey binding survives). Preflight validates SQLite shape + page
  geometry before any wipe.
- **Drop-in host UI** — `SqliteWasmBlazor.Crypto.UI` ships the
  Authentication / Encryption / DatabaseErrorAlert / SessionExpired panels,
  fully RxBlazorV2-based and resx-localized (en + de).
- **Offline test adapter** — `SqliteWasmBlazor.Crypto.BouncyCastle` mirrors
  the in-browser primitives in pure C# for tooling and integration tests
  that need to run without a browser.
- **Formally verified** — 3 Tamarin theories under `docs/formal/vfs-tamarin/`
  cover per-slot AEAD soundness, in-place lifecycle, and key-cache /
  manifest unlock. 74 lemmas, all verified; `docs/formal/verify.sh` runs the
  gate, `docs/formal/mutation-check.sh` checks the models still bite.

Full reference: [`docs/crypto-vfs.md`](docs/crypto-vfs.md). Threat model
and assurance summary: [`docs/security/`](docs/security/README.md).

### Other recent additions

- **V2 Worker-Side Bulk Import/Export** - Worker-side prepared statement loops for 10-50x faster import. Self-describing V2 MessagePack format with column metadata. Memory-safe streaming for large datasets [(details)](CHANGELOG.md#v2-worker-side-bulk-importexport)
- **Multi-Part Export** - Large databases automatically split into manageable parts with a meta file. Adaptive part sizing based on configurable MB limit
- **Raw Database Import/Export** - Export and import complete .db files directly from/to OPFS with schema validation and automatic backup/restore on failure [(details)](CHANGELOG.md#raw-database-importexport)
- **Multi-Database Support** - Run multiple independent SQLite databases simultaneously in the same Web Worker [(details)](docs/multi-database.md)
- **Incremental Database Export/Import** - File-based delta sync with checkpoint management and conflict resolution [(details)](CHANGELOG.md#incremental-database-exportimport-delta-sync)
- **Real-World Sample** - Check out the [Datasync TodoApp](https://github.com/b-straub/Datasync/tree/main/samples/todoapp-blazor-wasm-offline) for offline-first data synchronization with SqliteWasmBlazor

## Breaking Changes

- **v0.9.3-pre** — the unit of encryption is a *pool* of databases, not a *disk*.
  Every `Disk`-named public symbol is renamed; the `.db` / `.dbs` / `.eds` file
  extensions are unchanged (renaming those would orphan existing backups).

  | Before | After |
  |--------|-------|
  | `EncryptedDiskState` | `EncryptedPoolState` |
  | `DiskImportResult` | `PoolImportResult` |
  | `DiskLockedException` | `PoolLockedException` |
  | `ResetDiskAsync` | `ResetPoolAsync` |
  | `ImportDiskGuidedFromStreamAsync` | `ImportPoolGuidedFromStreamAsync` |
  | `ExportDiskToPubkeyAndDownloadAsync` | `ExportPoolToPubkeyAndDownloadAsync` |
  | `UseEncryptedDiskLifecycle()` | `UseEncryptedPoolLifecycle()` |
  | `EncryptionModel.ImportDisk` / `ExportDiskBackup` / `ExportDiskForRecipient` | `ImportPool` / `ExportPoolBackup` / `ExportPoolForRecipient` |

  The worker message protocol is renamed on both sides at once, so anyone driving
  the worker directly is affected; consumers who only use the C# API are not.

  Also breaking in this release:

  - Every managed-`byte[]` file method on `ISqliteWasmDatabaseService` is gone —
    holding a whole database in memory is exactly the profile this release removes.
    The replacements are on the same interface, so nothing has to take another
    package:

    | Before | After |
    |--------|-------|
    | `ExportAllDatabasesAsync` | `ExportDatabasesToDownloadAsync` |
    | `ImportAllDatabasesAsync` | `ImportDatabasesFromStreamAsync` |
    | `ExportDatabaseAsync` | `ExportDatabaseToStreamAsync` (or `ExportDatabaseToDownloadAsync`) |
    | `ImportDatabaseAsync` | `ImportDatabaseFromStreamAsync` |

    `ExportDatabaseToStreamAsync` writes plain pages, the same bytes the download
    path emits — where `ExportDatabaseAsync` returned what was physically on disk,
    which on an encrypted pool was ciphertext only that pool could read. Tests that
    want the bytes in hand pass a `MemoryStream`. `ImportDatabaseFromStreamAsync`
    signals by exception instead of returning `PoolImportResult`.
  - `SqliteWasmBlazor.Components` no longer exposes
    `FileOperationsInterop.DownloadMessagePackFile`, for the same reason. Use
    `ExportDatabaseToDownloadAsync`.
  - `ImportDatabaseFromStreamAsync` and `ImportDatabasesFromStreamAsync` take a
    `validateImported` delegate **before** `cancellationToken`; callers that passed
    the token positionally must name it.
  - `IHostDatabaseService` moved to `SqliteWasmBlazor` and now declares only
    `OwnedDatabases`, `MigrateAsync` and `ValidateSchemaAsync` — its whole contract
    was already written in base types. `IsAvailable` and `ResetAsync` are on
    `IHostRecoveryService : IHostDatabaseService` in `SqliteWasmBlazor.Crypto.UI`,
    which the panels resolve. Hosts implement the derived interface and register it
    once with `AddHostRecoveryService<THost>()` (or `AddHostDatabaseService<THost>()`
    with no UI); that binds one instance to both.
  - `EncryptionModel.DatabaseNames` is replaced by `Databases` (rows carrying `Owned`
    and `Present`), `ExportDatabase` (one row) joins `ExportDatabases` (bundle), and
    `ProposeDatabaseName` is gone with the free-text import target.
  - `DbContext.ValidateImportedSchemaAsync` throws `SchemaMismatchException` — still an
    `InvalidOperationException`, so existing catch clauses keep working — carrying
    `MissingTables` so a host can phrase the refusal in the user's language.
  - SQL command logging is off unless `SqliteWasmOptions.EnableCommandSqlLogging` is set.
  - `SqliteWasmBlazor.Crypto.UI` needs `RxBlazorV2.MudBlazor` **1.2.6 or newer**: below
    that a command's execution state never reaches the UI, so async buttons show no
    progress and `Disabled` never follows `CanExecute`.

- **v0.9.0-pre** — CSP hardening: removed the inline `data:text/javascript` import that
  auto-detected `<base href>`. The worker URL is now built from `SqliteWasmOptions.BaseHref`
  (default `"/"`).

  Apps deployed on a **sub-path** must now opt in explicitly:
  ```csharp
  builder.Services.AddSqliteWasm(o => o.BaseHref =
      new Uri(builder.HostEnvironment.BaseAddress).AbsolutePath);
  ```
  This derives `BaseHref` from the runtime `<base href>` exactly as the old auto-probe did,
  but without the CSP-blocked `data:` import. Root-path (`/`) deployments need no change.

  **Browser-extension** builds (which flatten the `_content/` underscore prefix) override
  the asset root:
  ```csharp
  builder.Services.AddSqliteWasm(o => o.AssetRoot = "content/SqliteWasmBlazor/");
  ```

  Companion packages gained the same options pattern:
  - `Components`: `FileOperationsInterop.InitializeAsync(o => o.AssetRoot = "...")`
  - `FloatingWindow`: `services.AddFloatingWindow(o => o.AssetRoot = "...")`

  Calling a database operation before `InitializeSqliteWasmAsync` /
  `InitializeSqliteWasmDatabaseAsync<T>` now throws `InvalidOperationException` with a clear
  message, instead of silently lazy-initialising with default settings (which 404'd on
  sub-path / extension builds).

- **v0.7.2-pre** - `SqliteWasmWorkerBridge` is now internal. Use `ISqliteWasmDatabaseService` via DI instead:
  ```csharp
  // Program.cs - add service registration
  builder.Services.AddSqliteWasm();

  // Components - inject the interface
  @inject ISqliteWasmDatabaseService DatabaseService

  // Replace SqliteWasmWorkerBridge.Instance.DeleteDatabaseAsync(...)
  // with:   DatabaseService.DeleteDatabaseAsync(...)
  ```

## What Makes This Special?

Unlike other Blazor WASM database solutions that use in-memory storage or IndexedDB emulation, **SqliteWasmBlazor** is the **first implementation** that combines:

- **True Filesystem Storage** - Uses OPFS (Origin Private File System) with synchronous access handles
- **Full EF Core Support** - Complete ADO.NET provider with migrations, relationships, and LINQ
- **Real SQLite Engine** - Official sqlite-wasm (3.53.0) running in Web Worker
- **Persistent Data** - Survives page refreshes, browser restarts, and even browser updates
- **No Server Required** - Everything runs client-side in the browser

| Solution | Storage | Persistence | EF Core | Limitations |
|----------|---------|-------------|---------|-------------|
| **InMemory** | RAM | None | Full | Lost on refresh |
| **IndexedDB** | IndexedDB | Yes | Limited | No SQL, complex API |
| **SQL.js** | IndexedDB | Yes | None | Manual serialization |
| **besql** | Cache API | Yes | Partial | Emulated filesystem |
| **SqliteWasmBlazor** | **OPFS** | **Yes** | **Full** | **None!** |

## Public API

SqliteWasmBlazor exposes a **stable public API** for database management operations via dependency injection:

### ISqliteWasmDatabaseService

The primary interface for database operations outside of EF Core:

```csharp
public interface ISqliteWasmDatabaseService
{
    // Database management
    Task<IReadOnlyList<string>> ListDatabasesAsync(CancellationToken ct = default);
    Task<bool> ExistsDatabaseAsync(string databaseName, CancellationToken ct = default);
    Task DeleteDatabaseAsync(string databaseName, CancellationToken ct = default);
    Task RenameDatabaseAsync(string oldName, string newName, CancellationToken ct = default);
    Task CloseDatabaseAsync(string databaseName, CancellationToken ct = default);

    // File paths — every one memory-flat. One database or many, in or out.
    Task ExportDatabaseToStreamAsync(string databaseName, Stream destination,
        CancellationToken ct = default);
    Task ExportDatabaseToDownloadAsync(string databaseName, string filename,
        CancellationToken ct = default);
    Task ExportDatabasesToDownloadAsync(IReadOnlyList<string> databaseNames,
        string filename, CancellationToken ct = default);
    Task ImportDatabaseFromStreamAsync(string databaseName, Stream stream, long size,
        Func<string, CancellationToken, ValueTask>? validateImported = null,
        CancellationToken ct = default);
    Task ImportDatabasesFromStreamAsync(Stream envelopeStream, long envelopeSize,
        Func<string, CancellationToken, ValueTask>? validateImported = null,
        CancellationToken ct = default);

    // V2 bulk row import (worker-side prepared statement loop)
    Task<int> ImportRowsAsync(string databaseName, byte[] data,
        CancellationToken ct = default);
}
```

**Usage in components:**

```csharp
@inject ISqliteWasmDatabaseService DatabaseService

@code {
    private async Task ResetDatabaseAsync()
    {
        // Delete and recreate database
        await DatabaseService.DeleteDatabaseAsync("MyApp.db");

        await using var context = await DbContextFactory.CreateDbContextAsync();
        await context.Database.MigrateAsync();
    }

    private async Task DownloadAsync()
    {
        // Straight to a browser download — the worker stages the file in OPFS
        // and the browser saves it from disk, so memory stays flat no matter
        // how large the database is. Closes the DB for a consistent snapshot;
        // the next query re-opens it.
        await DatabaseService.ExportDatabaseToDownloadAsync("MyApp.db", "backup.db");
    }

    private async Task ExportAsync(Stream destination)
    {
        // Same bytes, to a Stream you own. Nothing materialises the file —
        // pass a MemoryStream if you want it in memory, and mean it.
        await DatabaseService.ExportDatabaseToStreamAsync("MyApp.db", destination);
    }

    private async Task ImportAsync(IBrowserFile file)
    {
        // Streamed in one chunk at a time. `validateImported` (omitted here)
        // parks what it replaces and hands you the imported database to
        // inspect before it counts.
        await using var stream = file.OpenReadStream(maxAllowedSize: file.Size);
        await DatabaseService.ImportDatabaseFromStreamAsync(
            "MyApp.db", stream, file.Size);
    }
}
```

### Other Public Types

| Type | Purpose |
|------|---------|
| `SqliteWasmConnection` | ADO.NET `DbConnection` for direct SQL access |
| `SqliteWasmCommand` | ADO.NET `DbCommand` for query execution |
| `SqliteWasmDataReader` | ADO.NET `DbDataReader` for result iteration |
| `SqliteWasmParameter` | ADO.NET `DbParameter` for query parameters |
| `SqliteWasmTransaction` | ADO.NET `DbTransaction` for transaction support |
| `IDbInitializationStatus` | Tracks database initialization state and errors |
| `PoolImportResult` | Outcome of a raw `.db` import (`OK`, `WRONG_KEY`, `EXISTING_DB_REFUSED`) |
| `SchemaMismatchException` | Thrown by `ValidateImportedSchemaAsync`; carries `MissingTables` |

All internal implementation details (worker bridge, serialization, etc.) are encapsulated and not part of the public API.

## Installation

### NuGet Package

```bash
dotnet add package SqliteWasmBlazor --prerelease
```

Optional at-rest encryption and its drop-in UI are separate packages — add them only
if you need them:

```bash
dotnet add package SqliteWasmBlazor.Crypto --prerelease     # encrypted VFS
dotnet add package SqliteWasmBlazor.Crypto.UI --prerelease  # auth / encryption panels
```

Or install a specific version:

```bash
dotnet add package SqliteWasmBlazor --version 0.9.3-pre
```

Visit [NuGet.org](https://www.nuget.org/packages/SqliteWasmBlazor) for the latest version.

### From Source

```bash
git clone https://github.com/b-straub/SqliteWasmBlazor.git
cd SqliteWasmBlazor
dotnet build
```

## Quick Start

### 1. Configure Your Project

**Program.cs:**

```csharp
using SqliteWasmBlazor;

var builder = WebAssemblyHostBuilder.CreateDefault(args);

// Add your DbContext with SqliteWasm provider
builder.Services.AddDbContextFactory<TodoDbContext>(options =>
{
    var connection = new SqliteWasmConnection("Data Source=TodoDb.db");
    options.UseSqliteWasm(connection);
});


// Register SqliteWasm database management service (for ISqliteWasmDatabaseService)
builder.Services.AddSqliteWasm();

var host = builder.Build();

// Initialize SqliteWasm database with automatic migration support
await host.Services.InitializeSqliteWasmDatabaseAsync<TodoDbContext>();

await host.RunAsync();
```

The `InitializeSqliteWasmDatabaseAsync` extension method automatically:
- Initializes the Web Worker bridge
- Applies pending migrations (with automatic migration history recovery)
- Handles multi-tab conflicts with helpful error messages
- Tracks initialization status via `IDbInitializationStatus`

### 2. Define Your DbContext

```csharp
using Microsoft.EntityFrameworkCore;

public class TodoDbContext : DbContext
{
    public TodoDbContext(DbContextOptions<TodoDbContext> options) : base(options) { }

    public DbSet<TodoItem> TodoItems { get; set; }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<TodoItem>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Title).IsRequired().HasMaxLength(200);
        });
    }
}

public class TodoItem
{
    public int Id { get; set; }
    public string Title { get; set; }
    public bool IsCompleted { get; set; }
    public DateTime CreatedAt { get; set; }
}
```

### 3. Use in Your Components

```razor
@inject IDbContextFactory<TodoDbContext> DbFactory

<h3>Todo List</h3>

@foreach (var todo in todos)
{
    <div>
        <input type="checkbox" @bind="todo.IsCompleted" @bind:after="() => SaveTodo(todo)" />
        <span>@todo.Title</span>
    </div>
}

@code {
    private List<TodoItem> todos = new();

    protected override async Task OnInitializedAsync()
    {
        await using var db = await DbFactory.CreateDbContextAsync();
        todos = await db.TodoItems.OrderBy(t => t.CreatedAt).ToListAsync();
    }

    private async Task SaveTodo(TodoItem todo)
    {
        await using var db = await DbFactory.CreateDbContextAsync();
        db.TodoItems.Update(todo);
        await db.SaveChangesAsync(); // Automatically persists to OPFS!
    }
}
```

## Features

### Full EF Core Support

```csharp
// Migrations
await dbContext.Database.MigrateAsync();

// Complex queries with LINQ
var results = await dbContext.Orders
    .Include(o => o.Customer)
    .Where(o => o.Total > 100)
    .OrderByDescending(o => o.Date)
    .ToListAsync();

// Relationships
public class Order
{
    public int Id { get; set; }
    public Customer Customer { get; set; }
    public List<OrderItem> Items { get; set; }
}

// Decimal arithmetic (via ef_ scalar functions)
var expensive = await dbContext.Products
    .Where(p => p.Price * 1.2m > 100m)
    .ToListAsync();
```

### High Performance

- **Efficient Serialization** - JSON for requests (small), MessagePack for responses (optimized for data)
- **Typed Column Information** - Worker sends type metadata to reduce .NET marshalling overhead
- **OPFS SAHPool** - Near-native filesystem performance with synchronous access
- **Direct Execution** - Queries run directly on persistent storage, no copying needed

### Enterprise-Ready

- **Type Safety** - Full .NET type system with proper decimal support
- **EF Core Functions** - All `ef_*` scalar and aggregate functions implemented
- **JSON Collections** - Store `List<T>` with proper value comparers
- **Logging** - Configurable logging levels (Debug/Info/Warning/Error)
- **Error Handling** - Proper async error propagation

## Documentation

| Topic | Description |
|-------|-------------|
| [Architecture](docs/architecture.md) | Worker-based architecture, how it works, technical details |
| [ADO.NET Usage](docs/ado-net.md) | Using SqliteWasmBlazor without EF Core, transactions |
| [Advanced Features](docs/advanced-features.md) | Migrations, FTS5 search, JSON collections, logging |
| [Multi-Database](docs/multi-database.md) | Running multiple databases, cross-database references |
| [Bulk Import/Export](docs/bulk-import-export.md) | V2 format, multi-part export, delta sync, type conversions |
| [Encrypted VFS](docs/crypto-vfs.md) | At-rest encryption: ChaCha20-Poly1305, PRF-derived keys, threat model |
| [Security](docs/security/README.md) | Threat model + assurance summary + Tamarin proofs |
| [Formal Models](docs/formal/README.md) | The Tamarin theories themselves, plus `verify.sh` / `mutation-check.sh` |
| [Recommended Patterns](docs/patterns.md) | Multi-view pattern, data initialization best practices |
| [FAQ](docs/faq.md) | Common questions and browser support |
| [Changelog](CHANGELOG.md) | Release notes and version history |

## Browser Support

| Browser | Version | OPFS Support |
|---------|---------|--------------|
| Chrome  | 108+    | Full SAH support |
| Edge    | 108+    | Full SAH support |
| Firefox | 111+    | Full SAH support |
| Safari  | 16.4+   | Full SAH support |

All modern browsers (2023+) support OPFS with Synchronous Access Handles, including mobile browsers (iOS/iPadOS Safari, Android Chrome).

## Roadmap

- [x] Core ADO.NET provider
- [x] OPFS SAHPool integration
- [x] EF Core migrations support
- [x] MessagePack serialization
- [x] Custom EF functions (decimals)
- [x] FTS5 full-text search with highlighting and snippets
- [x] MudBlazor demo app
- [x] NuGet package pre-release
- [x] Database export/import API (MessagePack serialization + raw .db files)
- [x] Backup/restore utilities (delta sync with checkpoints)
- [x] Raw database import/export with schema validation
- [x] Multi-database support
- [x] V2 worker-side bulk import/export with prepared statement loops
- [x] Multi-part export for large databases
- [x] Seed Server API for cloud-based database provisioning
- [x] At-rest encryption with passkey-derived keys (`SqliteWasmBlazor.Crypto`)
- [x] Drop-in authentication / encryption UI (`SqliteWasmBlazor.Crypto.UI`)
- [x] Memory-flat exports and streamed imports (OPFS staging, mobile-safe)
- [x] Machine-checked Tamarin models for the encryption lifecycle
- [ ] Stable NuGet package release
- [ ] CryptoSync — end-to-end encrypted multi-device delta sync
- [ ] Server-side delta generation from SQLite databases

## Contributing

Issues, bug reports, and feature discussions are always welcome.

For code contributions we use an **issue-first policy**: please open an issue and agree on the approach before submitting a pull request. Trivial fixes (typos, obvious one-line bugs) can be sent directly. See [CONTRIBUTING.md](CONTRIBUTING.md) for details and rationale.

## Credits

**Author**: bernisoft
**License**: MIT

Built with:
- [SQLite](https://sqlite.org) - The world's most deployed database
- [sqlite-wasm](https://sqlite.org/wasm) - Official SQLite WebAssembly build
- [Entity Framework Core](https://github.com/dotnet/efcore) - Modern data access
- [MessagePack](https://msgpack.org/) - Efficient binary serialization
- [MudBlazor](https://mudblazor.com/) - Material Design components

## License

MIT License - Copyright (c) 2025 bernisoft

See [LICENSE](LICENSE) file for details.

---

**Built with love for the Blazor community**

If you find this useful, please star the repository!
