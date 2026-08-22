# Plane re-homing: memory-safe I/O to base, encryption to Crypto

The streamed import/export paths were developed on plane 2 because that is where
the forked SAHPool VFS lived, not because they need encryption. The result is a
public surface where a plain consumer cannot import a large database without
taking a crypto package, `ExportDatabaseToDownloadAsync` is declared twice with
two different implementations, and a host seam whose whole contract is expressed
in plane-1 types sits behind a MudBlazor dependency.

This is a re-homing pass, not a merge. Three packages stay three packages: the
MudBlazor + RxBlazorV2 dependency in `Crypto.UI` is a real boundary, and forcing
the 3.7 MB crypto worker bundle on plain consumers to save an API annoyance is a
bad trade.

## Target surface

**`ISqliteWasmDatabaseService`** — symmetric `{one, many} x {in, out}`, every
path memory-flat:

```csharp
Task ExportDatabaseToDownloadAsync(string name, string filename, CT);
Task ExportDatabasesToDownloadAsync(IReadOnlyList<string> names, string filename, CT);
Task ExportDatabaseToStreamAsync(string name, Stream destination, CT);
Task ImportDatabaseFromStreamAsync(string name, Stream src, long size, Func<...>? validate, CT);
Task ImportDatabasesFromStreamAsync(Stream src, long size, Func<...>? validate, CT);
```

plus the non-file ops (`List` / `Exists` / `Delete` / `Rename` / `Close` /
`ImportRowsAsync`).

**`IEncryptedSqliteWasmDatabaseService`** — encryption only:

```csharp
GetStateAsync / UnlockAsync / LockAsync / EnterEncryptedAsync / LeaveEncryptedAsync / ResetPoolAsync
ExportPoolToPubkeyAndDownloadAsync / ImportPoolGuidedFromStreamAsync   // .eds
```

Ten file-movement methods across two planes (one of them duplicated) become five
on base and two on Crypto.

## Goals

Each goal is independently approvable. Dependencies: `1 -> 2 -> 3 -> {4, 5} -> 6 -> 7`;
`0` stands alone, `5` can run beside `4`.

### Goal 0 — Land the current doc changes as they are

`CHANGELOG.md`, `README.md` and `docs/faq.md` are modified in the working tree
and describe 0.9.3-pre as it ships today. They are correct now and every goal
below invalidates part of them. Commit as-is; Goal 7 revises once, rather than
carrying a half-rewritten set through the whole pass.

### Goal 1 — Write-side pool primitives in the base VFS patch

`writeFileSlice` and `atomicReplaceFile` exist only in `vfs-prf/sahpool-prf-vfs.ts`
(the plane-2 fork). Base runs the vendor SAHPool, and nothing can move until that
pool can do a slot write and an atomic swap.

Extend `src/Base/SqliteWasmBlazor/TypeScript/patches/@sqlite.org+sqlite-wasm+3.53.0-build1.patch`
with the two write-side methods, mirroring exactly what the patch already does
for the read side: the pool class *and* the `OpfsSAHPoolUtil` facade both get the
member (the existing patch adds `getFileSize` / `exportFileSlice` in both places).

**DONE.** `writeFileSlice`, `atomicReplaceFile` and `recoverAccessHandles` are in
the patch, on the pool class and the `OpfsSAHPoolUtil` facade both, matching how
the read-side pair was already added. `recoverAccessHandles` came along because
`withHandleRecovery` wraps exactly the three ops Goal 3 relocates (rename, unlink,
`replaceDb`) — without it base park/restore would have no defence when the
platform closes handles.

Two things the port turned up:

- The fork calls the open-file map `mapS3FileToOFile`; in the vendor that name is
  a **method** and the field is `#mapS3FileToOFile_`. Copying the fork verbatim
  fails the esbuild private-name check. The vendor's own `pauseVfs` guard uses
  the underscored field — match that.
- **`patches/` was not an input to `BuildTypeScriptBundles`.** `_TsSrc` globbed
  `**/*.ts` and `package.json` only, so editing the vendor patch left the stamp
  current and the bundle kept the previous vendor code with nothing to say so —
  the exact Inputs/Outputs trap this repo has hit before. Fixed in
  `SqliteWasmBlazor.csproj`; verified by touching the patch and watching
  `npm run build` re-run.

Verified: patch re-applies clean, typecheck, eslint, 12/12 base TS tests, and all
six primitives present in the built worker bundle.

- **Standing risk:** the patch is against a pinned `3.53.0-build1`. A sqlite-wasm
  bump re-rolls it — the same exposure the read-side pair already carried, now
  twice the surface.

### Goal 2 — Move the plane-neutral worker machinery into `worker-common`

`@sqlitewasmblazor/worker-common` already ships the staging half
(`openExportStaging` / `openImportStaging` / `readStagingFile` /
`sweepExportStaging`) and plane 2 already imports it from base. Move the rest of
the plane-neutral machinery down beside it:

| Moves | Why it is plane-neutral |
| --- | --- |
| `bridge/msgpack-stream.ts` (324 lines) | `.dbs` codec; contains no crypto. Base already imports `msgpackr`. |
| `vfs-prf/import-streamed.ts` (531 lines) | Already takes the rekey transform as an optional callback; with no key it runs plain. |
| park / multi-import temp naming from `vfs-prf/pool-naming.ts` | Pool bookkeeping. The encrypt/decrypt temp suffixes stay in `vfs-prf`. |
| `importSessionOpen` / `Append` / `Close` / `Discard`, the session map, `stagedSessionFile` | Chunk pump over an OPFS staging file. |

The one piece of new design: `importSessionOpen` currently reaches for
`hasGlobalKey()` / `snapshotGlobalKey()` directly. In `worker-common` it takes an
injected sink hook; the plane-2 worker supplies the key lookup and the
`rekeySlots` transform, the plane-1 worker supplies nothing. `import-streamed.ts`
already declares the structural pool type it needs (`atomicReplaceFile(src, dst): true`)
— reuse it as the contract both VFS variants satisfy.

**DONE.** Eight modules now live in `worker-common`: `msgpack-stream`,
`pool-naming`, `import-sink`, `import-session`, `envelope-import`,
`envelope-export`, `handle-recovery`, `memory`. Encryption enters each as an
injected transform — the session takes an `openDatabaseSink`, the envelope
paths take a `crypto` object with `snapshotKey` + `rekey`/`toPlain`. The crypto
worker's `.dbs` import and export handlers, 144 and 90 lines of near-identical
batching, are now six-line delegations.

Deviations from the sketch above, both deliberate:

- **`pool-naming.ts` moved whole**, encrypt/decrypt temp suffixes included.
  Splitting it would have broken `planPoolSweep`'s decision table, which is the
  unit-tested part; two of the suffixes being crypto-flavoured strings is not
  worth fracturing that.
- **`import-streamed.ts` was two modules in one file.** Only the sink half moved;
  the `.eds` envelope passes stay in `vfs-prf`, which is where they belong. The
  first cut put `consumeEnvelopeMetadata` on the wrong side — it parses the
  envelope, so it went back.

`withHandleRecovery` came along and finally has a test (one retry, exactly one,
other errors untouched); its recover step is a parameter so the branch is
reachable at all. Base TS 6 files / 38 tests, crypto 4 / 43, both typecheck and
lint clean.

### Goal 3 — Move the C# streamed surface to base

**Bridge.** Five of `EncryptedSqliteWasmWorkerBridge`'s eleven members are pool
operations, not crypto operations, and move to the base bridge:
`ImportSessionOpenAsync`, `ImportSessionAppendAsync`, `ImportSessionCloseAsync`,
`ImportSessionDiscardAsync`, `ReplaceDatabaseAsync`. The six that stay are the
real plane-2 surface (`SetEncryptionKey`, `ClearEncryptionKey`,
`EncryptDatabaseInPlace`, `DecryptDatabaseInPlace`, `WritePoolManifest`,
`ClearPoolManifest`).

**Service.** `PumpIntoImportSessionAsync`, `StreamIntoPoolAsync`,
`SweepImportParksAsync`, `UndoPoolImportAsync`, `PoolNaming`,
`PoolOperationRejectedException` + `PoolOperationRejection` move to base.
`PoolLockedException` is already there.

**Interface.** `ISqliteWasmDatabaseService` gains `ExportDatabasesToDownloadAsync`,
`ImportDatabaseFromStreamAsync`, `ImportDatabasesFromStreamAsync`. The duplicate
`ExportDatabaseToDownloadAsync` is deleted from `IEncryptedSqliteWasmDatabaseService`
— one implementation survives, and the worker stays state-aware behind it.

**State guard.** Base must not call `GetStateAsync` (a plane-2 type). Use the
existing seam: `IDatabaseLockProbe` + `ThrowIfPoolLocked`, which base's
`ExportDatabaseToDownloadAsync` already uses, and which Crypto already registers.

- **Carry `withHandleRecovery` across.** It wraps `rename` / `unlink` /
  `replaceDb` — all three move here. Keep it (the condition it catches,
  `InvalidStateError` from a platform-closed SAH, has causes we do not control:
  OS memory pressure from other apps, tab suspension, storage eviction) and add
  the unit test it never had: make `op` throw a `DOMException('…',
  'InvalidStateError')` once, assert exactly one retry, and assert any other
  error propagates untouched. Its comment should describe the platform
  condition, not the import bug that first exposed it.

**DONE.** The bridge members, the service methods, `PoolNaming` and
`PoolOperationRejectedException` are on base; `ISqliteWasmDatabaseService`
carries the five memory-flat file paths and `IEncryptedSqliteWasmDatabaseService`
is encryption-only. Solution builds clean, 98/98 Playwright, base TS 6 files /
38 tests, crypto 4 / 43. Net -582 lines while base gained the whole surface.

What the pass turned up, beyond the survey:

- **The stream protocol became one shared router**, `worker-common/stream-bridge.ts`,
  rather than ~25 duplicated lines per bridge. It owns the negative id counter,
  the handler registry and the dispatch; a caller supplies `build(streamId)` and
  `settle(done)`. The two plane-neutral ops that ride it —
  `exportDatabasesToDownload`, `importDatabasesFromSession` — are in the same
  module and both bridges expose one-line adapters. The crypto bridge lost 172
  lines to this.
- **Which `ExportDatabaseToDownloadAsync` survived: base's.** It rides the
  request/response protocol (`exportDbToStaging` + `downloadStagedExport`), and
  the crypto worker already implemented that op state-aware, so base's works
  against either bundle. What went away is plane 2's stream-protocol twin —
  the JS entry point, the worker's `exportDatabaseToStaging` stream case, and
  the `ExportDatabaseToDownloadJsAsync` JSImport.
- **The lock guard changed exception type, on purpose.** The plan said use
  `ThrowIfPoolLocked`, which throws `PoolLockedException` — but that type means
  "consumer code reached the DB outside the AuthorizeView gate", and the copy
  says so. A user clicking Export on a locked pool is not that. So the guard
  gained an overload taking a `PoolOperationRejection`, and all five file paths
  (base's existing single-DB export included) raise
  `EXPORT_NEEDS_UNLOCK` / `PLAIN_IMPORT_NEEDS_UNLOCK` — which is what
  `EncryptionModel` already localizes. `IDatabaseLockProbe` was not needed: the
  `_poolLocked` flag it feeds is refreshed on every state probe and transition,
  which is the same freshness the SQL path already relies on.
- **`ReportDbState` needed a home.** The whole-pool import reports READY so
  every `<AuthorizeView>` re-evaluates, and the reporter is a base type
  (`IDbInitializationReporter`) — but the bridge is a `Lazy` singleton outside
  the container. Resolving it from the DI registration risks a cycle
  (`DbStateModel` → `IPrfAuthenticationStateProvider` → …), so the two init
  helpers, which already resolve both facets at app start, call
  `AttachBootStatus`. Same mutator shape as `SetPoolLocked`.
- **`withHandleRecovery` needs no new test** — Goal 2 gave it one when it moved.
  Base now wraps the same three ops it wraps on plane 2 (rename, unlink,
  replace), and base's init runs `planPoolSweep` so a park can be restored
  there too.

- **Coverage gap, deliberate and open.** TestApp calls
  `AddSqliteWasmBlazorCrypto`, so its `AssetRoot` points at the Crypto bundle
  and all 98 Playwright tests drive the plane-2 worker. Base's six new worker
  cases and its init sweep are typechecked and bundled but executed by nothing
  in the repo. Closing it means a plain-plane Playwright fixture (a sample app
  on `AddSqliteWasm` alone, plus host wiring) — a scope call, not part of this
  goal.

### Goal 4 — Retire the `byte[]` file methods

`ExportDatabaseAsync` -> `byte[]` and `ImportDatabaseAsync(name, byte[])` are the
last non-memory-safe file paths. Nothing in `src/` or the demo calls them; the
callers are 21 TestApp files that want bytes in hand for assertions.

- Add `ExportDatabaseToStreamAsync(string name, Stream destination, CT)` — the
  worker chunks into a caller-supplied stream, so materializing is an explicit
  caller choice. Tests pass a `MemoryStream`.
- Delete both `byte[]` methods; drop the worker's `importDb` / `exportDb` cases
  once nothing calls them.
- `ImportRowsAsync` stays — it is a bulk-row path, not a file path.

**Open decision.** `ImportDatabaseAsync` returns `PoolImportResult`
(`OK` / `WRONG_KEY` / `EXISTING_DB_REFUSED`); the streamed single-DB import
returns `Task` and signals by exception. Either give the streamed import the same
return, or accept exception-only signalling and document it. Exception-only is
the smaller surface and matches the multi-DB path — recommend that unless a
consumer branches on the value.

### Goal 5 — Split the host seam

`IHostDatabaseService` bundles two contracts. `EncryptionModel` uses
`OwnedDatabases` / `MigrateAsync` / `ValidateSchemaAsync`; `DatabaseErrorAlertModel`
uses `IsAvailable` (purely to hide a reset button) and `ResetAsync`. The first
three describe databases and their contract is written in base types —
`DbContext.ValidateImportedSchemaAsync` and `SchemaMismatchException` both live in
base already.

```csharp
// base
public interface IHostDatabaseService
{
    IReadOnlyList<string> OwnedDatabases { get; }
    ValueTask MigrateAsync(CancellationToken ct = default);
    ValueTask ValidateSchemaAsync(string ownedName, string probeName, CancellationToken ct = default);
}

// Crypto.UI
public interface IHostRecoveryService : IHostDatabaseService
{
    bool IsAvailable { get; }
    ValueTask ResetAsync(CancellationToken ct = default);
}
```

Hosts still write one class and one registration; the UI resolves the derived
interface. `NullHostDatabaseService` splits the same way. Keep the
`validateImported` delegate as the primitive on the import call — that part of
the current design is already plane-clean; the interface is the host-facing
convenience, not the mechanism.

Also fix the folder/namespace mismatch: the file sits in `Abstractions/` and
declares `namespace SqliteWasmBlazor.Crypto.UI.Services`.

### Goal 6 — Pull migrate-after-import into the import path

The release notes state an invariant: every successful import re-runs the host's
migrations and re-creates owned databases the file omitted. It is implemented in
`EncryptionModel` (four `MigrateAsync` call sites), so it holds only for hosts
that use the drop-in UI. A headless consumer gets a pool with a possibly-older
schema and no re-created owned databases, silently.

Move it into the base import methods, after the validated import commits, and
delete the duplicated calls from `EncryptionModel`.

**Open decision.** How the import path reaches the host: resolve
`IHostDatabaseService` from DI inside the service (absent -> skip, documented), or
take it as an optional parameter. DI-resolved is the smaller call site and keeps
the invariant automatic — recommend that.

### Goal A — Disclose that a plain export leaves an encrypted pool unencrypted

*Independent of goals 1-7; blocked by nothing.*

On an Encrypted+Unlocked pool, `ExportDatabaseToDownloadAsync` and
`ExportDatabasesToDownloadAsync` decrypt slot-by-slot and write a vanilla SQLite
file. That is the documented, intended behaviour — a `.db` that only re-imports
into a pool holding the same key would be useless — and the Locked refusal
(`EXPORT_NEEDS_UNLOCK`) is correct. The engine is right.

The UI never says so. `Status_SingleDbExported` is `"Exported {0} as {1}."`;
nothing in any `Crypto.UI` resx or razor mentions that export output is
plaintext. The only "Unencrypted" strings describe pool *state* and the reset
path. A user who has just completed a passkey ceremony reasonably assumes the
export inherits that protection.

Sharper still, four export paths give four different outcomes on an encrypted
pool, undisclosed: `ExportDatabaseAsync` -> ciphertext, the two download paths ->
plaintext, `.eds` -> encrypted envelope. Goal 4 removes the first.

Do **not** hide the buttons — portable data out is a legitimate capability
(migration, backup into the user's own encrypted storage). Disclose instead.

**DONE.** Confirm-before-export rather than a post-hoc notice: a dialog is raised
on an Encrypted+Unlocked pool before either plain export runs, and the message
points at the encrypted backup as the alternative. `Confirm_ExportPlainSingle`,
`Confirm_ExportPlainBundle` and `Btn_SaveUnencrypted` live in `EncryptionModel`'s
resources (en + de) so any host gets the copy; the demo wires them through
`ConfirmExecutionAsync`, matching the reset and import confirmations. A plain
pool raises nothing.

Goal 3 touches this only by renaming the call site from `Session.` to
`DatabaseService.`.

### Goal 7 — Docs, round two

- README: rewrite the 0.9.3-pre breaking-changes block. The "add
  `SqliteWasmBlazor.Crypto` as the migration path" paragraph goes away entirely —
  after Goal 3 the replacements are on the package the consumer already has. The
  base-plane-vs-Crypto-plane capability gap note is likewise unnecessary.
- CHANGELOG: rewrite the 0.9.3-pre entries in place. Pre-release rules — no
  append-only "additionally, since 0.9.3-pre1" layering.
- `docs/crypto-vfs.md`, `docs/bulk-import-export.md`, `docs/faq.md`: re-point the
  import/export references at the base plane.
- ROADMAP + memory update as part of the final commit.

## Cross-cutting

- **Breaking again, on top of 0.9.3-pre.** Acceptable on a pre-release branch and
  the pre-release rule says rewrite in place rather than layering compatibility
  shims. It does mean the 0.9.3-pre notes get rewritten, not extended.
- **Base bundle grows** by the session + envelope + codec code. `msgpackr` and
  the staging helpers are already in base, so this is code size, not a new
  dependency.
- **Playwright at close.** 90 test files; run the suite at workstream close and
  after Goal 3 specifically — that is the goal that moves the JS bridge surface.
- **iPad re-verify.** The large-import fix (`e489c88` + `f4da6cd`) is verified on
  device as of 2026-08-22 — very large databases import without failure. Goal 3
  relocates that exact code path from plane 2 to base, so the device check has to
  be re-run after it: the verification attaches to the code, not to the feature.
  Anything that regresses there regresses a *known-good* baseline, which makes it
  a clean bisect rather than an open question.
