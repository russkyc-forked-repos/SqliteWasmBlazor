# Disk → Pool rename

Status: **complete**. resx keys, groups A–D and both cross-language contracts
are renamed; no `Disk`-named identifier remains in source. Kept as the record of
what moved and what deliberately did not.

Not yet verified in a browser. Group D changed two cross-language contracts, so
a TestApp Playwright run is the outstanding check — see *Verification* below.

## Why

The storage unit this codebase calls a "disk" is an OPFS SAHPool: a fixed set of
pre-allocated, opaquely-named OPFS files ("slots"), each carrying a 4096-byte
plaintext header that holds the logical path SQLite believes it is using. There
is no directory tree and no file-system semantics — the names users see exist
only as strings inside slot headers.

`Pool` is the accurate term, and it is what the vendor VFS calls itself. It is
also the only free one: `Databases` is already taken by `ExportDatabases` /
`ImportDatabases` in `EncryptionModel`, which are the *per-selection* commands —
a genuinely different operation from the whole-pool ones being renamed here.

The unit-of-encryption framing is stated in the code itself, in
`vfs-prf/manifest.ts`: *"Disk-as-unit: every DB in the pool carries the same
manifest."* That invariant is what needed a noun; "disk" was the wrong one.

## Done — resx keys

Renamed in both `.resx` and `.de.resx`, with every `Localizer[...]` lookup
updated in the same pass. 38 replacements across 15 files.

| Old key | New key | File |
| --- | --- | --- |
| `Btn_ImportDisk` | `Btn_ImportPool` | `EncryptionModel` |
| `Btn_ExportDiskBackup` | `Btn_ExportPoolBackup` | `EncryptionModel` |
| `Btn_ExportDiskForRecipient` | `Btn_ExportPoolForRecipient` | `EncryptionModel` |
| `Confirm_ImportDisk` | `Confirm_ImportPool` | `EncryptionModel` |
| `Status_DiskExported` | `Status_PoolExported` | `EncryptionModel` |
| `Status_DiskExportedForRecipient` | `Status_PoolExportedForRecipient` | `EncryptionModel` |
| `Status_DiskImported` | `Status_PoolImported` | `EncryptionModel` |
| `Caption_EncryptedDisk` | `Caption_EncryptedPool` | `AuthenticationModel` |
| `Status_WrongPasskeyForDisk` | `Status_WrongPasskeyForPool` | `AuthenticationModel` |
| `Alert_WrongPasskeyForDisk` | `Alert_WrongPasskeyForPool` | `AuthenticationModel` |
| `Error_DiskLocked` | `Error_PoolLocked` | `TodoListModel`, `MultiDatabaseModel` |

The keys now anticipate the C# names below, so until group C lands the key
`Btn_ExportPoolBackup` still drives a command called `ExportDiskBackup`.

## Done — group A: Crypto plane public API

Applied via `mcp__rider__rename_refactoring`, one symbol at a time, renaming the
four public members from their **interface** declaration so implementations
followed. Solution builds clean.

The engine updated declarations, call sites and `<see cref>` links. It did not
touch `<c>Name</c>` doc tags, `//` comments, or names embedded in exception
message literals — 24 of those were swept by hand afterwards, across 10 files.
`DiskLockedException.cs` also carried a `<see cref="EncryptedDiskState.Encrypted"/>`
that the engine could not resolve (base plane cannot see the Crypto plane), so it
would have silently rotted; the sweep caught it.

| Symbol | New name | Declared at | refs / files |
| --- | --- | --- | --- |
| `EncryptedDiskState` | `EncryptedPoolState` | `Crypto/Abstractions/IEncryptedSqliteWasmDatabaseService.cs:28` | 23 / 6 |
| `ResetDiskAsync` | `ResetPoolAsync` | `Crypto/Services/EncryptedSqliteWasmDatabaseService.cs:484` + interface | 22 / 14 |
| `ImportDiskGuidedFromStreamAsync` | `ImportPoolGuidedFromStreamAsync` | `IEncryptedSqliteWasmDatabaseService.cs:237` | 14 / 4 |
| `ExportDiskToPubkeyAndDownloadAsync` | `ExportPoolToPubkeyAndDownloadAsync` | `EncryptedSqliteWasmDatabaseService.cs:550` | 6 |
| `ExportDiskToPubkeyBytesAsync` | `ExportPoolToPubkeyBytesAsync` | `EncryptedSqliteWasmDatabaseService.cs:590` | 5 |
| `ReadDiskManifestAsync` | `ReadPoolManifestAsync` | `EncryptedSqliteWasmWorkerBridge.cs` | 2 |
| `WriteDiskManifestAsync` | `WritePoolManifestAsync` | `EncryptedSqliteWasmWorkerBridge.cs:199` | 2 |
| `ClearDiskManifestAsync` | `ClearPoolManifestAsync` | `EncryptedSqliteWasmWorkerBridge.cs:224` | 3 |

Reported `touched` runs lower than a grep count because the engine reports only
what it rewrote, and its `files` list under-reports: the `ResetDiskAsync` result
omitted `EncryptedSqliteWasmDatabaseService.cs` even though it correctly renamed
the implementing method there. Verify with grep, not with the tool's own count.

## Done — group B: base plane

| Symbol | New name | Declared at | refs / files |
| --- | --- | --- | --- |
| `PoolImportResult` | `PoolImportResult` | `Base/Abstractions/ISqliteWasmDatabaseService.cs:19` | 27 / 9 |
| `DiskLockedException` | `PoolLockedException` | `Base/Exceptions/DiskLockedException.cs:33` | 12 / 6 |
| `SetDiskLocked` | `SetPoolLocked` | `SqliteWasmWorkerBridge.cs:122` | 13 |
| `IsDiskLocked` | `IsPoolLocked` | `SqliteWasmWorkerBridge.cs:115` | 1 |
| `ThrowIfDiskLocked` | `ThrowIfPoolLocked` | `SqliteWasmWorkerBridge.cs:128` | 5 |
| `ExportDiskToDownloadAsync` | `ExportPoolToDownloadAsync` | `SqliteWasmWorkerBridge.cs:1006` ⚠️ | 3 |
| `ExportDiskToBytesSessionAsync` | `ExportPoolToBytesSessionAsync` | `SqliteWasmWorkerBridge.cs:1029` ⚠️ | 3 |
| `ImportDiskStreamPreflightFromSessionAsync` | `ImportPoolStreamPreflightFromSessionAsync` | `SqliteWasmWorkerBridge.cs:1059` ⚠️ | 2 |
| `ImportDiskStreamCommitFromSessionAsync` | `ImportPoolStreamCommitFromSessionAsync` | `SqliteWasmWorkerBridge.cs:1073` ⚠️ | 2 |

File rename: `DiskLockedException.cs` → `PoolLockedException.cs`.

⚠️ **The four marked rows are the only place this rename can break silently.**
Each carries a `[JSImport("exportPoolToDownload")]`-style attribute whose string
argument no rename engine touches. Either leave the JS names as they are and
accept that C# and JS diverge in naming, or change both sides in the same
commit. A half-rename compiles and fails at runtime.

## Done — group C: Crypto.UI

| Symbol | New name | Declared at | refs / files |
| --- | --- | --- | --- |
| `EncryptedDiskLifecycle` | `EncryptedPoolLifecycle` | `Crypto.UI/Services/EncryptedDiskLifecycle.cs:50` | 18 / 7 |
| `UseEncryptedDiskLifecycle` | `UseEncryptedPoolLifecycle` | `Extensions/CryptoUiServiceCollectionExtensions.cs:124` ⚠️ | 3 / 3 |
| `ImportDisk` + `ImportDiskCmdAsync` + `CanImportDisk` | `ImportPool` + `ImportPoolCmdAsync` + `CanImportPool` | `EncryptionModel.cs:114` | 13 + 6 + 4 |
| `ExportDiskBackup` + `ExportDiskBackupAsync` + `CanExportDisk` | `ExportPoolBackup` + `ExportPoolBackupAsync` + `CanExportPool` | `EncryptionModel.cs:88` | 10 + 6 + 4 |
| `ExportDiskForRecipient` + `ExportDiskForRecipientAsync` + `CanExportDiskForRecipient` | `ExportPoolForRecipient` + … | `EncryptionModel.cs:91` | 10 + 6 + 4 |
| `RefreshDiskStateAsync` | `RefreshPoolStateAsync` | `AuthenticationModel.Lifecycle.cs:80` | 6 / 2 |
| `FormatDiskIoFailure` | `FormatPoolIoFailure` | `samples/TestApp/Pages/PrfVfsTest.razor:670` | 3 |

File rename: `EncryptedDiskLifecycle.cs` → `EncryptedPoolLifecycle.cs`.

⚠️ `UseEncryptedDiskLifecycle` is consumer-facing — it appears in sample
`Program.cs` files and in any downstream host. Pre-release, so rename in place
without a compatibility shim, but it belongs in the changelog.

Skip `_importDisk`, `_exportDiskBackup`, `_exportDiskForRecipient` — RxBlazorV2
generates those backing fields from the property names and they follow the
property rename on their own.

## Done — group D: TypeScript + the cross-language contracts

Not reachable by any rename engine — applied as a scripted whole-word sweep over
tracked `.cs`, `.ts` and `.md`, 141 replacements across 15 files.

Two contracts had to move on both sides in this one commit:

1. **`[JSImport]` ↔ TS export map.** The four attribute strings in
   `SqliteWasmWorkerBridge.cs` and the matching keys in `worker-bridge.ts`:
   `exportPoolToDownload`, `exportPoolToBytesSession`,
   `importPoolStreamPreflightFromSession`, `importPoolStreamCommitFromSession`.
2. **C# message `type` ↔ TS worker `case`.** `EncryptedSqliteWasmWorkerBridge.cs`
   composes `new { type = "readPoolManifest" }` and friends; `sqlite-worker.ts`
   switches on them. `readPoolManifest`, `writePoolManifest`, `clearPoolManifest`.

Both were diffed side-by-side after the sweep and match exactly. A mismatch here
compiles and then fails in the browser, which is why they are listed rather than
assumed.

`bundles/*.js` are generated (untracked) but ship at runtime, so `npm run build`
was re-run and the bundles checked for old names — zero remaining.

`exportPoolToDownload`, `exportPoolToBytesSession`, `exportPoolToStaging`,
`exportPoolToStagingHandler`, `importPoolStreamPreflight`,
`importPoolStreamPreflightFromSession`, `importPoolStreamPreflightHandler`,
`importPoolStreamCommit`, `importPoolStreamCommitFromSession`,
`importPoolStreamCommitHandler`, `importPoolStreamed`, `readPoolManifest`,
`readPoolManifestOp`, `writePoolManifest`, `writePoolManifestOp`,
`clearPoolManifest`, `clearPoolManifestOp`, `ImportPoolGuidedFromStreamAsync`, `ResetDiskAsync`,
`EncryptedPoolEnvelope`, `EncryptedPoolFile`, `_sendImportPoolStreamSession`.

## Do not rename

- Anything `Vfs*`. It genuinely is a SQLite VFS — accurate, not a leaked
  metaphor.
- `onDiskCipher`, `onDiskNonce`, `onDiskTag`, `VfsOnDiskCiphertextTest`,
  `openFilesWithNonRootedDiskPath`. Here "on disk" means the actual bytes at
  rest, which is correct usage.
- The `.db` / `.dbs` / `.eds` file extensions. Renaming `.eds` would orphan every
  backup users have already exported, including the iPad-verified ones.

## Verification

Run after every group:

```bash
dotnet build SqliteWasmBlazor.slnx -c Debug          # 0 warnings, 0 errors
cd src/Crypto/SqliteWasmBlazor.Crypto/TypeScript
npm run typecheck && npm run lint && npm test        # 52 tests, 5 files
npm run build                                        # regenerate bundles/
```

**Dangling `<see cref>` links.** `GenerateDocumentationFile` is off in this repo,
so unresolvable crefs never surface — a rename can rot a link and nothing warns.
Enable it for an audit:

```bash
dotnet build SqliteWasmBlazor.slnx -c Debug --no-incremental \
  -p:GenerateDocumentationFile=true -p:NoWarn='1591%3B1573%3B1712%3B1570' 2>&1 | grep CS1574
```

`--no-incremental` matters; an up-to-date build does not re-emit warnings. The
baseline is **22 pre-existing** entries, mostly cross-plane links that can never
resolve. Every group above was diffed against it and added none. Two disk-named
crefs were found and fixed at the start: one in the base plane pointing at
`EncryptedPoolState` (which that plane cannot reference — now plain `<c>` text),
and one naming `ImportDiskGuidedAsync`, a method that never existed.

**Still outstanding: a browser run.** Nothing above exercises the two renamed
cross-language contracts at runtime. Run the TestApp Playwright suite before
merging.
