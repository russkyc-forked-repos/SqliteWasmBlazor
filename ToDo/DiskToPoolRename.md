# Disk → Pool rename

Status: **resx keys done**, **group A done**. Groups B, C, D — plus the
`[JSImport]` strings — are open.

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

## Open — group B: base plane

| Symbol | New name | Declared at | refs / files |
| --- | --- | --- | --- |
| `DiskImportResult` | `PoolImportResult` | `Base/Abstractions/ISqliteWasmDatabaseService.cs:19` | 27 / 9 |
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
Each carries a `[JSImport("exportDiskToDownload")]`-style attribute whose string
argument no rename engine touches. Either leave the JS names as they are and
accept that C# and JS diverge in naming, or change both sides in the same
commit. A half-rename compiles and fails at runtime.

## Open — group C: Crypto.UI

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

## Open — group D: TypeScript

Not reachable by any C# rename. Must move in lockstep with the `[JSImport]`
strings in group B and with any worker message `type` values.

`exportDiskToDownload`, `exportDiskToBytesSession`, `exportDiskToStaging`,
`exportDiskToStagingHandler`, `importDiskStreamPreflight`,
`importDiskStreamPreflightFromSession`, `importDiskStreamPreflightHandler`,
`importDiskStreamCommit`, `importDiskStreamCommitFromSession`,
`importDiskStreamCommitHandler`, `importDiskStreamed`, `readDiskManifest`,
`readDiskManifestOp`, `writeDiskManifest`, `writeDiskManifestOp`,
`clearDiskManifest`, `clearDiskManifestOp`, `ImportDiskAsync`, `ResetDiskAsync`,
`EncryptedDiskEnvelope`, `EncryptedDiskFile`, `_sendImportDiskStreamSession`.

## Do not rename

- Anything `Vfs*`. It genuinely is a SQLite VFS — accurate, not a leaked
  metaphor.
- `onDiskCipher`, `onDiskNonce`, `onDiskTag`, `VfsOnDiskCiphertextTest`,
  `openFilesWithNonRootedDiskPath`. Here "on disk" means the actual bytes at
  rest, which is correct usage.
- The `.db` / `.dbs` / `.eds` file extensions. Renaming `.eds` would orphan every
  backup users have already exported, including the iPad-verified ones.

## Verification after each group

```bash
dotnet build samples/SqliteWasmBlazor.Demo/SqliteWasmBlazor.Demo.csproj -c Debug
dotnet build samples/SqliteWasmBlazor.TestApp/SqliteWasmBlazor.TestApp.csproj -c Debug
```

Group B additionally needs a browser run — its failure mode is a JS name that no
longer resolves, which the compiler cannot see. Run the TestApp Playwright suite
after it.
