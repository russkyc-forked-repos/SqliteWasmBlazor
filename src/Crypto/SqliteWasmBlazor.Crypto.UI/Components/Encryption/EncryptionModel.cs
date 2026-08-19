using System.Buffers;
using System.Security.Cryptography;
using System.Text.RegularExpressions;
using Microsoft.AspNetCore.Components.Forms;
using Microsoft.Extensions.Localization;
using ObservableCollections;
using RxBlazorV2.Interface;
using RxBlazorV2.Model;
using RxBlazorV2.MudBlazor.Components;
using SqliteWasmBlazor.Crypto.Abstractions.Formatting;
using SqliteWasmBlazor.Crypto.Services;
using SqliteWasmBlazor.Crypto.UI.Components.Authentication;
using SqliteWasmBlazor.Crypto.UI.Services;

namespace SqliteWasmBlazor.Crypto.UI.Components.Encryption;

// Commands + state for the encryption page. Lifecycle hooks + page-topology
// documentation live in the .Lifecycle.cs partial sibling.
[ObservableModelScope(ModelScope.Scoped)]
[ObservableComponent]
public partial class EncryptionModel : ObservableModel
{
    public partial EncryptionModel(
        IEncryptedSqliteWasmDatabaseService session,
        ISqliteWasmDatabaseService databaseService,
        AuthenticationModel auth,
        DbStateModel dbState,
        IPrfService prfService,
        ISecureKeyCache keyCache,
        IHostDatabaseService hostDatabaseService,
        StatusModel statusModel,
        IStringLocalizer<EncryptionModel> localizer);

    public partial EncryptedDiskState? State { get; set; }
    public partial string PastedRecipientKey { get; set; } = string.Empty;
    public partial string? PastedRecipientError { get; set; }

    /// <summary>
    /// Names of every DB in the SAH pool (sourced from
    /// <c>ISqliteWasmDatabaseService.ListDatabasesAsync</c> at the last
    /// <see cref="RefreshAsync"/>). Drives the multi-select DB picker for
    /// the unified export affordance. Empty list → the picker hides itself.
    /// </summary>
    public partial IReadOnlyList<string> DatabaseNames { get; set; } = Array.Empty<string>();

    /// <summary>
    /// Currently-selected DB names for the unified export affordance. UI
    /// binds a list of checkboxes (one per <see cref="DatabaseNames"/>
    /// entry) to <see cref="ObservableList{T}.Add"/> /
    /// <see cref="ObservableList{T}.Remove"/> on this list. Cardinality
    /// decides the export shape: one entry → vanilla <c>.db</c> file;
    /// ≥ 2 entries → <c>.dbs</c> MessagePack envelope (no compression).
    /// </summary>
    public partial ObservableList<string> SelectedDatabases { get; private init; } = new();

    /// <summary>True when the VFS is plain (no passkey registered yet).</summary>
    public bool IsPlain => State?.Encrypted == false;

    /// <summary>True when the VFS is encrypted and the worker holds the global key.</summary>
    public bool IsUnlocked => State is { Encrypted: true, Unlocked: true };

    /// <summary>True when the VFS is encrypted but the worker has no global key.</summary>
    public bool IsLocked => State is { Encrypted: true, Unlocked: false };

    [ObservableCommand(nameof(RefreshAsync))]
    public partial IObservableCommandAsync Refresh { get; }

    [ObservableCommand(nameof(EnterEncryptedCmdAsync), nameof(CanEnterEncrypted), nameof(FormatOperationError))]
    public partial IObservableCommandAsync EnterEncrypted { get; }

    [ObservableCommand(nameof(LeaveEncryptedCmdAsync), nameof(CanLeaveEncrypted), nameof(FormatOperationError))]
    public partial IObservableCommandAsync LeaveEncrypted { get; }

    [ObservableCommand(nameof(LockCmdAsync), nameof(CanLock), nameof(FormatOperationError))]
    public partial IObservableCommandAsync Lock { get; }

    [ObservableCommand(nameof(ResetCmdAsync), null, nameof(FormatOperationError))]
    public partial IObservableCommandAsync Reset { get; }

    // Plain-disk sign-out — disabled on Encrypted (disk is bound to one
    // credential; switching keys requires Reset or recipient export/import).
    [ObservableCommand(nameof(SignOutCmdAsync), nameof(CanSignOut), nameof(FormatOperationError))]
    public partial IObservableCommandAsync SignOut { get; }

    // Encrypted-disk envelope ops. Encrypted+Unlocked only. Two flavours:
    // backup (verbatim ciphertext under current K, no re-encryption cost)
    // and recipient share (rekey to recipient K).
    [ObservableCommand(nameof(ExportDiskBackupAsync), nameof(CanExportDisk), nameof(FormatOperationError))]
    public partial IObservableCommandAsync ExportDiskBackup { get; }

    [ObservableCommand(nameof(ExportDiskForRecipientAsync), nameof(CanExportDiskForRecipient), nameof(FormatOperationError))]
    public partial IObservableCommandAsync ExportDiskForRecipient { get; }

    // Replaces the entire pool. Caller (page partial) owns the destructive
    // confirmation dialog; parameter is the picked file itself — the model
    // streams it into the JS-side BlobSession one ArrayPool chunk at a time
    // so C# managed heap stays bounded regardless of envelope size.
    [ObservableCommand(nameof(ImportDiskCmdAsync), nameof(CanImportDisk), nameof(FormatOperationError))]
    public partial IObservableCommandAsync<IBrowserFile> ImportDisk { get; }

    // Unified plain export. Reads <see cref="SelectedDatabases"/> and
    // dispatches by cardinality: 1 → vanilla .db download; ≥ 2 →
    // streaming .dbs envelope (MessagePack array of [name, bytes],
    // no compression). Plain disk emits verbatim; Encrypted+Unlocked
    // decrypts slot-by-slot before emit; Encrypted+Locked is refused
    // (CanExportDatabases gates).
    [ObservableCommand(nameof(ExportDatabasesCmdAsync), nameof(CanExportDatabases), nameof(FormatOperationError))]
    public partial IObservableCommandAsync ExportDatabases { get; }

    // Single-DB plain import: streaming write under the caller-chosen pool
    // name (see SingleDatabaseImport for why the name is not the file's).
    // Plain writes plain pages; Encrypted+Unlocked rekey-on-writes under
    // globalKey; Encrypted+Locked is refused (the .eds guided import is the
    // rebind-to-new-credential path).
    [ObservableCommand(nameof(ImportDatabaseCmdAsync), nameof(CanImportDatabases), nameof(FormatOperationError))]
    public partial IObservableCommandAsync<SingleDatabaseImport> ImportDatabase { get; }

    // Multi-DB bundle import: streaming <c>.dbs</c> envelope that replaces
    // the whole pool. Same disk-state gating as the single-DB path.
    [ObservableCommand(nameof(ImportDatabasesCmdAsync), nameof(CanImportDatabases), nameof(FormatOperationError))]
    public partial IObservableCommandAsync<IBrowserFile> ImportDatabases { get; }

    // Pool housekeeping: drop one database from the SAH pool. The pool
    // outlives the app's own databases — imports and retired features leave
    // entries behind — so the export picker needs a way to clean up.
    [ObservableCommand(nameof(DeleteDatabaseCmdAsync), nameof(CanDeleteDatabase), nameof(FormatOperationError))]
    public partial IObservableCommandAsync<string> DeleteDatabase { get; }

    private bool CanEnterEncrypted() =>
        IsPlain
        && !string.IsNullOrEmpty(Auth.PublicKey)
        && !string.IsNullOrEmpty(Auth.CredentialId);

    private bool CanLeaveEncrypted() => IsUnlocked;
    private bool CanLock() => IsUnlocked;
    private bool CanSignOut() => IsPlain && !string.IsNullOrEmpty(Auth.PublicKey);
    private bool CanExportDisk() => IsUnlocked;
    private bool CanExportDiskForRecipient() => IsUnlocked && TryGetPastedRecipientIdentity() is not null;

    // Guided import rebinds the disk to the import's credential — only
    // valid from Plain (no current binding) or Locked (binding exists but
    // worker key not installed). Unlocked is rejected: switching credentials
    // mid-session would orphan in-flight EF contexts and is conceptually a
    // Reset+Import, not an Import.
    private bool CanImportDisk() => IsPlain || IsLocked;

    // Plain export needs at least one DB selected and a disk state that can
    // produce plain pages. Plain disks emit verbatim; Encrypted+Unlocked
    // decrypts on read. Locked has no key to decrypt with — refuse.
    private bool CanExportDatabases() =>
        (IsPlain || IsUnlocked) && SelectedDatabases.Count > 0;

    // Plain import (single .db or multi-DB .dbs envelope) needs a writable
    // state. Plain writes plain pages; Encrypted+Unlocked rekey-on-writes.
    // Locked is refused — the .eds guided import is the rebind-to-new-
    // credential path; this affordance assumes the disk's key is installed.
    private bool CanImportDatabases() => IsPlain || IsUnlocked;

    // Pool deletion needs a writable pool: Plain writes plain pages,
    // Encrypted+Unlocked holds the key the SAHPool needs to keep the
    // remaining slots consistent. Locked is refused like every other
    // pool-mutating command.
    private bool CanDeleteDatabase() => IsPlain || IsUnlocked;

    private async Task RefreshAsync(CancellationToken cancellationToken)
    {
        State = await Session.GetStateAsync(cancellationToken);
        // Refresh the DB list — single-DB export picker hangs off this.
        // Read on every state transition so a newly-created or freshly-
        // imported DB shows up immediately.
        DatabaseNames = await DatabaseService.ListDatabasesAsync(cancellationToken);
        // Drop any selections that point at DBs no longer in the pool so the
        // export button disables itself once the last surviving choice is
        // gone. ObservableList has no RemoveWhere, so collect doomed names
        // first to avoid mutating during iteration.
        var alive = new HashSet<string>(DatabaseNames, StringComparer.Ordinal);
        var doomed = SelectedDatabases.Where(name => !alive.Contains(name)).ToArray();
        foreach (var name in doomed)
        {
            SelectedDatabases.Remove(name);
        }
    }

    private async Task EnterEncryptedCmdAsync(CancellationToken cancellationToken)
    {
        var credentialId = Auth.CredentialId;
        if (string.IsNullOrEmpty(credentialId))
        {
            throw new InvalidOperationException(
                "Authenticate or register a passkey before encrypting the VFS.");
        }

        var keyBytes = await DeriveVfsKeyAsync();
        try
        {
            await Session.EnterEncryptedAsync(keyBytes, credentialId, cancellationToken);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(keyBytes);
        }

        await RefreshAsync(cancellationToken);
        StatusModel.AddSuccess(Localizer["Status_Encrypted"], nameof(EnterEncrypted));
    }

    private async Task LeaveEncryptedCmdAsync(CancellationToken cancellationToken)
    {
        await Session.LeaveEncryptedAsync(cancellationToken);
        await RefreshAsync(cancellationToken);
        StatusModel.AddWarning(Localizer["Status_Decrypted"], nameof(LeaveEncrypted));
    }

    private async Task LockCmdAsync(CancellationToken cancellationToken)
    {
        await Session.LockAsync(cancellationToken);
        await Auth.ClearKeysAsync();
        await RefreshAsync(cancellationToken);
        StatusModel.AddWarning(Localizer["Status_Locked"], nameof(Lock));
    }

    private async Task SignOutCmdAsync(CancellationToken cancellationToken)
    {
        // SignOutAsync flips PublicKey → the OnAuthChangedAsync observer
        // drives Refresh; no manual call needed.
        await Auth.SignOutAsync();
        StatusModel.AddWarning(Localizer["Status_SignedOut"], nameof(SignOut));
    }

    // Single host-side seam owns the full reset sequence (disk wipe + PRF
    // clear + sign-out + per-context re-migrate + boot status → READY).
    // Hosts without recovery register NullHostDatabaseService; the call
    // no-ops there.
    private async Task ResetCmdAsync(CancellationToken cancellationToken)
    {
        await HostDatabaseService.ResetAsync(cancellationToken);
        await RefreshAsync(cancellationToken);
        StatusModel.AddWarning(Localizer["Status_Reset"], nameof(Reset));
    }

    // Backup-to-self uses the asymmetric path with the caller's OWN pubkey
    // as the recipient — restore from the same passkey re-derives the
    // matching wrap key. The credentialId stamped into the envelope is the
    // caller's own, so the guided import drives WebAuthn back to the same
    // passkey on restore.
    private async Task ExportDiskBackupAsync(CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(Auth.PublicKey))
        {
            throw new InvalidOperationException(
                "Cannot back up: not signed in (no X25519 public key available).");
        }
        if (string.IsNullOrEmpty(Auth.CredentialId))
        {
            throw new InvalidOperationException(
                "Cannot back up: not signed in (no WebAuthn credentialId available).");
        }
        var stamp = DateTime.Now.ToString("yyyyMMdd-HHmmss");
        var fileName = $"disk-backup-{stamp}.eds";
        await Session.ExportDiskToPubkeyAndDownloadAsync(
            fileName, Auth.PublicKey, Auth.CredentialId, cancellationToken);
        StatusModel.AddSuccess(
            Localizer["Status_DiskExported", fileName],
            nameof(ExportDiskBackup));
    }

    private async Task ExportDiskForRecipientAsync(CancellationToken cancellationToken)
    {
        var recipient = TryGetPastedRecipientIdentity()
            ?? throw new InvalidOperationException(
                "Pasted recipient identity is missing or invalid.");
        var stamp = DateTime.Now.ToString("yyyyMMdd-HHmmss");
        var fileName = $"disk-recipient-{stamp}.eds";
        await Session.ExportDiskToPubkeyAndDownloadAsync(
            fileName, recipient.PublicKey, recipient.CredentialId, cancellationToken);
        StatusModel.AddSuccess(
            Localizer["Status_DiskExportedForRecipient", fileName],
            nameof(ExportDiskForRecipient));
    }

    /// <summary>
    /// Unified plain-export command. One picked DB → vanilla <c>.db</c>
    /// download (filename = <c>{stem}-{stamp}.db</c>); multiple DBs →
    /// streaming <c>.dbs</c> envelope (filename =
    /// <c>databases-{stamp}.dbs</c>). Plain disks emit verbatim;
    /// Encrypted+Unlocked decrypts each file slot-by-slot so downloads are
    /// vanilla SQLite any tool can open. The service primitive
    /// (<c>ExportDatabasesToDownloadAsync</c>) handles single-vs-multi
    /// dispatch internally; we only choose the filename + status string.
    /// </summary>
    private async Task ExportDatabasesCmdAsync(CancellationToken cancellationToken)
    {
        var picked = SelectedDatabases.ToArray();
        if (picked.Length == 0)
        {
            throw new InvalidOperationException(
                "Select at least one database before exporting.");
        }
        var stamp = DateTime.Now.ToString("yyyyMMdd-HHmmss");
        string fileName;
        string statusKey;
        if (picked.Length == 1)
        {
            var dbName = picked[0];
            var stem = dbName.EndsWith(".db", StringComparison.OrdinalIgnoreCase)
                ? dbName[..^3]
                : dbName;
            fileName = $"{stem}-{stamp}.db";
            statusKey = "Status_SingleDbExported";
        }
        else
        {
            fileName = $"databases-{stamp}.dbs";
            statusKey = "Status_DbsExported";
        }
        await Session.ExportDatabasesToDownloadAsync(picked, fileName, cancellationToken);
        StatusModel.AddSuccess(
            picked.Length == 1
                ? Localizer[statusKey, picked[0], fileName]
                : Localizer[statusKey, picked.Length, fileName],
            nameof(ExportDatabases));
    }

    /// <summary>
    /// Guided import — collapses the recipient ritual (Reset → EnterEncrypted
    /// → ImportDisk) into one orchestrated call. The picked file's stream is
    /// shipped to the JS-side BlobSession one ArrayPool chunk at a time; the
    /// worker re-streams it for AEAD preflight + per-slot rekey commit.
    /// C# managed heap peak stays at one chunk (~1 MB).
    ///
    /// Flow: peek envelope header (first ~4 KB only) → read CredentialIdHint
    /// → drive WebAuthn pinned to that passkey → derive VFS key from the
    /// freshly-cached PRF seed → call Session.ImportDiskGuidedFromStreamAsync.
    /// The PRF cache stays populated through the service call so the
    /// envelope's ECIES K_wrap can be unwrapped under the same seed.
    /// </summary>
    private async Task ImportDiskCmdAsync(IBrowserFile file, CancellationToken cancellationToken)
    {
        if (file is null || file.Size == 0)
        {
            throw new InvalidOperationException(
                "Pick a .eds envelope file before importing.");
        }

        // Header peek — read just the first 4 KB of the picked file so we
        // can extract the CredentialIdHint before driving WebAuthn. The
        // envelope's positional MessagePack header (version + AadVer +
        // PrfSalt + 4 strings) fits comfortably in 4 KB.
        const int peekBytes = 4096;
        var headerSize = (int)Math.Min(peekBytes, file.Size);
        var headerBuffer = new byte[headerSize];
        await using (var peekStream = file.OpenReadStream(maxAllowedSize: file.Size, cancellationToken))
        {
            var read = 0;
            while (read < headerSize)
            {
                var n = await peekStream.ReadAsync(
                    headerBuffer.AsMemory(read, headerSize - read), cancellationToken);
                if (n <= 0) { break; }
                read += n;
            }
            if (read < headerSize)
            {
                Array.Resize(ref headerBuffer, read);
            }
        }

        var hint = await Session.ReadEnvelopeCredentialIdHintAsync(headerBuffer, cancellationToken);
        if (string.IsNullOrEmpty(hint))
        {
            throw new InvalidOperationException(
                Localizer["Error_ImportEnvelopeHasNoCredentialId"]);
        }

        // WebAuthn pinned to the envelope's credentialId — bypasses
        // AuthenticationModel.ApplySessionAsync because that guard rejects
        // any credential whose id doesn't match the current disk hint
        // (would reject this credential up front for the Locked case).
        var derive = await PrfService.DeriveKeysAsync(hint);
        if (derive.Cancelled)
        {
            throw new OperationCanceledException(
                "User cancelled the passkey ceremony.",
                cancellationToken);
        }
        if (!derive.Success || derive.Value is null)
        {
            throw new InvalidOperationException(
                $"PRF derive for envelope's credentialId failed: " +
                $"{derive.Error ?? derive.ErrorCode?.ToString() ?? "unknown"}");
        }
        var importedPublicKey = derive.Value;

        var vfsKey = await DeriveVfsKeyAsync();
        try
        {
            // Re-open the file's stream for the chunked import — the peek
            // stream above was consumed for 4 KB; IBrowserFile yields a
            // fresh stream per OpenReadStream call. The service streams
            // the full body into the JS-side BlobSession one ArrayPool
            // chunk at a time, then drives preflight + commit.
            await using var importStream = file.OpenReadStream(
                maxAllowedSize: file.Size, cancellationToken);
            var result = await Session.ImportDiskGuidedFromStreamAsync(
                importStream, file.Size, vfsKey, hint, cancellationToken);
            if (result == DiskImportResult.WRONG_KEY)
            {
                throw new InvalidOperationException(
                    "Imported envelope's wrap key did not verify under the recipient's PRF-derived priv key " +
                    "(envelope was sealed for a different pubkey than the one the chosen passkey derives).");
            }
            if (result != DiskImportResult.OK)
            {
                throw new InvalidOperationException($"ImportDisk failed: {result}");
            }
        }
        finally
        {
            CryptographicOperations.ZeroMemory(vfsKey);
        }

        // Disk's manifest is now bound to (hint, importedPublicKey). Rebind
        // Auth state without the disk-mismatch guard — the disk has just
        // been rewritten to match this credential.
        Auth.ApplyImportedSession(hint, importedPublicKey);

        await RefreshAsync(cancellationToken);
        StatusModel.AddSuccess(Localizer["Status_DiskImported"], nameof(ImportDisk));
    }

    /// <summary>
    /// Single-DB plain-import command. Streams the picked <c>.db</c> file
    /// into <see cref="SingleDatabaseImport.DatabaseName"/>, creating that
    /// pool entry or replacing it wholesale. The JS-side BlobSession keeps
    /// the C# managed heap bounded regardless of file size. Plain disks
    /// write plain pages; Encrypted+Unlocked rekey-on-writes under
    /// <c>globalKey</c>; Encrypted+Locked is refused (the <c>.eds</c>
    /// guided import is the rebind-to-new-credential path;
    /// <see cref="CanImportDatabases"/> gates the button).
    /// </summary>
    private async Task ImportDatabaseCmdAsync(
        SingleDatabaseImport request, CancellationToken cancellationToken)
    {
        var file = request.File;
        if (file is null || file.Size == 0)
        {
            throw new InvalidOperationException(
                "Pick a .db file before importing.");
        }
        if (!file.Name.EndsWith(".db", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                $"Unsupported single-database file '{file.Name}': expected .db.");
        }
        var target = NormalizeDatabaseName(request.DatabaseName);
        await using var stream = file.OpenReadStream(
            maxAllowedSize: file.Size, cancellationToken);
        await Session.ImportDatabaseFromStreamAsync(
            target, stream, file.Size, cancellationToken);
        await RefreshAsync(cancellationToken);
        StatusModel.AddSuccess(
            Localizer["Status_SingleDbImported", target],
            nameof(ImportDatabase));
    }

    /// <summary>
    /// Multi-DB plain-import command. Replays every <c>[name, bytes]</c>
    /// entry of a <c>.dbs</c> envelope through the chunked write path,
    /// wiping the existing pool first — the pool ends up being exactly what
    /// the envelope carries. Streaming and disk-state rules match
    /// <see cref="ImportDatabaseCmdAsync"/>.
    /// </summary>
    private async Task ImportDatabasesCmdAsync(IBrowserFile file, CancellationToken cancellationToken)
    {
        if (file is null || file.Size == 0)
        {
            throw new InvalidOperationException(
                "Pick a .dbs bundle before importing.");
        }
        if (!file.Name.EndsWith(".dbs", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                $"Unsupported bundle file '{file.Name}': expected .dbs.");
        }
        await using var stream = file.OpenReadStream(
            maxAllowedSize: file.Size, cancellationToken);
        await Session.ImportDatabasesFromStreamAsync(stream, file.Size, cancellationToken);
        await RefreshAsync(cancellationToken);
        StatusModel.AddSuccess(
            Localizer["Status_DbsImported", file.Name],
            nameof(ImportDatabases));
    }

    /// <summary>
    /// Drop one database from the SAH pool and refresh the picker. The pool
    /// is app-wide storage, not a session: whatever an import or a retired
    /// feature created stays until deleted here.
    /// </summary>
    private async Task DeleteDatabaseCmdAsync(
        string databaseName, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(databaseName))
        {
            throw new InvalidOperationException(
                "databaseName must be non-empty.");
        }
        await DatabaseService.DeleteDatabaseAsync(databaseName, cancellationToken);
        await RefreshAsync(cancellationToken);
        StatusModel.AddSuccess(
            Localizer["Status_DatabaseDeleted", databaseName],
            nameof(DeleteDatabase));
    }

    /// <summary>
    /// Pool name proposed for a picked <c>.db</c> file: the file name minus
    /// the stamp <see cref="ExportDatabasesCmdAsync"/> appends, so an
    /// export/import round trip lands back on the database it came from
    /// instead of leaving a <c>TodoDb-20260818-193737.db</c> stray. Any
    /// other name is returned unchanged — this is the value a host prefills
    /// its name field with, never a silent rewrite of the user's choice.
    /// </summary>
    public string ProposeDatabaseName(string fileName)
    {
        if (string.IsNullOrWhiteSpace(fileName))
        {
            throw new ArgumentException("fileName must be non-empty.", nameof(fileName));
        }
        return ExportStampPattern().Replace(fileName, ".db");
    }

    // Trailing "-yyyyMMdd-HHmmss.db" — the shape ExportDatabasesCmdAsync
    // writes. Anchored at the end so a database whose own name contains
    // digits keeps them.
    [GeneratedRegex(@"-\d{8}-\d{6}\.db$", RegexOptions.IgnoreCase)]
    private static partial Regex ExportStampPattern();

    // Pool entries are addressed by file name; a name without the extension
    // would create a database no connection string points at.
    private static string NormalizeDatabaseName(string databaseName)
    {
        var trimmed = databaseName?.Trim() ?? string.Empty;
        if (trimmed.Length == 0)
        {
            throw new InvalidOperationException(
                "Enter the database name to import into.");
        }
        return trimmed.EndsWith(".db", StringComparison.OrdinalIgnoreCase)
            ? trimmed
            : $"{trimmed}.db";
    }

    private async ValueTask<byte[]> DeriveVfsKeyAsync()
    {
        var derive = await PrfService.DeriveDomainKeyAsync(
            EncryptedDiskLifecycle.VfsDomainId,
            EncryptedDiskLifecycle.VfsHkdfContext);
        if (!derive.Success || derive.Value is null)
        {
            throw new InvalidOperationException(
                $"VFS key derive failed: {derive.Error ?? derive.ErrorCode?.ToString() ?? "unknown"}");
        }
        var bytes = KeyCache.TryGet(derive.Value)
            ?? throw new InvalidOperationException(
                "VFS key not present in secure cache after derive.");
        if (bytes.Length != 32)
        {
            CryptographicOperations.ZeroMemory(bytes);
            throw new InvalidOperationException(
                $"VFS key must be 32 bytes, got {bytes.Length}.");
        }
        return bytes;
    }

    // Parse pasted PFA-armored recipient identity — both the X25519 PUBLIC
    // key (32 bytes) and the WebAuthn credentialId are required. The
    // credentialId is the metadata field embedded in the armored payload
    // by the recipient's PublicKeyDisplay (G1). Raw-Base64 pastes without
    // metadata are rejected because the guided-import flow (G3) needs the
    // credentialId to drive WebAuthn's allowCredentials.
    // Sets PastedRecipientError so the markup can show the inline diagnostic.
    private (string PublicKey, string CredentialId)? TryGetPastedRecipientIdentity()
    {
        if (string.IsNullOrWhiteSpace(PastedRecipientKey))
        {
            PastedRecipientError = null;
            return null;
        }

        var (base64Key, metadata) = PrfArmor.UnArmorPublicKeyWithMetadata(PastedRecipientKey);
        if (base64Key is null)
        {
            PastedRecipientError = Localizer["Error_PastedKey_BadBase64"];
            return null;
        }

        byte[] bytes;
        try
        {
            bytes = Convert.FromBase64String(base64Key);
        }
        catch (FormatException)
        {
            PastedRecipientError = Localizer["Error_PastedKey_BadBase64"];
            return null;
        }

        if (bytes.Length != 32)
        {
            CryptographicOperations.ZeroMemory(bytes);
            PastedRecipientError = Localizer["Error_PastedKey_WrongLength", bytes.Length];
            return null;
        }

        if (string.IsNullOrEmpty(metadata?.CredentialId))
        {
            CryptographicOperations.ZeroMemory(bytes);
            PastedRecipientError = Localizer["Error_PastedKey_NoCredentialId"];
            return null;
        }

        CryptographicOperations.ZeroMemory(bytes);
        PastedRecipientError = null;
        return (base64Key, metadata.CredentialId);
    }

    private string FormatOperationError(Exception ex) => ex switch
    {
        OperationCanceledException => Localizer["Status_OperationCancelled"],
        InvalidOperationException => Localizer["Error_Operation", ex.Message],
        _ => Localizer["Error_Operation", ex.Message],
    };
}
