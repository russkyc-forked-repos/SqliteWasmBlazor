// SqliteWasmBlazor - Minimal EF Core compatible provider
// MIT License

namespace SqliteWasmBlazor;

/// <summary>
/// Snapshot of the encrypted-disk state, reported by
/// <see cref="IEncryptedSqliteWasmDatabaseService.GetStateAsync"/>.
/// </summary>
/// <param name="Encrypted">
/// True when the on-disk passkey manifest is present on every DB in the
/// SAH pool — the disk contains ciphertext databases under the manifest's
/// credentialId. False = Plain (no manifest written, no legacy hint).
/// </param>
/// <param name="Unlocked">
/// True when the worker has the global key installed and reads/writes
/// can proceed. False with <see cref="Encrypted"/> = true means
/// <i>Locked</i> (next op fails until <see cref="IEncryptedSqliteWasmDatabaseService.UnlockAsync"/>).
/// </param>
/// <param name="Hint">
    /// The encrypted-disk passkey credentialId from the on-disk manifest.
    /// Set whenever <see cref="Encrypted"/> = true. UI uses
/// it to render "Sign in with passkey: {hint}" so the user recognises
/// which credential to provide. Null when the manifest is in a corrupted
/// state (Mismatch / Tampered / Malformed) — UI surfaces a "ResetPool
/// required" affordance instead of routing to a specific credential.
/// </param>
public sealed record EncryptedPoolState(bool Encrypted, bool Unlocked, string? Hint)
{
    /// <summary>
    /// Pristine disk — no registered passkey, no global key.
    /// </summary>
    public static EncryptedPoolState Plain { get; } = new(false, false, null);
}

/// <summary>
/// Encrypted-disk lifecycle + whole-disk envelope I/O. The encrypted disk
/// is a <i>virtual encrypted device</i>: one global key, every database in
/// the SAH pool encrypted under it, one-shot unlock at session start, lock
/// at session end. Whole-disk only — per-DB sharing isn't supported (mixed
/// plain + encrypted state is not representable).
///
/// <para>
/// <b>Audience.</b> Apps wanting passkey-encrypted persistence wrap their
/// content in <c>&lt;AuthorizeView Policy="DatabaseOpen"&gt;</c> and call
/// the lifecycle methods (Enter/Leave/Lock/Unlock/Reset) plus
/// Export/ImportPool for backup/restore/share. Plain SQLite-on-OPFS apps
/// don't need this interface at all — they use
/// <see cref="ISqliteWasmDatabaseService"/> only.
/// </para>
///
/// <para>
/// <b>Wire formats.</b> Encrypted whole-disk uses the asymmetric
/// <c>.eds</c> envelope (ECIES-wrapped per-export key, slot-format
/// ciphertext). Plain interop is per-DB <c>.db</c> files (one file per
/// database) or a multi-DB <c>.dbs</c> envelope (uncompressed MessagePack
/// array of <c>[name, bytes]</c>). All three are streamed end-to-end —
/// no managed <see cref="byte"/>[] of the full payload at any layer.
/// </para>
///
/// <para>
/// <b>State source of truth.</b> The on-disk passkey manifest
/// (bytes 524..1023 of every SAH slot's plaintext header sector — see
/// <c>worker/vfs-prf/manifest.ts</c>) is the encrypted/plain signal:
/// manifest present ⇒ encrypted, manifest absent ⇒ plain. The manifest
/// is owned by this interface (Enter writes it atomically as the last
/// step; Leave/Reset clear it). Co-located with the SAH slots so it
/// cannot drift from the disk's actual content.
/// </para>
///
/// <para>
/// Registered as a singleton by <c>AddSqliteWasm()</c>. Production code
/// consumes this interface; bridge primitives (per-DB encrypt-in-place,
/// mode-based export) are internal implementation detail.
/// </para>
/// </summary>
public interface IEncryptedSqliteWasmDatabaseService
{
    /// <summary>
    /// Current state — derived from the on-disk manifest (presence of the
    /// PFAM magic on every DB ⇒ Encrypted) plus the in-memory tracking of
    /// whether <see cref="UnlockAsync"/> has run since the last
    /// <see cref="LockAsync"/>. One bridge round-trip per call (manifest
    /// region of every DB is read and compared); cheap.
    /// </summary>
    Task<EncryptedPoolState> GetStateAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Unlock the disk with the supplied 32-byte ChaCha20-Poly1305 key.
    /// The credential hint is owned by the auth flow; this method only
    /// ships the key to the worker. The worker closes every cached DB at
    /// the session boundary so SQLite's page cache can't leak K_old
    /// plaintext into a K_new session. Idempotent.
    /// </summary>
    /// <param name="key">Exactly 32 bytes of key material.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task UnlockAsync(ReadOnlyMemory<byte> key,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Lock the disk — drops the worker's <c>globalKey</c> and force-
    /// closes every open DB. The passkey hint is preserved so the next
    /// boot still sees the disk as encrypted (and prompts for credentials).
    /// Idempotent — when called on a Plain disk just clears any stray key
    /// without engaging the disk-locked gate.
    /// </summary>
    Task LockAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// <b>Plain → Encrypted transition.</b> Encrypts every database
    /// currently in the SAH pool in place under <paramref name="key"/>,
    /// installs the global key, then writes <paramref name="credentialId"/>
    /// into the on-disk manifest of every DB so <see cref="GetStateAsync"/>
    /// reflects Encrypted+Unlocked. Manifest write is the last step —
    /// atomic from the consumer's perspective: either the whole transition
    /// succeeds or the disk stays Plain.
    ///
    /// <para>
    /// Caller invariant: <see cref="GetStateAsync"/> must currently
    /// return <see cref="EncryptedPoolState.Plain"/>. Throws
    /// <see cref="PoolOperationRejectedException"/>
    /// (<see cref="PoolOperationRejection.ENTER_NEEDS_PLAIN"/>) otherwise.
    /// The auth flow's Register ceremony returns the credential id to pass here.
    /// </para>
    /// </summary>
    /// <param name="key">Exactly 32 bytes of key material.</param>
    /// <param name="credentialId">
    /// The WebAuthn credential id returned by the Register ceremony.
    /// Stored in the manifest body and consulted by the auth flow for
    /// the wrong-passkey early-out.
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task EnterEncryptedAsync(ReadOnlyMemory<byte> key,
        string credentialId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// <b>Encrypted → Plain transition.</b> Decrypts every database in the
    /// SAH pool in place under the active <c>globalKey</c>, clears the
    /// passkey hint, and drops the global key. The disk ends in
    /// <see cref="EncryptedPoolState.Plain"/>. After this, native SQLite
    /// interop is available via <see cref="ISqliteWasmDatabaseService"/>'s
    /// per-DB or batch-ZIP export.
    ///
    /// <para>
    /// Caller invariant: must currently be Encrypted+Unlocked. Throws
    /// <see cref="PoolOperationRejectedException"/>
    /// (<see cref="PoolOperationRejection.LEAVE_NEEDS_UNLOCK"/>) otherwise. Caller is
    /// separately responsible for revoking the passkey credential at the
    /// WebAuthn layer.
    /// </para>
    /// </summary>
    Task LeaveEncryptedAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Streaming asymmetric disk export. The recipient is identified by
    /// their X25519 PUBLIC key (Base64) + their WebAuthn credentialId —
    /// never their VFS key. The sender:
    /// <list type="number">
    ///   <item>Generates a random 32-byte content key (the per-export disk
    ///         wrap key, <c>K_wrap</c>).</item>
    ///   <item>Wraps <c>K_wrap</c> via ECIES (X25519 ECDH +
    ///         HKDF + AES-256-GCM) to <paramref name="recipientX25519PublicKeyBase64"/>.</item>
    ///   <item>Rekeys every page from the current <c>globalKey</c> under
    ///         <c>K_wrap</c> in chunked slot-batches; the worker emits each
    ///         batch to the bridge as a Blob part, the bridge composes the
    ///         v3 envelope as a virtual-concat Blob, the browser disk-backs
    ///         the parts list and an anchor click triggers the download.
    ///         <see cref="byte"/>[] never enters the picture.</item>
    /// </list>
    /// Recipient unwraps with their PRF-derived X25519 private key; only the
    /// holder of that key can recover <c>K_wrap</c>. The credentialId hint
    /// carried in the envelope lets the guided-import UI drive WebAuthn's
    /// <c>allowCredentials</c> to the exact passkey the recipient needs.
    /// <para>
    /// <b>Backup vs share.</b> "Backup to self" passes the caller's own
    /// X25519 pubkey + credentialId — the caller's passkey re-derives the
    /// matching private key on next session. "Share with peer" passes the
    /// peer's pubkey + credentialId (both carried in the peer's armored PFA
    /// PUBLIC KEY block). Mechanics identical; only the recipient identity
    /// differs.
    /// </para>
    /// <para>
    /// Caller invariant: must be Encrypted+Unlocked. Throws otherwise.
    /// <paramref name="recipientX25519PublicKeyBase64"/> must decode to a
    /// 32-byte raw X25519 pubkey (UI is responsible for unarmoring before
    /// calling). <paramref name="recipientCredentialId"/> must be a
    /// non-empty Base64 credentialId.
    /// </para>
    /// </summary>
    Task ExportPoolToPubkeyAndDownloadAsync(
        string filename,
        string recipientX25519PublicKeyBase64,
        string recipientCredentialId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Peek inside a v3 disk-import envelope (or its first ~4 KB) to
    /// extract the embedded <c>CredentialIdHint</c>. Used by the
    /// guided-import UI to know which passkey to drive WebAuthn against
    /// BEFORE the wipe-and-rebind ritual starts.
    /// </summary>
    /// <returns>
    /// The envelope's credentialId hint, or <c>null</c> if the envelope is
    /// malformed, an unsupported version, or carries an empty hint.
    /// </returns>
    ValueTask<string?> ReadEnvelopeCredentialIdHintAsync(
        ReadOnlyMemory<byte> envelope,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Streaming guided import — collapses the recipient ritual
    /// (Reset → EnterEncrypted → ImportPool) into one atomic call. The
    /// envelope arrives as a <see cref="Stream"/> (typically
    /// <c>IBrowserFile.OpenReadStream(file.Size)</c>) and is shipped to a
    /// JS-side BlobSession one ArrayPool chunk at a time. C# managed heap
    /// peak stays at one chunk (~1 MB) regardless of envelope size; the
    /// JS Blob parts list is browser-managed (Safari disk-backs above
    /// ~50 MB).
    ///
    /// <para>
    /// Sequence: peek the first ~4 KB into a small <c>MemoryStream</c> →
    /// ECIES-unwrap <c>K_wrap</c> via the caller's PRF seed → AEAD-preflight
    /// every envelope file (worker reads via <c>blob.stream()</c>) → wipe
    /// the existing pool (PRF cache preserved) → <see cref="EnterEncryptedAsync"/>
    /// under (<paramref name="vfsKey"/>, <paramref name="credentialId"/>) →
    /// rekey-commit every envelope file under the freshly-installed
    /// <c>globalKey</c>. The disk ends Encrypted+Unlocked, bound to the
    /// import's credential.
    /// </para>
    /// <para>
    /// Caller invariant: state must be <see cref="EncryptedPoolState.Plain"/>
    /// or Encrypted+Locked. Encrypted+Unlocked is rejected with
    /// <see cref="PoolOperationRejection.GUIDED_IMPORT_NEEDS_LOCK"/> — caller must
    /// <see cref="LockAsync"/> first. The envelope's <c>CredentialIdHint</c>
    /// must match <paramref name="credentialId"/>; <paramref name="vfsKey"/>
    /// must come from the WebAuthn ceremony pinned to that credential.
    /// </para>
    /// </summary>
    Task<PoolImportResult> ImportPoolGuidedFromStreamAsync(
        Stream envelopeStream,
        long envelopeSize,
        ReadOnlyMemory<byte> vfsKey,
        string credentialId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Streaming single-DB export — downloads one DB as a raw plain
    /// <c>.db</c> file (the format <c>sqlite3 file.db</c> can open). State-
    /// aware: Plain disk emits verbatim; Encrypted+Unlocked decrypts each
    /// slot to plain pages before emit; Encrypted+Locked throws.
    ///
    /// <para>
    /// C# never sees the bytes — the worker reads the source in slices via
    /// <c>exportFileSlice</c> and writes them into an OPFS staging file, the
    /// bridge lifts the finished entry as a disk-backed <c>File</c>, and an
    /// anchor click triggers the save. Output is byte-identical to what the
    /// import side reads on the other end of
    /// <see cref="ImportDatabaseFromStreamAsync"/>.
    /// </para>
    /// </summary>
    Task ExportDatabaseToDownloadAsync(
        string databaseName,
        string filename,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Streaming multi-DB plain export — downloads the selected DBs as a
    /// <c>.dbs</c> envelope (MessagePack <c>array</c> of <c>[name, bytes]</c>
    /// tuples; no compression). State-aware: Plain disk emits each file
    /// verbatim; Encrypted+Unlocked decrypts each file slot-by-slot to
    /// plain pages before emit; Encrypted+Locked throws.
    ///
    /// <para>
    /// One-element <paramref name="databaseNames"/> short-circuits to
    /// <see cref="ExportDatabaseToDownloadAsync"/> so the download is a
    /// vanilla <c>.db</c> file (no envelope wrapper). Callers can pass a
    /// list of any length without branching at the call site.
    /// </para>
    /// </summary>
    Task ExportDatabasesToDownloadAsync(
        IReadOnlyList<string> databaseNames,
        string filename,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Streaming multi-DB plain import — reads a <c>.dbs</c> envelope from
    /// <paramref name="envelopeStream"/>. Wipes the existing pool first
    /// (matches the old plain-ZIP-on-Plain semantics) then writes each
    /// envelope entry through the chunked SAH path. State-aware: Plain
    /// disk writes plain pages; Encrypted+Unlocked rekey-on-writes under
    /// the registered globalKey; Encrypted+Locked throws.
    ///
    /// <para>
    /// C# managed-heap peak: one ArrayPool chunk (~1 MB). The envelope is
    /// streamed into a JS-side BlobSession chunk by chunk; the worker
    /// reads via <c>blob.stream()</c> + the streaming MessagePack decoder.
    /// </para>
    /// <para>
    /// <b>Validated import.</b> Pass <paramref name="validateImported"/> to
    /// see what arrived before it becomes the pool's content: the previous
    /// databases are parked under
    /// <see cref="PoolNaming.ImportParkSuffix"/>, the envelope's entries
    /// land under their real names, and the delegate is called once per
    /// imported database — long enough to open each with an ordinary
    /// connection string and check whatever the caller considers identity.
    /// A delegate that throws propagates and the pool goes back exactly as
    /// it was, parked bytes and all. Without it the envelope replaces the
    /// pool unconditionally.
    /// </para>
    /// </summary>
    Task ImportDatabasesFromStreamAsync(
        Stream envelopeStream,
        long envelopeSize,
        Func<string, CancellationToken, ValueTask>? validateImported = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Streaming single-DB plain import — the right primitive for "I have
    /// one big <c>.db</c> file and want it on this disk". State-aware:
    /// writes plain pages on a Plain disk, rekey-on-write to encrypted
    /// slots on Encrypted+Unlocked, throws on Encrypted+Locked (caller
    /// should Unlock first; the <c>.eds</c> guided import is the
    /// rebind-to-new-credential path).
    ///
    /// <para>
    /// C# managed-heap peak: one ArrayPool chunk (~1 MB) regardless of file
    /// size. The source <paramref name="stream"/> is shipped into the
    /// JS-side BlobSession one chunk at a time; the worker reads it via
    /// <c>blob.stream()</c> and writes a temp SAH slot then atomic-promotes
    /// to <paramref name="databaseName"/>. Existing same-name DB is
    /// replaced.
    /// </para>
    /// <para>
    /// <b>Validated import.</b> Pass <paramref name="validateImported"/> to
    /// see the imported database before it becomes the database: the file
    /// lands under <paramref name="databaseName"/> (page AAD binds
    /// ciphertext to the database path, so it cannot be written anywhere
    /// else and moved), what was there is parked under
    /// <see cref="PoolNaming.ImportParkSuffix"/>, and the delegate is called
    /// with the database name — long enough to open it with an ordinary
    /// connection string and check whatever the caller considers identity
    /// (its tables, its migration history). A delegate that throws
    /// propagates, the imported file is dropped and the parked content goes
    /// back untouched. This is what keeps a <c>.db</c> file from landing in
    /// a database it does not belong to; without it the file replaces the
    /// database unconditionally.
    /// </para>
    /// </summary>
    Task ImportDatabaseFromStreamAsync(
        string databaseName,
        Stream stream,
        long size,
        Func<string, CancellationToken, ValueTask>? validateImported = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Scorched-earth disk reset. Closes every open DB, drops
    /// <c>globalKey</c>, deletes every DB file from the pool, and clears
    /// the PRF cache so the auth UI flips to NotAuthorized in lockstep.
    /// The next state probe sees an empty pool (manifest absent) and
    /// reports <see cref="EncryptedPoolState.Plain"/>.
    ///
    /// <para>
    /// Wiping the files is part of the contract — without it, the next
    /// <see cref="EnterEncryptedAsync"/> would walk the still-present
    /// ciphertext slots, run encrypt-in-place against them, and the
    /// worker's plain-source shape check would reject. So Reset →
    /// re-Encrypt → Import is the canonical recipient flow for the
    /// asymmetric envelope, and Reset is the only "factory reset"
    /// affordance the UI needs.
    /// </para>
    /// <para>
    /// Use for "factory reset" UX or test fixture cleanup. For routine
    /// session lifecycle, prefer <see cref="LockAsync"/> /
    /// <see cref="LeaveEncryptedAsync"/> — those don't destroy data.
    /// </para>
    /// </summary>
    Task ResetPoolAsync(CancellationToken cancellationToken = default);
}
