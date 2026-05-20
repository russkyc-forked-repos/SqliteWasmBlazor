// SqliteWasmBlazor - Minimal EF Core compatible provider
// MIT License

using System.Buffers;
using System.IO.Compression;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using MessagePack;
using SqliteWasmBlazor.Crypto.Abstractions;
using SqliteWasmBlazor.Crypto.Abstractions.Models;
using SqliteWasmBlazor.Crypto.Services;

namespace SqliteWasmBlazor;

/// <summary>
/// Production implementation of <see cref="IEncryptedSqliteWasmDatabaseService"/>.
/// Composes the worker bridge primitives (<c>SetEncryptionKeyAsync</c> /
/// <c>ClearEncryptionKeyAsync</c> / <c>ListDatabasesAsync</c> / per-DB
/// encrypt-in-place / decrypt-in-place / export) into the session-shaped
/// lifecycle and the four boundary scenarios.
///
/// <para>
/// <b>Encrypted-state signal.</b> The disk-bound passkey manifest
/// (bytes 524..1023 of every SAH slot's plaintext header sector — see
/// <c>worker/vfs-prf/manifest.ts</c>) is the source of truth for whether
/// the VFS is encrypted. Manifest present on every DB in the pool ⇒
/// Plain → Encrypted transition completed ⇒ DBs on disk are ciphertext
/// under the PRF-derived key. Manifest absent ⇒ VFS is plain. Co-located
/// with the SAHPool slots so it cannot drift from the disk state.
/// </para>
///
/// <para>
/// Singleton — registered by <c>AddSqliteWasmBlazorCrypto()</c> (lives
/// alongside <c>IPrfService</c> because <see cref="ResetDiskAsync"/>
/// implicitly clears the PRF cache to keep the auth UI in lockstep with
/// the disk's Plain transition). Tracks <see cref="_isUnlocked"/> in
/// memory to avoid an extra worker round-trip in
/// <see cref="GetStateAsync"/>.
/// </para>
/// </summary>
internal sealed class EncryptedSqliteWasmDatabaseService
    : IEncryptedSqliteWasmDatabaseService, IDatabaseLockProbe
{
    private const int PlainVfsSlotSize = 4096;

    private readonly SqliteWasmWorkerBridge _bridge;
    private readonly EncryptedSqliteWasmWorkerBridge _encryptedBridge;
    private readonly IDbInitializationReporter _reporter;
    private readonly IDbInitializationStatus _status;
    private readonly IPrfService _prfService;
    private readonly ICryptoProvider _cryptoProvider;
    private bool _isUnlocked;

    /// <summary>
    /// CredentialId that EnterEncryptedAsync intends to bind the disk to.
    /// Held in memory across an Encrypted+Unlocked session so we can auto-
    /// heal the manifest when the pool was empty at EnterEncrypted time
    /// (no DBs yet → nothing to write into) and DBs are subsequently
    /// created — typically via DbContext.EnsureCreatedAsync. Cleared on
    /// Leave/Reset.
    /// </summary>
    private string? _expectedCredentialId;

    public EncryptedSqliteWasmDatabaseService(
        IDbInitializationReporter reporter,
        IDbInitializationStatus status,
        IPrfService prfService,
        ICryptoProvider cryptoProvider)
    {
        _bridge = SqliteWasmWorkerBridge.Instance;
        _encryptedBridge = EncryptedSqliteWasmWorkerBridge.Instance;
        _reporter = reporter;
        _status = status;
        _prfService = prfService;
        _cryptoProvider = cryptoProvider;
    }

    /// <summary>
    /// Bridge between the encrypted-VFS session lifecycle and
    /// <see cref="IDbInitializationStatus"/>: the
    /// <c>PrfAuthenticationStateProvider</c> emits a
    /// <c>DatabaseState=OPEN/LOCKED</c> claim from this state, and the
    /// <c>DatabaseOpen</c> AuthorizeView policy gates page content on it.
    /// Unlock/Lock/Reset must report through here so every &lt;AuthorizeView&gt;
    /// in the tree re-evaluates without a manual UI poke.
    /// </summary>
    private void ReportDbState(DbInitState state, IDbInitFailure? failure = null)
    {
        // Don't downgrade out of a hard-stop boot diagnosis (TAB_LOCKED,
        // SCHEMA_INCOMPATIBLE, FAILED, TIMEOUT) — those need user action
        // beyond unlock/lock and shouldn't be silently overwritten.
        if (_status.State is DbInitState.TAB_LOCKED
                            or DbInitState.SCHEMA_INCOMPATIBLE
                            or DbInitState.TIMEOUT
                            or DbInitState.FAILED)
        {
            return;
        }
        _reporter.Report(state, failure);
    }

    // IDatabaseLockProbe — plane-1-facing minimal probe so
    // InitializeSqliteWasmDatabaseAsync<TContext> can detect ENCRYPTED_LOCKED
    // boot state without referencing plane-2 types. Maps the rich
    // EncryptedDiskState down to the three fields plane 1 cares about.
    async Task<DatabaseLockState> IDatabaseLockProbe.GetStateAsync(CancellationToken cancellationToken)
    {
        var state = await GetStateAsync(cancellationToken);
        return new DatabaseLockState(state.Encrypted, state.Unlocked, state.Hint);
    }

    public async Task<EncryptedDiskState> GetStateAsync(CancellationToken cancellationToken = default)
    {
        // Manifest is the source of truth for the Encrypted/Plain axis.
        // Disk-as-unit invariant guarantees one shared manifest across
        // every DB; the worker side asserts equality on read.
        var (state, credentialId) = await ReadManifestAsync(verifyMac: _isUnlocked, cancellationToken);

        switch (state)
        {
            case ManifestState.PRESENT:
                _expectedCredentialId = credentialId;
                _bridge.SetDiskLocked(!_isUnlocked);
                return new EncryptedDiskState(true, _isUnlocked, credentialId);

            case ManifestState.ABSENT:
                return await ProbeAbsentManifestAsync(cancellationToken);

            case ManifestState.MISMATCH:
                // Mismatch can arise legitimately when a new DB is created
                // post-EnterEncrypted while another DB already carries the
                // manifest (the worker doesn't auto-stamp new SAH slots).
                // If we know who owns this disk and the worker still holds
                // the global key, re-flush the manifest across every DB to
                // restore the disk-as-unit invariant. If we don't, the
                // disk is genuinely inconsistent — surface as Encrypted+
                // Locked with no hint so the UI offers a reset.
                if (_expectedCredentialId is { Length: > 0 } expectedForHeal
                    && _isUnlocked)
                {
                    await WriteManifestAsync(expectedForHeal, cancellationToken);
                    _bridge.SetDiskLocked(false);
                    return new EncryptedDiskState(true, true, expectedForHeal);
                }
                _isUnlocked = false;
                _bridge.SetDiskLocked(true);
                return new EncryptedDiskState(true, false, null);

            case ManifestState.MALFORMED:
            case ManifestState.TAMPERED:
                // Surface corruption as Encrypted+Locked with no hint —
                // UI sees the auth panel + reset escape hatch but can't
                // route to a specific passkey because the manifest can't
                // be trusted. Caller's only recovery is ResetDiskAsync.
                _isUnlocked = false;
                _bridge.SetDiskLocked(true);
                return new EncryptedDiskState(true, false, null);

            default:
                throw new InvalidOperationException(
                    $"Unhandled disk manifest state '{state}'.");
        }
    }

    /// <summary>
    /// Manifest absent — two cases:
    ///   1. Empty-pool Encrypted+Unlocked — EnterEncryptedAsync was called
    ///      while the pool had no DBs (so the manifest write was a no-op),
    ///      and we still hold the credentialId in memory. Now that a DB
    ///      may exist (e.g. <c>DbContext.EnsureCreatedAsync</c> just ran),
    ///      flush the manifest into it.
    ///   2. Genuine Plain — no manifest, no expected credentialId.
    /// </summary>
    private async Task<EncryptedDiskState> ProbeAbsentManifestAsync(CancellationToken cancellationToken)
    {
        if (_expectedCredentialId is { Length: > 0 } expected && _isUnlocked)
        {
            var dbs = await _bridge.ListDatabasesAsync(cancellationToken);
            if (dbs.Count > 0)
            {
                await WriteManifestAsync(expected, cancellationToken);
            }
            _bridge.SetDiskLocked(false);
            return new EncryptedDiskState(true, true, expected);
        }

        _isUnlocked = false;
        _bridge.SetDiskLocked(false);
        return EncryptedDiskState.Plain;
    }

    /// <summary>
    /// Read the disk manifest via the worker bridge and unpack the
    /// MessagePack body. Returns <see cref="ManifestState.PRESENT"/> +
    /// the credentialId when the manifest is intact; other states are
    /// reported with a null credentialId (caller branches on state).
    /// </summary>
    private async Task<(ManifestState State, string? CredentialId)> ReadManifestAsync(
        bool verifyMac, CancellationToken cancellationToken)
    {
        var (raw, body, _) = await _encryptedBridge.ReadDiskManifestAsync(verifyMac, cancellationToken);
        var state = raw switch
        {
            "absent" => ManifestState.ABSENT,
            "present" => ManifestState.PRESENT,
            "mismatch" => ManifestState.MISMATCH,
            "tampered" => ManifestState.TAMPERED,
            "malformed" => ManifestState.MALFORMED,
            _ => throw new InvalidOperationException(
                $"Worker returned unexpected disk-manifest state '{raw}'."),
        };
        if (state != ManifestState.PRESENT || body is null)
        {
            return (state, null);
        }

        DiskManifestBody decoded;
        try
        {
            decoded = MessagePackSerializer.Deserialize<DiskManifestBody>(body);
        }
        catch (MessagePackSerializationException)
        {
            // Body bytes don't deserialize even though magic + HMAC checked
            // out — surface as malformed (probably a future-schema body
            // landed under a current-schema parser).
            return (ManifestState.MALFORMED, null);
        }
        return (ManifestState.PRESENT, decoded.CredentialId ?? string.Empty);
    }

    /// <summary>
    /// Build the MessagePack body for <paramref name="credentialId"/>
    /// (diagnostic fingerprint left empty — not load-bearing for unlock)
    /// and ship it to the worker, which derives the manifest MAC key from
    /// the active globalKey and writes the 500-byte region into every DB
    /// in the pool.
    /// </summary>
    private Task WriteManifestAsync(string credentialId, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(credentialId))
        {
            throw new ArgumentException(
                "credentialId must be non-empty when writing the disk manifest.",
                nameof(credentialId));
        }
        var body = new DiskManifestBody
        {
            CredentialId = credentialId,
            PublicKeyFingerprint = string.Empty,
        };
        var bytes = MessagePackSerializer.Serialize(body);
        return _encryptedBridge.WriteDiskManifestAsync(bytes, cancellationToken);
    }

    public async Task UnlockAsync(
        ReadOnlyMemory<byte> key,
        CancellationToken cancellationToken = default)
    {
        if (key.Length != 32)
        {
            throw new ArgumentException(
                $"key must be exactly 32 bytes, got {key.Length}", nameof(key));
        }

        // State-aware. Mirrors LockAsync's plain-disk handling.
        //   - Encrypted disk: install + verify manifest MAC under the key.
        //     A wrong key trips VerifyUnlockedManifestAsync, which clears
        //     globalKey, drops back to Encrypted+Locked, and throws. This
        //     is the security improvement from the manifest-MAC audit.
        //   - Plain disk (no manifest yet): UnlockAsync is being used as
        //     an "install globalKey primitive" — by test fixtures, by the
        //     synthetic-PRF-seed paths, and by any pre-EnterEncrypted
        //     setup. Install silently and skip verify; there's nothing
        //     to verify against. The MAC-bound security guarantee for
        //     the encrypted-disk path is preserved.
        var pre = await GetStateAsync(cancellationToken);
        await InstallEncryptionKeyAsync(key, cancellationToken);
        if (pre.Encrypted)
        {
            await VerifyUnlockedManifestAsync(allowAbsentForEmptyPool: false, cancellationToken);
        }
        ReportDbState(DbInitState.READY);
    }

    private async Task InstallEncryptionKeyAsync(
        ReadOnlyMemory<byte> key,
        CancellationToken cancellationToken)
    {
        // Disk-as-unit: install globalKey in the worker and release the
        // bridge gate so DB ops route through the encrypted hot path.
        await _encryptedBridge.SetEncryptionKeyAsync(key, cancellationToken);
        _isUnlocked = true;
        _bridge.SetDiskLocked(false);
    }

    private async Task VerifyUnlockedManifestAsync(
        bool allowAbsentForEmptyPool,
        CancellationToken cancellationToken)
    {
        var (state, credentialId) = await ReadManifestAsync(verifyMac: true, cancellationToken);
        switch (state)
        {
            case ManifestState.PRESENT:
                _expectedCredentialId = credentialId;
                return;

            case ManifestState.ABSENT when allowAbsentForEmptyPool:
                return;

            default:
                await _encryptedBridge.ClearEncryptionKeyAsync(cancellationToken);
                _isUnlocked = false;
                _bridge.SetDiskLocked(state != ManifestState.ABSENT);
                ReportDbState(
                    state == ManifestState.ABSENT ? DbInitState.READY : DbInitState.ENCRYPTED_LOCKED,
                    state == ManifestState.ABSENT
                        ? null
                        : new EncryptedDatabaseLockedFailure(string.Empty, string.Empty));
                throw new InvalidOperationException(
                    $"Unlock rejected: disk manifest did not verify under the supplied key (state={state}).");
        }
    }

    public async Task LockAsync(CancellationToken cancellationToken = default)
    {
        // State-aware lock. Three possible starting points:
        //   - Encrypted+Unlocked: real lock transition. Engage gate FIRST so
        //     in-flight or post-clear DB ops fail with DiskLockedException
        //     instead of reading ciphertext as plain. Then drop globalKey
        //     and report ENCRYPTED_LOCKED so the AuthorizeView gate flips.
        //   - Plain (no manifest): callers (typically test fixtures) use Lock
        //     as a "drop any stray globalKey" primitive. Honor that
        //     semantically: clear key, but DO NOT engage the gate (would
        //     break plain ops) and DO NOT report ENCRYPTED_LOCKED.
        //   - Encrypted+Locked already: drop the (empty) key idempotently and
        //     re-engage the gate; nothing else to do.
        var state = await GetStateAsync(cancellationToken);
        var encrypted = state.Encrypted;

        if (encrypted)
        {
            _bridge.SetDiskLocked(true);
        }

        await _encryptedBridge.ClearEncryptionKeyAsync(cancellationToken);
        _isUnlocked = false;

        if (encrypted)
        {
            ReportDbState(
                DbInitState.ENCRYPTED_LOCKED,
                new EncryptedDatabaseLockedFailure(string.Empty, state.Hint ?? string.Empty));
        }
    }

    public async Task EnterEncryptedAsync(
        ReadOnlyMemory<byte> key,
        string credentialId,
        CancellationToken cancellationToken = default)
    {
        if (key.Length != 32)
        {
            throw new ArgumentException(
                $"key must be exactly 32 bytes, got {key.Length}", nameof(key));
        }
        if (string.IsNullOrWhiteSpace(credentialId))
        {
            throw new ArgumentException(
                "credentialId must be non-empty (Base64 WebAuthn credential id from Register).",
                nameof(credentialId));
        }

        var current = await GetStateAsync(cancellationToken);
        if (current.Encrypted)
        {
            throw new InvalidOperationException(
                "EnterEncryptedAsync requires EncryptedDiskState.Plain — VFS is already encrypted.");
        }

        var databases = await _bridge.ListDatabasesAsync(cancellationToken);
        var encryptedSoFar = new List<string>(databases.Count);
        try
        {
            // Phase 1: install the global key BEFORE the encrypt loop.
            // Install-K-first ordering avoids the managed byte[] backup of
            // every plain DB that the pre-G7 path did — a 247 MB DB no
            // longer OOMs Mobile Safari before the first worker call.
            // Mid-loop failure rolls back via per-DB decrypt-in-place
            // under the still-registered K, so the crash-safety contract
            // matches the byte[] backup era with zero managed allocation
            // per DB.
            await InstallEncryptionKeyAsync(key, cancellationToken);

            // Phase 2: walk every plain DB in OPFS, encrypt-in-place under K.
            // EncryptDatabaseInPlaceAsync is per-DB; the worker closes each
            // DB during the conversion, so OFile state can't leak. Track
            // encryptedSoFar so a later failure can decrypt back exactly
            // the DBs that crossed the cipher boundary.
            foreach (var db in databases)
            {
                await _encryptedBridge.EncryptDatabaseInPlaceAsync(db, key, cancellationToken);
                encryptedSoFar.Add(db);
            }

            // Phase 3: write the disk-bound manifest as the last atomic step.
            // GetStateAsync flips to Encrypted+Unlocked only after every DB has
            // been re-wrapped, the worker holds the key, AND the manifest has
            // been recorded onto every DB's header sector. When the pool is
            // empty (caller invokes EnterEncryptedAsync before any DB exists),
            // the worker write is a no-op; we still cache _expectedCredentialId
            // so the next GetStateAsync auto-heals once a DB has been created.
            _expectedCredentialId = credentialId;
            await WriteManifestAsync(credentialId, cancellationToken);
            await VerifyUnlockedManifestAsync(
                allowAbsentForEmptyPool: databases.Count == 0,
                cancellationToken);
        }
        catch
        {
            await RollBackEnterEncryptedAsync(encryptedSoFar, CancellationToken.None);
            throw;
        }
    }

    /// <summary>
    /// Roll back a partial Plain → Encrypted transition without managed-heap
    /// snapshots. Decrypts every DB that crossed the cipher boundary (in
    /// reverse install order) via the same chunked decrypt-in-place primitive
    /// used by LeaveEncryptedAsync. Clears the manifest (which may have been
    /// partially written if the failure was post-encrypt) and drops the
    /// installed key. After this the disk is back to Plain.
    /// </summary>
    private async Task RollBackEnterEncryptedAsync(
        IReadOnlyList<string> encryptedSoFar,
        CancellationToken cancellationToken)
    {
        // Decrypt under the still-registered globalKey first — the worker's
        // decryptDatabaseInPlace requires hasGlobalKey() and snapshots from
        // it, so we must not clear the key until every DB is back to plain.
        for (var i = encryptedSoFar.Count - 1; i >= 0; i--)
        {
            await _encryptedBridge.DecryptDatabaseInPlaceAsync(encryptedSoFar[i], cancellationToken);
        }

        // Clear any manifest bytes that landed before the failure (the
        // primitive is a no-op when nothing was written).
        await _encryptedBridge.ClearDiskManifestAsync(cancellationToken);

        // Finally drop the key + reset in-memory state.
        await _encryptedBridge.ClearEncryptionKeyAsync(cancellationToken);
        _isUnlocked = false;
        _expectedCredentialId = null;
        _bridge.SetDiskLocked(false);
    }

    public async Task LeaveEncryptedAsync(CancellationToken cancellationToken = default)
    {
        var current = await GetStateAsync(cancellationToken);
        if (!current.Encrypted || !current.Unlocked)
        {
            throw new InvalidOperationException(
                "LeaveEncryptedAsync requires Encrypted + Unlocked — call UnlockAsync first.");
        }

        // Phase 1: walk every encrypted DB, decrypt-in-place under the
        // active globalKey. DecryptDatabaseInPlaceAsync uses globalKey;
        // the worker closes each DB during the conversion.
        var databases = await _bridge.ListDatabasesAsync(cancellationToken);
        foreach (var db in databases)
        {
            await _encryptedBridge.DecryptDatabaseInPlaceAsync(db, cancellationToken);
        }

        // Phase 2: zero the manifest + drop globalKey. After this,
        // GetStateAsync returns Plain and the next boot proceeds without
        // an unlock prompt. Caller's responsibility to revoke the passkey
        // credential at the WebAuthn layer separately.
        await _encryptedBridge.ClearDiskManifestAsync(cancellationToken);
        await _encryptedBridge.ClearEncryptionKeyAsync(cancellationToken);
        _isUnlocked = false;
        _expectedCredentialId = null;
        _bridge.SetDiskLocked(false);
        ReportDbState(DbInitState.READY);
    }

    public async Task ResetDiskAsync(CancellationToken cancellationToken = default)
    {
        // Scorched-earth boundary: wipe the pool + drop the PRF cache.
        // PrfService.ClearKeys cascades through KeyExpired →
        // AuthenticationModel.OnSessionExpired → PublicKey=null →
        // PrfAuthenticationStateProvider → AuthorizeView re-evaluates.
        // Calling it here means a single Session.ResetDiskAsync() leaves
        // the whole encryption stack consistent — the auth flow doesn't
        // need to also be told "reset happened" by the orchestrating page.
        await WipePoolAsync(cancellationToken);
        _prfService.ClearKeys();
        ReportDbState(DbInitState.READY);
    }

    /// <summary>
    /// Drop globalKey, wipe every DB file from the pool, clear in-memory
    /// state. Does NOT clear the PRF cache — used by the guided-import flow
    /// which needs the PRF seed for the envelope's ECIES K_wrap unwrap
    /// after re-entering encrypted mode under the imported credential.
    /// </summary>
    private async Task WipePoolAsync(CancellationToken cancellationToken)
    {
        // Why delete the DB files (not just the manifest): EnterEncryptedAsync
        // walks every plain DB and runs encrypt-in-place. A pool that still
        // contains pre-Reset ciphertext slots fails the worker's plain-source
        // shape check ("not a multiple of 4096"); the user lands in a stuck
        // state where Reset → re-Encrypt is impossible without manual OPFS
        // surgery. Wiping the files makes Reset → re-Encrypt → Import the
        // canonical recipient flow for the asymmetric envelope.
        await _encryptedBridge.ClearEncryptionKeyAsync(cancellationToken);

        // Delete every DB file in the pool. The SAHPool delete path
        // explicitly zeros the reserved manifest sector before returning a
        // handle to the available pool, so a later plain import cannot inherit
        // a stale PFAM header. Ordering: globalKey is already dropped (above),
        // so the worker's deleteDatabase path runs without an active key —
        // pure file unlink, no AEAD verification. Snapshot the list before
        // looping because DeleteDatabaseAsync mutates the pool.
        var existing = await _bridge.ListDatabasesAsync(cancellationToken);
        foreach (var name in existing)
        {
            await _bridge.DeleteDatabaseAsync(name, cancellationToken);
        }

        _isUnlocked = false;
        _expectedCredentialId = null;
        _bridge.SetDiskLocked(false);
    }

    // ---------------------------------------------------------------------
    // Whole-disk export — disk-as-unit asymmetric (v3) envelope. Wraps a
    // fresh K_wrap via ECIES (X25519 ECDH + HKDF + AES-256-GCM) and rekeys
    // every page under K_wrap. Backup vs share is just "own pubkey vs peer
    // pubkey" at the call site — same code path. Streams the envelope to
    // a browser download Blob; C# never holds the envelope as a managed
    // byte[]. Mixed plain + encrypted DBs is not a representable disk
    // state, so the public surface only exposes whole-pool envelopes.
    // ---------------------------------------------------------------------

    /// <summary>
    /// Streaming asymmetric disk export — assembles the v3 envelope as a
    /// virtual-concat Blob on the main thread and triggers the browser
    /// download directly. C# never sees a managed <c>byte[]</c> of the
    /// envelope; worker chunks the per-DB rekey, bridge composes the Blob,
    /// anchor click fires the save.
    /// </summary>
    public async Task ExportDiskToPubkeyAndDownloadAsync(
        string filename,
        string recipientX25519PublicKeyBase64,
        string recipientCredentialId,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(filename))
        {
            throw new ArgumentException(
                "filename must be a non-empty string.", nameof(filename));
        }
        if (string.IsNullOrWhiteSpace(recipientX25519PublicKeyBase64))
        {
            throw new ArgumentException(
                "recipientX25519PublicKeyBase64 must be a non-empty Base64 X25519 pubkey.",
                nameof(recipientX25519PublicKeyBase64));
        }
        if (string.IsNullOrWhiteSpace(recipientCredentialId))
        {
            throw new ArgumentException(
                "recipientCredentialId must be a non-empty Base64 WebAuthn credentialId.",
                nameof(recipientCredentialId));
        }

        // Decode + length-check the recipient pubkey before doing any
        // worker round-trip — mirrors ExportDiskToPubkeyAsync's preflight.
        byte[] recipientPubBytes;
        try
        {
            recipientPubBytes = Convert.FromBase64String(recipientX25519PublicKeyBase64);
        }
        catch (FormatException ex)
        {
            throw new ArgumentException(
                "recipientX25519PublicKeyBase64 is not valid Base64.",
                nameof(recipientX25519PublicKeyBase64), ex);
        }
        if (recipientPubBytes.Length != 32)
        {
            throw new ArgumentException(
                $"recipientX25519PublicKeyBase64 must decode to 32 bytes; got {recipientPubBytes.Length}.",
                nameof(recipientX25519PublicKeyBase64));
        }

        var current = await GetStateAsync(cancellationToken);
        if (!current.Encrypted || !current.Unlocked)
        {
            throw new InvalidOperationException(
                "ExportDiskToPubkeyAndDownloadAsync requires Encrypted + Unlocked — call UnlockAsync first.");
        }

        // Same K_wrap generation + ECIES wrap as the byte[] path. The
        // wrap key is held in C# only long enough to ship to the worker;
        // the worker side wipes its copy after the chunked rekey loop
        // finishes.
        var wrapKeyMem = await _cryptoProvider.GenerateContentKeyAsync();
        var wrapKey = wrapKeyMem.ToArray();
        try
        {
            var wrappedResult = await _cryptoProvider.EncryptAsymmetricFromBytesAsync(
                wrapKey, recipientX25519PublicKeyBase64);
            if (!wrappedResult.Success || wrappedResult.Value is null)
            {
                throw new InvalidOperationException(
                    $"ExportDiskToPubkeyAndDownloadAsync: ECIES wrap of K_wrap failed " +
                    $"({wrappedResult.ErrorCode}).");
            }
            var wrapped = wrappedResult.Value;

            var metadata = new
            {
                version = 3,
                aadVersion = "v1",
                prfSaltBase64 = Convert.ToBase64String(_prfService.HashedSaltBytes),
                ephemeralPublicKey = wrapped.EphemeralPublicKey,
                wrappedContentKeyCiphertext = wrapped.Ciphertext,
                wrappedContentKeyNonce = wrapped.Nonce,
                credentialIdHint = recipientCredentialId,
            };
            var metadataJson = System.Text.Json.JsonSerializer.Serialize(metadata);

            var ok = await SqliteWasmWorkerBridge.ExportDiskToDownloadAsync(
                filename, metadataJson, new ArraySegment<byte>(wrapKey));
            if (!ok)
            {
                throw new InvalidOperationException(
                    "ExportDiskToPubkeyAndDownloadAsync: bridge reported failure.");
            }
        }
        finally
        {
            CryptographicOperations.ZeroMemory(wrapKey);
            if (MemoryMarshal.TryGetArray(wrapKeyMem, out var wrapKeySegment)
                && wrapKeySegment.Array is not null)
            {
                CryptographicOperations.ZeroMemory(wrapKeySegment.AsSpan());
            }
        }
    }

    /// <summary>
    /// Monotonic JS-side BlobSession id allocator. Independent of the
    /// worker bridge's request-id counter; only needs to be unique within
    /// the JS-side <c>blobSessions</c> Map for the duration of one
    /// streaming import.
    /// </summary>
    private int _nextSessionId;

    /// <summary>
    /// Streaming guided-import variant: the envelope arrives as a Stream
    /// (typically <c>IBrowserFile.OpenReadStream</c>) and is shipped to
    /// the JS-side BlobSession one ArrayPool chunk at a time. C# managed
    /// heap peak is one chunk (~1 MB); the JS Blob parts list is the
    /// browser's responsibility (Safari disk-backs above ~50 MB).
    ///
    /// Same security contract as <see cref="ImportDiskGuidedAsync"/>:
    /// state must be Plain or Encrypted+Locked; envelope's CredentialIdHint
    /// must match <paramref name="credentialId"/>; vfsKey must come from
    /// the WebAuthn ceremony pinned to that credential. Throws otherwise.
    /// Token-equivalent: session id is C#-issued, JS holds it only between
    /// Open and Discard.
    /// </summary>
    public async Task<DiskImportResult> ImportDiskGuidedFromStreamAsync(
        Stream envelopeStream,
        long envelopeSize,
        ReadOnlyMemory<byte> vfsKey,
        string credentialId,
        CancellationToken cancellationToken = default)
    {
        if (envelopeSize <= 0)
        {
            throw new ArgumentException(
                $"envelopeSize must be positive, got {envelopeSize}", nameof(envelopeSize));
        }
        if (string.IsNullOrWhiteSpace(credentialId))
        {
            throw new ArgumentException(
                "credentialId must be a non-empty Base64 WebAuthn credentialId.",
                nameof(credentialId));
        }
        if (vfsKey.Length != 32)
        {
            throw new ArgumentException(
                $"vfsKey must be exactly 32 bytes; got {vfsKey.Length}.", nameof(vfsKey));
        }

        var current = await GetStateAsync(cancellationToken);
        if (current.Encrypted && current.Unlocked)
        {
            throw new InvalidOperationException(
                "ImportDiskGuidedFromStreamAsync rejected: disk is Encrypted+Unlocked. " +
                "Lock or Reset first; guided import rebinds the disk to the import's " +
                "credential and is only allowed from Plain or Locked.");
        }

        var sessionId = Interlocked.Increment(ref _nextSessionId);
        SqliteWasmWorkerBridge.BlobSessionOpen(sessionId);

        byte[]? wrapKey = null;
        try
        {
            // Stream the picked file into the JS-side BlobSession one
            // 1 MB chunk at a time. While the first chunk(s) arrive,
            // keep a local copy of the first 4 KB — that's the envelope
            // header that PeekEnvelopeHeader parses for the credential-id
            // check and the ECIES wrap fields.
            const int chunkSize = 1 << 20;
            using var headerCopy = new MemoryStream(4096);
            var buf = ArrayPool<byte>.Shared.Rent(chunkSize);
            try
            {
                long totalRead = 0;
                while (totalRead < envelopeSize)
                {
                    var read = await envelopeStream.ReadAsync(
                        buf.AsMemory(0, chunkSize), cancellationToken);
                    if (read <= 0)
                    {
                        throw new InvalidOperationException(
                            $"ImportDiskGuidedFromStreamAsync: stream ended at {totalRead} " +
                            $"of {envelopeSize} bytes; envelope is truncated.");
                    }
                    if (headerCopy.Length < 4096)
                    {
                        var keep = (int)Math.Min(read, 4096 - headerCopy.Length);
                        headerCopy.Write(buf, 0, keep);
                    }
                    totalRead += read;
                    bool isLast = totalRead == envelopeSize;
                    SqliteWasmWorkerBridge.BlobSessionAppend(
                        sessionId, new Span<byte>(buf, 0, read), isLast);
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buf, clearArray: true);
            }

            // Peek the envelope header from the local 4 KB copy — small
            // enough that one MessagePackReader pass on a managed array
            // is cheap, and large enough to cover the positional fields
            // before Files (PrfSalt + 4 strings).
            var header = PeekEnvelopeHeader(headerCopy.ToArray());
            if (header.Version != 3)
            {
                throw new InvalidOperationException(
                    $"ImportDiskGuidedFromStreamAsync: unsupported envelope Version={header.Version} (expected 3).");
            }
            if (string.IsNullOrEmpty(header.CredentialIdHint))
            {
                throw new InvalidOperationException(
                    "ImportDiskGuidedFromStreamAsync: envelope is missing CredentialIdHint.");
            }
            if (!string.Equals(header.CredentialIdHint, credentialId, StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    "ImportDiskGuidedFromStreamAsync: envelope's CredentialIdHint does not " +
                    "match the supplied credentialId.");
            }

            var wrapped = new AsymmetricEncryptedData(
                header.EphemeralPublicKey,
                header.WrappedContentKeyCiphertext,
                header.WrappedContentKeyNonce);
            var unwrapResult = await _prfService.DecryptAsymmetricToBytesAsync(wrapped);
            if (!unwrapResult.Success || unwrapResult.Value is null)
            {
                throw new InvalidOperationException(
                    $"ImportDiskGuidedFromStreamAsync: ECIES unwrap of K_wrap failed " +
                    $"({unwrapResult.ErrorCode}). The envelope may be sealed for a different " +
                    $"recipient pubkey than the one this passkey derives.");
            }
            wrapKey = unwrapResult.Value;
            if (wrapKey.Length != 32)
            {
                throw new InvalidOperationException(
                    $"ImportDiskGuidedFromStreamAsync: unwrapped K_wrap must be 32 bytes; " +
                    $"got {wrapKey.Length}.");
            }

            // Pass 1 — preflight. Worker AEAD-verifies slot 0 of every
            // file via blob.stream(). Read-only; no pool mutation.
            var preflight = await SqliteWasmWorkerBridge.ImportDiskStreamPreflightFromSessionAsync(
                sessionId, new ArraySegment<byte>(wrapKey));
            if (preflight != (int)DiskImportResult.OK)
            {
                return (DiskImportResult)preflight;
            }

            // Wipe pool + EnterEncrypted under the import's credential.
            await WipePoolAsync(cancellationToken);
            await EnterEncryptedAsync(vfsKey, credentialId, cancellationToken);

            // Pass 2 — commit. Worker rebuilds a fresh Blob from the same
            // session's parts (still live), re-streams via blob.stream(),
            // decrypts under K_wrap and re-encrypts under the freshly-
            // installed globalKey.
            var commitResult = await SqliteWasmWorkerBridge.ImportDiskStreamCommitFromSessionAsync(
                sessionId, new ArraySegment<byte>(wrapKey));
            if (commitResult != (int)DiskImportResult.OK)
            {
                return (DiskImportResult)commitResult;
            }

            ReportDbState(DbInitState.READY);
            return DiskImportResult.OK;
        }
        finally
        {
            if (wrapKey is not null)
            {
                CryptographicOperations.ZeroMemory(wrapKey);
            }
            // Idempotent on every exit — success, AEAD failure, exception.
            // The JS-side parts list is dropped so the browser can GC the
            // underlying Blob storage.
            SqliteWasmWorkerBridge.BlobSessionDiscard(sessionId);
        }
    }

    /// <summary>
    /// Streaming single-DB plain import — the right primitive for "I have
    /// one big .db file and want it on this disk". State-aware: writes
    /// plain pages on a Plain disk, rekeys-on-write to encrypted slots on
    /// Encrypted+Unlocked, refuses on Encrypted+Locked (caller should
    /// Unlock first; the .eds guided import is the rebind-to-new-credential
    /// path).
    ///
    /// C# managed-heap peak: one ArrayPool chunk (~1 MB) regardless of file
    /// size. The picked file's bytes are streamed into the JS-side
    /// BlobSession; the worker reads them via <c>blob.stream()</c> and
    /// writes a temp SAH slot via writeFileSlice + atomicReplaceFile.
    /// </summary>
    public async Task ImportDatabaseFromStreamAsync(
        string databaseName,
        Stream stream,
        long size,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(databaseName))
        {
            throw new ArgumentException(
                "databaseName must be non-empty.", nameof(databaseName));
        }
        if (size <= 0)
        {
            throw new ArgumentException(
                $"size must be positive, got {size}.", nameof(size));
        }
        var current = await GetStateAsync(cancellationToken);
        if (current.Encrypted && !current.Unlocked)
        {
            throw new InvalidOperationException(
                "ImportDatabaseFromStreamAsync rejected: disk is Encrypted+Locked. " +
                "Unlock first, or use the .eds guided import to rebind the disk to " +
                "a different credential.");
        }

        var sessionId = Interlocked.Increment(ref _nextSessionId);
        SqliteWasmWorkerBridge.BlobSessionOpen(sessionId);
        try
        {
            const int chunkSize = 1 << 20;
            var buf = ArrayPool<byte>.Shared.Rent(chunkSize);
            try
            {
                long totalRead = 0;
                while (totalRead < size)
                {
                    var read = await stream.ReadAsync(
                        buf.AsMemory(0, chunkSize), cancellationToken);
                    if (read <= 0)
                    {
                        throw new InvalidOperationException(
                            $"ImportDatabaseFromStreamAsync: stream ended at {totalRead} " +
                            $"of {size} bytes; source is truncated.");
                    }
                    totalRead += read;
                    bool isLast = totalRead == size;
                    SqliteWasmWorkerBridge.BlobSessionAppend(
                        sessionId, new Span<byte>(buf, 0, read), isLast);
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buf, clearArray: true);
            }

            var result = await SqliteWasmWorkerBridge.ImportDatabaseFromSessionAsync(
                sessionId, databaseName);
            if (result != (int)DiskImportResult.OK)
            {
                throw new InvalidOperationException(
                    $"ImportDatabaseFromStreamAsync: worker returned result={result}.");
            }
        }
        finally
        {
            SqliteWasmWorkerBridge.BlobSessionDiscard(sessionId);
        }
    }

    /// <summary>
    /// Envelope-header peek result — every field the import flow needs
    /// before it can decide to wipe + re-encrypt. <c>Files</c> is
    /// intentionally absent: the streaming worker reads it directly off
    /// the JS-side Blob.
    /// </summary>
    private readonly struct EnvelopeHeader
    {
        public int Version { get; init; }
        public string AadVersion { get; init; }
        public string EphemeralPublicKey { get; init; }
        public string WrappedContentKeyCiphertext { get; init; }
        public string WrappedContentKeyNonce { get; init; }
        public string CredentialIdHint { get; init; }
    }

    /// <summary>
    /// Forward-parse just the envelope's positional header without copying
    /// the <c>Files</c> bulk into managed memory. Uses
    /// <see cref="MessagePackReader"/>'s streaming primitives so the
    /// entries are read in-place from <paramref name="envelope"/>; only
    /// the small string fields are allocated. The 32-byte PrfSalt is
    /// consumed and discarded — the receiver uses local PrfService config;
    /// envelope salt is forward-compat for cross-app import.
    /// </summary>
    private static EnvelopeHeader PeekEnvelopeHeader(byte[] envelope)
    {
        var reader = new MessagePackReader(envelope);
        var arrLen = reader.ReadArrayHeader();
        if (arrLen != 8)
        {
            throw new InvalidOperationException(
                $"PeekEnvelopeHeader: expected envelope array(8), got array({arrLen}).");
        }
        var version = reader.ReadInt32();
        var aadVersion = reader.ReadString()
            ?? throw new InvalidOperationException("PeekEnvelopeHeader: AadVersion is null.");
        var prfSaltSeq = reader.ReadBytes()
            ?? throw new InvalidOperationException("PeekEnvelopeHeader: PrfSalt is missing.");
        if (prfSaltSeq.Length != 32)
        {
            throw new InvalidOperationException(
                $"PeekEnvelopeHeader: PrfSalt must be 32 bytes, got {prfSaltSeq.Length}.");
        }
        var ephPub = reader.ReadString()
            ?? throw new InvalidOperationException("PeekEnvelopeHeader: EphemeralPublicKey is null.");
        var wrapCt = reader.ReadString()
            ?? throw new InvalidOperationException("PeekEnvelopeHeader: WrappedContentKeyCiphertext is null.");
        var wrapNonce = reader.ReadString()
            ?? throw new InvalidOperationException("PeekEnvelopeHeader: WrappedContentKeyNonce is null.");
        var credIdHint = reader.ReadString()
            ?? throw new InvalidOperationException("PeekEnvelopeHeader: CredentialIdHint is null.");
        // Files array tail is intentionally not consumed — the streaming
        // worker reads it directly off the JS-side Blob.
        return new EnvelopeHeader
        {
            Version = version,
            AadVersion = aadVersion,
            EphemeralPublicKey = ephPub,
            WrappedContentKeyCiphertext = wrapCt,
            WrappedContentKeyNonce = wrapNonce,
            CredentialIdHint = credIdHint,
        };
    }

    /// <inheritdoc />
    public async Task<DiskImportResult> ImportAllDatabasesAsync(
        byte[] zipBytes,
        CancellationToken cancellationToken = default)
    {
        if (zipBytes is null || zipBytes.Length == 0)
        {
            throw new ArgumentException(
                "ImportAllDatabasesAsync: zipBytes must be a non-empty ZIP archive.",
                nameof(zipBytes));
        }

        var current = await GetStateAsync(cancellationToken);
        var entries = await ReadPlainSqliteZipEntriesAsync(
            zipBytes,
            requireVfsPageShape: current.Encrypted && current.Unlocked,
            cancellationToken);
        if (entries is null)
        {
            return DiskImportResult.WRONG_KEY;
        }

        // Branch on disk state. Plain delegates straight to the base bridge;
        // Locked breaks encryption (recovery path); Unlocked re-encrypts on
        // write (preserves passkey binding).
        if (!current.Encrypted)
        {
            return await _bridge.ImportAllDatabasesAsync(zipBytes, cancellationToken);
        }

        if (!current.Unlocked)
        {
            // Encrypted+Locked → break encryption. Drop globalKey + delete
            // every DB + clear manifest, then unpack the ZIP via the base
            // bridge (which now writes plain pages because no key is
            // registered). State ends Plain; user can re-encrypt under any
            // new passkey via EnterEncryptedAsync.
            await WipePoolAsync(cancellationToken);
            await _encryptedBridge.ClearDiskManifestAsync(cancellationToken);
            var result = await _bridge.ImportAllDatabasesAsync(zipBytes, cancellationToken);
            if (result == DiskImportResult.OK)
            {
                ReportDbState(DbInitState.READY);
            }
            return result;
        }

        // Encrypted+Unlocked → preserve encryption. Wipe DBs only (keep
        // manifest + globalKey + passkey binding); re-encrypt each ZIP
        // entry on write under the registered globalKey via the worker's
        // importDbPlain handler. State stays Encrypted+Unlocked.
        var existing = await _bridge.ListDatabasesAsync(cancellationToken);
        foreach (var name in existing)
        {
            await _bridge.DeleteDatabaseAsync(name, cancellationToken);
        }

        foreach (var entry in entries)
        {
            var result = await _encryptedBridge.ImportPlainDatabaseAsync(
                entry.Name, entry.Bytes, cancellationToken);
            if (result != DiskImportResult.OK)
            {
                return result;
            }
        }
        return DiskImportResult.OK;
    }

    private static async Task<List<PlainZipEntry>?> ReadPlainSqliteZipEntriesAsync(
        byte[] zipBytes,
        bool requireVfsPageShape,
        CancellationToken cancellationToken)
    {
        var result = new List<PlainZipEntry>();
        var seenNames = new HashSet<string>(StringComparer.Ordinal);
        using var preflightMs = new MemoryStream(zipBytes, writable: false);
        using var preflightZip = new ZipArchive(preflightMs, ZipArchiveMode.Read);
        foreach (var entry in preflightZip.Entries)
        {
            if (string.IsNullOrEmpty(entry.Name))
            {
                continue;
            }

            if (!IsBareDatabaseName(entry.Name)
                || !string.Equals(entry.FullName, entry.Name, StringComparison.Ordinal)
                || !seenNames.Add(entry.Name)
                || entry.Length > int.MaxValue
                || entry.Length < SqliteWasmWorkerBridge.SqliteHeaderMagic.Length)
            {
                return null;
            }

            using var entryMs = new MemoryStream(checked((int)entry.Length));
            await using (var entryStream = entry.Open())
            {
                await entryStream.CopyToAsync(entryMs, cancellationToken);
            }

            var bytes = entryMs.ToArray();
            if (bytes.Length < SqliteWasmWorkerBridge.SqliteHeaderMagic.Length
                || !bytes.AsSpan(0, SqliteWasmWorkerBridge.SqliteHeaderMagic.Length)
                    .SequenceEqual(SqliteWasmWorkerBridge.SqliteHeaderMagic))
            {
                return null;
            }

            if (requireVfsPageShape
                && (bytes.Length == 0 || bytes.Length % PlainVfsSlotSize != 0))
            {
                return null;
            }

            result.Add(new PlainZipEntry(entry.Name, bytes));
        }

        return result.Count == 0 ? null : result;
    }

    public ValueTask<string?> ReadEnvelopeCredentialIdHintAsync(
        ReadOnlyMemory<byte> envelope,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        // Forward-parse the positional header via PeekEnvelopeHeader rather
        // than full MessagePackSerializer.Deserialize<EncryptedDiskEnvelope> —
        // works equally well on a small (≥ ~256 byte) prefix as on a full
        // envelope, which is what the streaming import path needs to peek
        // CredentialIdHint without buffering the whole .eds file in C#.
        EnvelopeHeader header;
        try
        {
            header = PeekEnvelopeHeader(envelope.ToArray());
        }
        catch (Exception)
        {
            // Truncated / malformed prefix → no hint available.
            return ValueTask.FromResult<string?>(null);
        }
        if (header.Version != 3 || string.IsNullOrEmpty(header.CredentialIdHint))
        {
            return ValueTask.FromResult<string?>(null);
        }
        return ValueTask.FromResult<string?>(header.CredentialIdHint);
    }

    private static bool IsBareDatabaseName(string name)
        => !string.IsNullOrWhiteSpace(name)
            && name != "."
            && name != ".."
            && name.IndexOf('/') < 0
            && name.IndexOf('\\') < 0
            && string.Equals(name, Path.GetFileName(name), StringComparison.Ordinal);

}

internal sealed record PlainZipEntry(string Name, byte[] Bytes);

[MessagePackObject(AllowPrivate = true)]
internal sealed class DiskManifestBody
{
    [Key(0)]
    public string? CredentialId { get; set; }

    [Key(1)]
    public string? PublicKeyFingerprint { get; set; }
}
/// <summary>
/// Pool-wide disk-manifest state surfaced by
/// <see cref="EncryptedSqliteWasmDatabaseService.ReadManifestAsync"/>.
/// File-internal helper — the only consumer is the encrypted-disk service.
/// </summary>
internal enum ManifestState
{
    /// <summary>No DB carries the manifest magic — disk is Plain.</summary>
    ABSENT,
    /// <summary>Every DB carries an identical, structurally-valid manifest.</summary>
    PRESENT,
    /// <summary>DBs carry different manifest bytes — corruption / partial import.</summary>
    MISMATCH,
    /// <summary>HMAC verification failed (verifyMac=true was passed).</summary>
    TAMPERED,
    /// <summary>Magic present but layout decode failed — corruption.</summary>
    MALFORMED,
}
