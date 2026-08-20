using System.Security.Cryptography;
using Microsoft.EntityFrameworkCore;
using SqliteWasmBlazor.Crypto.Abstractions;
using SqliteWasmBlazor.Crypto.Services;

namespace SqliteWasmBlazor.TestApp.TestInfrastructure.VfsEncryption;

/// <summary>
/// End-to-end streaming cross-key guided import — the <c>.eds</c> rebind
/// path (<see cref="IEncryptedSqliteWasmDatabaseService.ImportPoolGuidedFromStreamAsync"/>)
/// that powers "share / back-up to a passkey, then import on the recipient
/// side". Closes the browser-E2E coverage gap left when commit c239c23
/// deleted the old <c>byte[]</c> <c>DiskImportGuidedCrossKeyTest</c> without
/// a streaming replacement.
///
/// <para>
/// The production export's download side (<c>&lt;a download&gt;</c>) is
/// unreachable from in-page test code, so the envelope is produced by the
/// REAL export assembly via the <c>ExportPoolToPubkeyBytesAsync</c> seam
/// (same ECIES wrap + worker slot rekey + bridge composition as the
/// download path, only the final anchor click is swapped for a byte
/// return). The bytes are then streamed back into the production guided
/// import unchanged — exercising the genuine ECIES unwrap, the
/// AEAD-preflight-then-wipe ordering, and the rebind to a fresh credential.
/// </para>
///
/// <para>
/// Recipient identity is planted under the PRF convention key id
/// (<see cref="PrfKeyConventions.GetJsKeyId"/> over the live
/// <see cref="IPrfService.Salt"/>) so the import-side unwrap —
/// <c>_prfService.DecryptAsymmetricToBytesAsync</c>, routed through the
/// provider's keyId cache — recovers <c>K_wrap</c> exactly as a real passkey
/// ceremony would. The synthetic seed replaces the WebAuthn ceremony the
/// same way <c>SyntheticPrfSeedRoundTripTest</c> does.
/// </para>
///
/// <para>
/// <b>Cross-key, cross-credential.</b> The source disk is encrypted under
/// <c>vfsKeyA</c> bound to <c>sourceCredentialId</c>; the import rebinds it
/// to a DIFFERENT <c>vfsKeyB</c> bound to <c>recipientCredentialId</c>. Rows
/// surviving the round-trip proves the data flowed through the envelope
/// (re-encrypted under <c>K_wrap</c>, then under <c>vfsKeyB</c> on commit)
/// rather than through any residue of the wiped source pool.
/// </para>
/// </summary>
internal sealed class DiskImportGuidedCrossKeyStreamingTest
{
    private const int RowCount = 12;

    private readonly IDbContextFactory<PrfVfsTestContext> _factory;
    private readonly ISqliteWasmDatabaseService _databaseService;
    private readonly ICryptoProvider _cryptoProvider;
    private readonly IPrfService _prfService;
    private readonly IEncryptedSqliteWasmDatabaseService _session;

    public string Name => "DiskImport_GuidedCrossKey_StreamingRoundTrip";

    public DiskImportGuidedCrossKeyStreamingTest(
        IDbContextFactory<PrfVfsTestContext> factory,
        ISqliteWasmDatabaseService databaseService,
        ICryptoProvider cryptoProvider,
        IPrfService prfService,
        IEncryptedSqliteWasmDatabaseService session)
    {
        _factory = factory;
        _databaseService = databaseService;
        _cryptoProvider = cryptoProvider;
        _prfService = prfService;
        _session = session;
    }

    public async ValueTask<string?> RunAsync()
    {
        var dbName = PrfVfsTestContext.DatabaseName;

        // The bytes-return export seam lives on the concrete service (it is
        // deliberately off the public interface). InternalsVisibleTo makes
        // the cast legal from the TestApp assembly.
        if (_session is not EncryptedSqliteWasmDatabaseService concrete)
        {
            return $"FAIL: session is {_session.GetType().Name}, expected " +
                   "EncryptedSqliteWasmDatabaseService for the bytes-export seam";
        }

        // Distinct cross-key / cross-credential material. Identifiable byte
        // patterns so any leak is obvious in a hex dump.
        var vfsKeyA = MakeKey(0xA0);
        var vfsKeyB = MakeKey(0x10);
        const string sourceCredentialId = "c291cmNlLWNyZWQ=";        // "source-cred"
        const string recipientCredentialId = "cmVjaXBpZW50LWNyZWQ="; // "recipient-cred"

        // Recipient X25519 keypair, planted under the PRF convention id so
        // the production unwrap finds the matching private key.
        var jsKeyId = PrfKeyConventions.GetJsKeyId(_prfService.Salt);
        var seed = new byte[32];
        for (var i = 0; i < 32; i++) { seed[i] = (byte)(0x42 + i); }

        try
        {
            // Clean slate. ResetPoolAsync also clears the PRF cache, so this
            // MUST run before the keypair is planted (and never again until
            // the finally block).
            await CleanupAsync(dbName, jsKeyId);

            var storeResult = await _cryptoProvider.StoreKeysAsync(jsKeyId, seed, ttlMs: null);
            if (!storeResult.Success || storeResult.Value is null)
            {
                return $"FAIL: StoreKeysAsync returned {storeResult.ErrorCode}";
            }
            var recipientPubBase64 = storeResult.Value.X25519PublicKey;

            // ---- Source disk: Encrypted+Unlocked under vfsKeyA, populate ----
            await _session.EnterEncryptedAsync(vfsKeyA, sourceCredentialId);

            await using (var ctx = await _factory.CreateDbContextAsync())
            {
                await ctx.Database.EnsureCreatedAsync();
                for (var i = 0; i < RowCount; i++)
                {
                    ctx.Items.Add(new VfsTestItem
                    {
                        Marker = $"crosskey-{i}",
                        Payload = $"payload-{i}-{Guid.NewGuid():N}",
                    });
                }
                await ctx.SaveChangesAsync();
            }


            // ---- Export the real v3 envelope to the recipient pubkey -------
            byte[] envelope;
            try
            {
                envelope = await concrete.ExportPoolToPubkeyBytesAsync(
                    recipientPubBase64, recipientCredentialId);
            }
            catch (Exception ex)
            {
                return $"FAIL[Export]: {ex.GetType().Name}: {ex.Message}";
            }
            if (envelope.Length == 0)
            {
                return "FAIL[Export]: bytes seam returned an empty envelope";
            }

            // ---- Lock (Encrypted+Locked is a valid guided-import source) ---
            // Lock drops the globalKey but preserves the PRF cache, so the
            // planted keypair survives into the import's unwrap.
            await _session.LockAsync();

            // ---- Guided import: rebind to vfsKeyB / recipientCredentialId --
            DiskImportResult result;
            try
            {
                using var stream = new MemoryStream(envelope, writable: false);
                result = await _session.ImportPoolGuidedFromStreamAsync(
                    stream, envelope.Length, vfsKeyB, recipientCredentialId);
            }
            catch (Exception ex)
            {
                return $"FAIL[Import]: {ex.GetType().Name}: {ex.Message}";
            }
            if (result != DiskImportResult.OK)
            {
                return $"FAIL[Import]: guided import returned {result}";
            }

            // ---- Verify state: Encrypted+Unlocked, bound to recipient ------
            var state = await _session.GetStateAsync();
            if (!state.Encrypted || !state.Unlocked)
            {
                return $"FAIL[State]: expected Encrypted+Unlocked after import, " +
                       $"got Encrypted={state.Encrypted} Unlocked={state.Unlocked}";
            }
            if (!string.Equals(state.Hint, recipientCredentialId, StringComparison.Ordinal))
            {
                return $"FAIL[State]: disk rebound to credential '{state.Hint}', " +
                       $"expected '{recipientCredentialId}'";
            }

            // ---- Verify the DB landed under its name ----------------------
            var names = await _databaseService.ListDatabasesAsync();
            if (!names.Contains(dbName))
            {
                return $"FAIL[Verify]: pool missing '{dbName}' after guided import " +
                       $"(got [{string.Join(", ", names)}])";
            }

            // ---- Verify rows survived the cross-key round-trip -------------
            // Read through the EF context: the worker decrypts each slot under
            // the freshly-installed vfsKeyB globalKey, so a correct row set
            // proves the data flowed envelope → K_wrap → vfsKeyB intact.
            List<VfsTestItem> rows;
            await using (var ctx = await _factory.CreateDbContextAsync())
            {
                rows = await ctx.Items.OrderBy(x => x.Id).ToListAsync();
            }
            if (rows.Count != RowCount)
            {
                return $"FAIL[Verify]: expected {RowCount} rows after guided import, got {rows.Count}";
            }
            for (var i = 0; i < RowCount; i++)
            {
                if (rows[i].Marker != $"crosskey-{i}")
                {
                    return $"FAIL[Verify]: row {i} Marker mismatch (got '{rows[i].Marker}')";
                }
                if (!rows[i].Payload.StartsWith($"payload-{i}-", StringComparison.Ordinal))
                {
                    return $"FAIL[Verify]: row {i} Payload mismatch (got '{rows[i].Payload}')";
                }
            }

            return "OK";
        }
        finally
        {
            _cryptoProvider.RemoveCachedKey(jsKeyId);
            await CleanupAsync(dbName, jsKeyId);
            CryptographicOperations.ZeroMemory(vfsKeyA);
            CryptographicOperations.ZeroMemory(vfsKeyB);
            CryptographicOperations.ZeroMemory(seed);
        }
    }

    private static byte[] MakeKey(int start)
    {
        var k = new byte[32];
        for (var i = 0; i < 32; i++) { k[i] = (byte)(start + i); }
        return k;
    }

    private async Task CleanupAsync(string dbName, string jsKeyId)
    {
        try { await _session.ResetPoolAsync(); } catch { }
        try { await _databaseService.DeleteDatabaseAsync(dbName); } catch { }
        _cryptoProvider.RemoveCachedKey(jsKeyId);
    }
}
