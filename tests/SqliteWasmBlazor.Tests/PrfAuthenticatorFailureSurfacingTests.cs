using R3;
using SqliteWasmBlazor.Crypto.Abstractions.Models;
using SqliteWasmBlazor.Crypto.Configuration;
using SqliteWasmBlazor.Crypto.Services;
using SqliteWasmBlazor.Crypto.UI.Services;

namespace SqliteWasmBlazor.Tests;

/// <summary>
/// Pins the seam contract that decides whether a failed WebAuthn ceremony is
/// visible to the user at all.
///
/// <para>
/// RxBlazorV2 treats <see cref="OperationCanceledException"/> as
/// switch-cancellation and discards it before an <c>[ObservableCommand]</c>'s
/// error formatter runs. Routing an abandoned ceremony through one therefore
/// produces no status message, no snackbar, nothing — while the browser has
/// already told the user their PIN was rejected. These tests fail if that
/// mapping ever comes back.
/// </para>
/// </summary>
public class PrfAuthenticatorFailureSurfacingTests
{
    private const string CeremonyDetail = "NotAllowedError: The operation either timed out or was not allowed.";

    [Fact]
    public async Task RegisterCancelled_ThrowsStructuredError_NotOperationCanceled()
    {
        var authenticator = CreateAuthenticator(new FakePrfService
        {
            RegisterResult = new PrfResult<PrfCredential>
            {
                Success = false,
                Cancelled = true,
                ErrorDetail = CeremonyDetail,
            },
        });

        var ex = await Assert.ThrowsAsync<PrfAuthenticatorException>(
            async () => await authenticator.RegisterAsync(displayName: null, excludeCredentialIds: []));

        Assert.Equal(PrfAuthenticatorOperation.Register, ex.Operation);
        Assert.Equal(PrfErrorCode.CEREMONY_INCOMPLETE, ex.Code);
        Assert.Equal(CeremonyDetail, ex.Detail);
        Assert.IsNotType<OperationCanceledException>(ex);
    }

    [Fact]
    public async Task RegisterFailed_CarriesBrowserDetail()
    {
        var authenticator = CreateAuthenticator(new FakePrfService
        {
            RegisterResult = PrfResult<PrfCredential>.Fail(
                PrfErrorCode.CREDENTIAL_ALREADY_REGISTERED,
                CeremonyDetail),
        });

        var ex = await Assert.ThrowsAsync<PrfAuthenticatorException>(
            async () => await authenticator.RegisterAsync(displayName: null, excludeCredentialIds: []));

        Assert.Equal(PrfErrorCode.CREDENTIAL_ALREADY_REGISTERED, ex.Code);
        Assert.Equal(CeremonyDetail, ex.Detail);
    }

    [Fact]
    public async Task AuthenticateCancelled_ReturnsIncompleteOutcomeWithDetail()
    {
        var authenticator = CreateAuthenticator(new FakePrfService
        {
            DeriveDiscoverableResult = new PrfResult<(string, string)>
            {
                Success = false,
                Cancelled = true,
                ErrorDetail = CeremonyDetail,
            },
        });

        // Non-completed rather than thrown: the panel's cancel-to-register
        // fallback keys off the return value.
        var outcome = await authenticator.AuthenticateAsync(credentialIdHint: null);

        Assert.False(outcome.Completed);
        Assert.Null(outcome.Result);
        Assert.Equal(CeremonyDetail, outcome.Detail);
    }

    private static IPrfAuthenticator CreateAuthenticator(FakePrfService prf) => new PrfAuthenticator(prf);

    /// <summary>
    /// Only the three members the register / authenticate paths touch are
    /// implemented; the rest throw so an unexpected call is a loud failure
    /// rather than a silent default.
    /// </summary>
    private sealed class FakePrfService : IPrfService
    {
        public PrfResult<PrfCredential> RegisterResult { get; init; } =
            PrfResult<PrfCredential>.Fail(PrfErrorCode.UNKNOWN);

        public PrfResult<string> DeriveResult { get; init; } =
            PrfResult<string>.Fail(PrfErrorCode.UNKNOWN);

        public PrfResult<(string CredentialId, string PublicKey)> DeriveDiscoverableResult { get; init; } =
            PrfResult<(string, string)>.Fail(PrfErrorCode.UNKNOWN);

        public ValueTask<PrfResult<PrfCredential>> RegisterAsync(
            string? displayName,
            IReadOnlyList<string> excludeCredentialIds) => ValueTask.FromResult(RegisterResult);

        public ValueTask<PrfResult<string>> DeriveKeysAsync(string credentialId) =>
            ValueTask.FromResult(DeriveResult);

        public ValueTask<PrfResult<(string CredentialId, string PublicKey)>> DeriveKeysDiscoverableAsync() =>
            ValueTask.FromResult(DeriveDiscoverableResult);

        public KeyCacheStrategy CacheStrategy => throw new NotSupportedException();
        public string Salt => throw new NotSupportedException();
        public byte[] HashedSaltBytes => throw new NotSupportedException();
        public Observable<string> KeyExpired => throw new NotSupportedException();
        public ValueTask<bool> IsPrfSupportedAsync() => throw new NotSupportedException();
        public string? GetCachedPublicKey() => throw new NotSupportedException();
        public bool HasCachedKeys() => throw new NotSupportedException();
        public string? GetEd25519PublicKey() => throw new NotSupportedException();
        public void ClearKeys() => throw new NotSupportedException();
        public ValueTask<PrfResult<string>> DeriveDomainKeyAsync(string domainId, string context) =>
            throw new NotSupportedException();
        public ValueTask<PrfResult<byte[]>> DecryptAsymmetricToBytesAsync(AsymmetricEncryptedData data) =>
            throw new NotSupportedException();
    }
}
