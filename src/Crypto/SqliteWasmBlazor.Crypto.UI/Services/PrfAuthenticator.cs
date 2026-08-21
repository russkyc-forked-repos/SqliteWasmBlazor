using SqliteWasmBlazor.Crypto.Abstractions.Models;
using SqliteWasmBlazor.Crypto.Services;

namespace SqliteWasmBlazor.Crypto.UI.Services;

/// <summary>
/// Production <see cref="IPrfAuthenticator"/> implementation that bridges the
/// host-supplied seam consumed by <see cref="Components.Authentication.AuthenticationPanel"/>
/// onto the base-plane <see cref="IPrfService"/>. No new TS surface — the underlying
/// WebAuthn-PRF pipeline (<c>crypto-bridge.ts</c>, <c>navigator.credentials.create/get</c>,
/// X25519 derivation, <c>SecureKeyCache</c>) is already production-grade and
/// exercised end-to-end by the R2 / R3 Playwright suites.
///
/// <para>
/// <b>Register-then-derive contract.</b> WebAuthn create + assert are two
/// ceremonies — unavoidably so for security keys, since CTAP2 only emits
/// hmac-secret output from getAssertion, never from makeCredential.
/// <see cref="RegisterAsync"/> runs both back-to-back so it can satisfy the seam
/// contract of returning the X25519 pubkey alongside the credential id. The user
/// sees two prompts, and on a security key that means two PIN entries.
/// </para>
///
/// <para>
/// <b>Failure surfacing.</b> Per the seam: an abandoned register ceremony throws
/// <see cref="PrfAuthenticatorException"/> as
/// <see cref="PrfErrorCode.CEREMONY_INCOMPLETE"/>; an abandoned authenticate
/// ceremony returns a non-completed <see cref="PrfAuthenticationOutcome"/>;
/// transport / WebAuthn errors throw <see cref="PrfAuthenticatorException"/> with
/// the structured <see cref="PrfErrorCode"/> intact so the panel formatters can
/// localize via per-code resx keys (<c>Error_Register_{code}</c> /
/// <c>Error_Authenticate_{code}</c>) rather than embedding a hardcoded
/// English string from <see cref="PrfErrorMessages.GetMessage"/>. Every throw
/// carries the browser's own diagnostic in
/// <see cref="PrfAuthenticatorException.Detail"/>.
/// </para>
///
/// <para>
/// <b>Never OperationCanceledException.</b> RxBlazorV2 discards those as
/// switch-cancellation before the command error formatter runs, so routing a
/// dismissed prompt or a rejected authenticator PIN through one leaves the user
/// with no feedback whatsoever. Only <c>cancellationToken</c>-driven
/// teardown may raise it, which is exactly the case that should stay silent.
/// </para>
/// </summary>
internal sealed class PrfAuthenticator : IPrfAuthenticator
{
    private readonly IPrfService _prf;

    public PrfAuthenticator(IPrfService prf)
    {
        _prf = prf;
    }

    public ValueTask<bool> CheckPrfSupportAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return _prf.IsPrfSupportedAsync();
    }

    public async ValueTask<PrfRegistrationResult> RegisterAsync(
        string? displayName,
        IReadOnlyList<string> excludeCredentialIds,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(excludeCredentialIds);

        cancellationToken.ThrowIfCancellationRequested();

        var registerResult = await _prf.RegisterAsync(displayName, excludeCredentialIds);
        if (registerResult.Cancelled)
        {
            throw new PrfAuthenticatorException(
                PrfAuthenticatorOperation.Register,
                PrfErrorCode.CEREMONY_INCOMPLETE,
                registerResult.ErrorDetail);
        }
        if (!registerResult.Success || registerResult.Value is null)
        {
            throw new PrfAuthenticatorException(
                PrfAuthenticatorOperation.Register,
                registerResult.ErrorCode ?? PrfErrorCode.REGISTRATION_FAILED,
                registerResult.ErrorDetail);
        }

        var credential = registerResult.Value;
        cancellationToken.ThrowIfCancellationRequested();

        // Discoverable, not targeted. Registration forces residentKey='required', so
        // the credential just minted is always discoverable — and security keys serve
        // the discoverable ceremony far more reliably than an allowCredentials-scoped
        // one, which can leave the prompt hanging until it times out as
        // NotAllowedError even though the very same credential answers a discoverable
        // request immediately. The returned credential id is checked below, so
        // dropping allowCredentials costs no binding accuracy.
        var deriveResult = await _prf.DeriveKeysDiscoverableAsync();
        if (deriveResult.Cancelled)
        {
            throw new PrfAuthenticatorException(
                PrfAuthenticatorOperation.Register,
                PrfErrorCode.CEREMONY_INCOMPLETE,
                deriveResult.ErrorDetail);
        }
        if (!deriveResult.Success)
        {
            throw new PrfAuthenticatorException(
                PrfAuthenticatorOperation.Register,
                deriveResult.ErrorCode ?? PrfErrorCode.KEY_DERIVATION_FAILED,
                deriveResult.ErrorDetail);
        }

        var (assertedCredentialId, publicKey) = deriveResult.Value;

        // The picker can offer other passkeys, so confirm the one that answered is
        // the one just created. Returning a different credential's key would bind the
        // disk to a passkey the caller never registered.
        if (!string.Equals(assertedCredentialId, credential.RawId, StringComparison.Ordinal))
        {
            throw new PrfAuthenticatorException(
                PrfAuthenticatorOperation.Register,
                PrfErrorCode.CREDENTIAL_MISMATCH);
        }

        return new PrfRegistrationResult(credential.RawId, publicKey);
    }

    public async ValueTask<PrfAuthenticationOutcome> AuthenticateAsync(
        string? credentialIdHint,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (!string.IsNullOrWhiteSpace(credentialIdHint))
        {
            var byHint = await _prf.DeriveKeysAsync(credentialIdHint);
            if (byHint.Cancelled)
            {
                return PrfAuthenticationOutcome.Incomplete(byHint.ErrorDetail);
            }
            if (!byHint.Success || byHint.Value is null)
            {
                throw new PrfAuthenticatorException(
                    PrfAuthenticatorOperation.Authenticate,
                    byHint.ErrorCode ?? PrfErrorCode.KEY_DERIVATION_FAILED,
                    byHint.ErrorDetail);
            }
            return new PrfAuthenticationOutcome(
                new PrfAuthenticationResult(credentialIdHint, byHint.Value),
                Detail: null);
        }

        // Caller asked for discoverable explicitly — go straight to the
        // platform picker. DeriveKeysWithHintAsync would re-read the
        // persisted hint and run a targeted ceremony first, which is
        // wrong here: this branch is the plain-disk sign-in, where there
        // is no disk binding to target. Stacking a hinted ceremony in
        // front of the picker is exactly the redundant-prompts chain the
        // UI is meant to avoid — one click, one ceremony.
        var byDiscoverable = await _prf.DeriveKeysDiscoverableAsync();
        if (byDiscoverable.Cancelled)
        {
            return PrfAuthenticationOutcome.Incomplete(byDiscoverable.ErrorDetail);
        }
        if (!byDiscoverable.Success)
        {
            throw new PrfAuthenticatorException(
                PrfAuthenticatorOperation.Authenticate,
                byDiscoverable.ErrorCode ?? PrfErrorCode.KEY_DERIVATION_FAILED,
                byDiscoverable.ErrorDetail);
        }
        // PrfResult<T>.Value for an unconstrained T resolves to T itself (not
        // Nullable<T>) when T is a value type — the value-tuple components
        // here are populated under the IPrfService.Success contract.
        var (credentialId, publicKey) = byDiscoverable.Value;
        return new PrfAuthenticationOutcome(
            new PrfAuthenticationResult(credentialId, publicKey),
            Detail: null);
    }
}
