namespace SqliteWasmBlazor.Crypto.UI.Services;

/// <summary>
/// Host-supplied seam carrying the WebAuthn-PRF authentication / key
/// derivation pipeline behind <see cref="Components.Authentication.AuthenticationPanel"/>.
/// The Crypto.UI library does not register a default implementation —
/// the consumer wires either a stub (test fixtures) or the production
/// PRF-backed implementation via <c>AddCryptoUIPrfAuthenticator</c>.
///
/// <para>
/// Implementations must be safe to call from a Blazor render context
/// (typically driving JS interop into the browser's WebAuthn API).
/// </para>
/// </summary>
public interface IPrfAuthenticator
{
    /// <summary>
    /// Probe whether the current platform / browser supports the WebAuthn
    /// PRF extension. Called once on panel ready to gate the rest of the
    /// UI surface.
    /// </summary>
    ValueTask<bool> CheckPrfSupportAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Create a new WebAuthn credential with PRF support and return the
    /// credential identifier plus the X25519 public key derived from the
    /// PRF output. <paramref name="displayName"/> is shown in the platform's
    /// credential UI.
    /// </summary>
    /// <param name="excludeCredentialIds">
    /// Credential ids the disk is already bound to. An authenticator holding one of
    /// them refuses the ceremony instead of minting a duplicate passkey that would
    /// derive a different PRF key and never unlock the disk. Empty means "no
    /// restriction" — the normal case for a plaintext disk with no passkey yet.
    /// </param>
    ValueTask<PrfRegistrationResult> RegisterAsync(
        string? displayName,
        IReadOnlyList<string> excludeCredentialIds,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Run a WebAuthn assertion against an existing credential and return
    /// the derived public key. Pass a non-null <paramref name="credentialIdHint"/>
    /// to target a specific credential; pass <c>null</c> to use the
    /// platform's discoverable-credential picker.
    /// A non-completed outcome means the prompt closed without an assertion.
    /// </summary>
    ValueTask<PrfAuthenticationOutcome> AuthenticateAsync(
        string? credentialIdHint,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Result of <see cref="IPrfAuthenticator.RegisterAsync"/>. The
/// <paramref name="CredentialId"/> is opaque to the panel and persisted by
/// the host so subsequent <see cref="IPrfAuthenticator.AuthenticateAsync"/>
/// calls can pass it back as a hint.
/// </summary>
public sealed record PrfRegistrationResult(
    string CredentialId,
    string PublicKeyBase64);

/// <summary>
/// Result of <see cref="IPrfAuthenticator.AuthenticateAsync"/>. Mirrors
/// <see cref="PrfRegistrationResult"/> — the credential id may differ from
/// the hint when the discoverable-credential picker chose a different one.
/// </summary>
public sealed record PrfAuthenticationResult(
    string CredentialId,
    string PublicKeyBase64);

/// <summary>
/// Outcome of <see cref="IPrfAuthenticator.AuthenticateAsync"/>. A null
/// <paramref name="Result"/> means the ceremony closed without producing an
/// assertion — the panel drives its cancel-to-register fallback off that, so it
/// is a return value rather than an exception.
/// </summary>
/// <param name="Result">The assertion, or <c>null</c> if the ceremony did not complete.</param>
/// <param name="Detail">
/// Browser-supplied diagnostic for a non-completed ceremony ("Name: message"), or
/// <c>null</c> when the browser gave none. A dismissed prompt, a timeout, and a
/// rejected authenticator PIN are all reported as <c>NotAllowedError</c>, so this
/// text is what tells the user which one happened.
/// </param>
public sealed record PrfAuthenticationOutcome(
    PrfAuthenticationResult? Result,
    string? Detail)
{
    /// <summary>Whether the ceremony produced an assertion.</summary>
    public bool Completed => Result is not null;

    /// <summary>Outcome for a ceremony that closed without an assertion.</summary>
    public static PrfAuthenticationOutcome Incomplete(string? detail) => new(null, detail);
}
