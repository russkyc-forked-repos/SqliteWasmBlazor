namespace SqliteWasmBlazor.Crypto.Configuration;

/// <summary>
/// Configuration options for PRF-based encryption.
/// </summary>
public sealed class PrfOptions
{
    /// <summary>
    /// Configuration section name in appsettings.json.
    /// </summary>
    public const string SectionName = "SqliteWasmBlazorCrypto";

    /// <summary>
    /// Display name of the relying party shown during WebAuthn registration.
    /// </summary>
    public string RpName { get; set; } = "SqliteWasmBlazorCrypto App";

    /// <summary>
    /// Relying party ID (domain). If null, uses window.location.hostname.
    /// </summary>
    public string? RpId { get; set; }

    /// <summary>
    /// Timeout in milliseconds for WebAuthn operations.
    /// </summary>
    public int TimeoutMs { get; set; } = 60000;

    /// <summary>
    /// Type of authenticator offered during registration.
    /// Platform = built-in biometrics only (Touch ID, Windows Hello, Face ID)
    /// CrossPlatform = USB/NFC security keys only
    /// Any = browser offers both, so the user can pick a security key from the
    /// create ceremony. Anything other than <see cref="AuthenticatorAttachment.ANY"/>
    /// pins <c>authenticatorSelection.authenticatorAttachment</c> and removes the
    /// other branch from the browser's picker entirely.
    /// </summary>
    public AuthenticatorAttachment AuthenticatorAttachment { get; set; } = AuthenticatorAttachment.ANY;

    /// <summary>
    /// App-wide PRF salt used for every ceremony under this <see cref="Services.PrfService"/>
    /// instance. Domain separation for derived keys belongs in the HKDF <c>context</c>
    /// argument to <see cref="Services.IPrfService.DeriveDomainKeyAsync"/>, not in a per-call
    /// salt — one app has one salt.
    /// </summary>
    public string Salt { get; set; } = "my-encryption-keypair";
}

/// <summary>
/// Authenticator attachment type.
/// </summary>
public enum AuthenticatorAttachment
{
    /// <summary>
    /// Platform authenticator only (Touch ID, Windows Hello, Face ID).
    /// Registration goes straight to the built-in sensor; the browser offers no
    /// security-key branch, so a user who dismisses that prompt has nowhere else to go.
    /// </summary>
    PLATFORM,

    /// <summary>
    /// Cross-platform authenticator (USB/NFC security keys).
    /// Many modern hardware keys (YubiKey 5+, SoloKeys v2) support the PRF extension.
    /// </summary>
    CROSS_PLATFORM,

    /// <summary>
    /// Allow both platform and cross-platform authenticators. Default.
    /// <c>authenticatorSelection.authenticatorAttachment</c> is omitted, so the browser
    /// shows every available destination — built-in sensor, passkey manager, USB/NFC
    /// security key, phone-as-authenticator. A key without PRF still fails cleanly with
    /// <c>PrfErrorCode.PRF_NOT_SUPPORTED</c> after the ceremony.
    /// </summary>
    ANY
}
