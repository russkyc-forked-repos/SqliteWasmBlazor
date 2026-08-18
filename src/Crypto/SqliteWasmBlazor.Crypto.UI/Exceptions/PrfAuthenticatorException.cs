using SqliteWasmBlazor.Crypto.Abstractions.Models;

namespace SqliteWasmBlazor.Crypto.UI.Services;

/// <summary>
/// Which PrfAuthenticator operation failed. Lets the per-command error
/// formatter on <see cref="Components.Authentication.AuthenticationModel"/>
/// pick the right localized resx prefix (<c>Error_Register_{code}</c> /
/// <c>Error_Authenticate_{code}</c>) without re-deriving from the stack.
/// </summary>
public enum PrfAuthenticatorOperation
{
    Register,
    Authenticate,
}

/// <summary>
/// Carries a structured <see cref="PrfErrorCode"/> failure out of
/// <see cref="PrfAuthenticator"/> so the panel formatters can localize
/// the user-visible message via per-code resx keys
/// (<c>Error_{Operation}_{Code}</c>) instead of embedding a hardcoded
/// English string from <see cref="PrfErrorMessages.GetMessage"/>.
///
/// <para>
/// The base <see cref="Exception.Message"/> stays in English with code
/// + canonical message — useful for logs / devtools, never user-facing.
/// User-visible text is always resolved through the
/// <see cref="Microsoft.Extensions.Localization.IStringLocalizer"/>
/// in the consuming model.
/// </para>
///
/// <para>
/// An abandoned register ceremony IS routed through this exception, as
/// <see cref="PrfErrorCode.CEREMONY_INCOMPLETE"/>. It must not be an
/// <see cref="OperationCanceledException"/>: RxBlazorV2 treats those as
/// switch-cancellation and discards them before the command's error formatter
/// runs, so the user is left with no feedback at all after the browser has
/// already reported a dismissed prompt or a rejected PIN. Abandoned
/// authenticate ceremonies still return a non-completed
/// <see cref="PrfAuthenticationOutcome"/> rather than throwing, because the
/// panel drives its cancel-to-register fallback off that.
/// </para>
/// </summary>
public sealed class PrfAuthenticatorException : Exception
{
    public PrfErrorCode Code { get; }
    public PrfAuthenticatorOperation Operation { get; }

    /// <summary>
    /// Browser-supplied diagnostic for the failed ceremony ("Name: message"), or
    /// <c>null</c> when the failure did not come from one. Appended to the localized
    /// message so the user sees the same reason the browser just showed them.
    /// </summary>
    public string? Detail { get; }

    public PrfAuthenticatorException(
        PrfAuthenticatorOperation operation,
        PrfErrorCode code,
        string? detail = null)
        : base($"PrfAuthenticator.{operation} failed: {code} — {PrfErrorMessages.GetMessage(code)}"
               + (detail is null ? string.Empty : $" [{detail}]"))
    {
        Operation = operation;
        Code = code;
        Detail = detail;
    }
}
