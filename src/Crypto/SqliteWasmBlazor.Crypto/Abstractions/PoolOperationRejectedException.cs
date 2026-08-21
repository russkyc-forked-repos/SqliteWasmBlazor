// SqliteWasmBlazor - Minimal EF Core compatible provider
// MIT License

namespace SqliteWasmBlazor;

/// <summary>
/// Why an encrypted-pool operation was refused before it touched the disk.
/// Every value names a precondition on <see cref="EncryptedPoolState"/> —
/// the caller can either fix the state (Lock / Unlock / Reset) and retry,
/// or render the matching explanation. Carried by
/// <see cref="PoolOperationRejectedException"/> so UI layers can localize
/// the refusal instead of surfacing an internal message.
/// </summary>
public enum PoolOperationRejection
{
    /// <summary>
    /// <c>EnterEncryptedAsync</c> needs <see cref="EncryptedPoolState.Plain"/>
    /// — the pool already carries a passkey manifest.
    /// </summary>
    ENTER_NEEDS_PLAIN,

    /// <summary>
    /// <c>LeaveEncryptedAsync</c> needs Encrypted+Unlocked — without the
    /// global key there is nothing to decrypt the databases with.
    /// </summary>
    LEAVE_NEEDS_UNLOCK,

    /// <summary>
    /// A plain export (<c>.db</c> / <c>.dbs</c>) or an <c>.eds</c> envelope
    /// export was attempted on Encrypted+Locked. Unlock first; the worker
    /// needs the global key to turn slots back into plain pages.
    /// </summary>
    EXPORT_NEEDS_UNLOCK,

    /// <summary>
    /// A plain import (<c>.db</c> / <c>.dbs</c>) was attempted on
    /// Encrypted+Locked. Unlock first — the writes are rekeyed under the
    /// global key as they land.
    /// </summary>
    PLAIN_IMPORT_NEEDS_UNLOCK,

    /// <summary>
    /// The guided <c>.eds</c> import was attempted on Encrypted+Unlocked.
    /// It rebinds the pool to the envelope's credential, so the current
    /// session has to end first — Lock (keeps the data until the import
    /// commits) or Reset.
    /// </summary>
    GUIDED_IMPORT_NEEDS_LOCK,
}

/// <summary>
/// Thrown when an encrypted-pool operation's <see cref="EncryptedPoolState"/>
/// precondition does not hold. Nothing has been written when this surfaces —
/// the guards run before any disk mutation.
///
/// <para>
/// UI layers branch on <see cref="Reason"/> to render localized copy (and,
/// where the remedy is mechanical, to offer it as a button). The
/// <see cref="Exception.Message"/> is a developer-facing diagnostic; it
/// names the primitive and is not meant for end users.
/// </para>
/// </summary>
public sealed class PoolOperationRejectedException : InvalidOperationException
{
    /// <summary>
    /// Create a rejection carrying <paramref name="reason"/> and a
    /// developer-facing <paramref name="message"/>.
    /// </summary>
    /// <param name="reason">The precondition that did not hold.</param>
    /// <param name="message">Diagnostic text naming the primitive and the state it saw.</param>
    public PoolOperationRejectedException(PoolOperationRejection reason, string message)
        : base(message)
    {
        Reason = reason;
    }

    /// <summary>The precondition that did not hold.</summary>
    public PoolOperationRejection Reason { get; }
}
