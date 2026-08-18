// Diagnostic extraction for WebAuthn ceremony failures.

/**
 * Browsers funnel most ceremony failures into a single `NotAllowedError` --
 * user dismissal, timeout, and a wrong or blocked authenticator PIN all land
 * on the same name -- so `error.name` alone cannot tell "user cancelled" from
 * "PIN blocked". The DOMException *message* is where the browser puts the
 * distinguishing text it just showed the user, so carry both across the bridge
 * instead of dropping them. Without this the app renders nothing after a failed
 * ceremony, contradicting the error the browser displayed a moment earlier.
 */
export function describeCeremonyError(error: unknown): string | undefined {
    if (error instanceof DOMException || error instanceof Error) {
        return error.message.length > 0 ? `${error.name}: ${error.message}` : error.name;
    }
    return undefined;
}
