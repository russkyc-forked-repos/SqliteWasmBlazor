// Diagnostic channel for tab-discard / reload investigations.
//
// The worker POSTs phase events as JSON to `<baseHref>debug-log.php`. The
// PHP sink (samples/SqliteWasmBlazor.Demo/PublishExtras/debug-log.php,
// copied into the publish output by scripts/deep-clean-publish.sh) appends
// each line to `debug-log.txt` next to it. If the PHP file isn't deployed
// the request 404s and the catch below swallows the error — the operation
// is unaffected.
//
// Fire-and-forget by design: a network blip must never break the op the
// log is trying to observe. `keepalive: true` so a tab-discard in the
// middle of a chunked op doesn't kill in-flight log lines.

let debugLogUrl: string | null = null;
let opCounter = 0;

/**
 * Resolve the sink URL from the SPA's base href. Called once during worker
 * initialisation; before this fires {@link debugLog} is a no-op.
 *
 * Worker runs in the same origin as the page, so `self.location.origin`
 * is reliable. Resolving relative to <base href> makes sub-path deploys
 * land on the right PHP without configuration.
 */
export function setDebugLogBase(baseHref: string): void {
    try {
        const origin = self.location?.origin ?? '';
        debugLogUrl = `${origin}${baseHref}debug-log.php`;
    } catch {
        debugLogUrl = null;
    }
}

/**
 * Monotonic op id for tying together the multiple phase events of one
 * worker operation. Format `op{N}` keeps log lines greppable by op.
 */
export function nextOpId(): string {
    opCounter = (opCounter + 1) | 0;
    return `op${opCounter}`;
}

/**
 * POST one phase event. Body is a JSON object with fixed fields
 * `{t, op, phase, heap, ...extra}` so the sink can grep by op or phase.
 *
 * `t` = milliseconds since worker start (or epoch fallback).
 * `heap` = MB of JS heap used (Chrome only; null elsewhere — iOS Safari
 *          intentionally does not expose `performance.memory`).
 * `extra` = caller-supplied fields (chunk index, slot count, sizes, …).
 */
export function debugLog(op: string, phase: string, extra?: Record<string, unknown>): void {
    if (!debugLogUrl) {
        return;
    }
    const t = (typeof performance !== 'undefined') ? Math.round(performance.now()) : Date.now();
    const heap = readHeap();
    const payload = JSON.stringify({ t, op, phase, heap, ...extra });
    try {
        fetch(debugLogUrl, {
            method: 'POST',
            headers: { 'Content-Type': 'text/plain' },
            body: payload,
            keepalive: true,
        }).catch(() => { /* swallow — see file header */ });
    } catch {
        // swallow
    }
}

function readHeap(): number | null {
    const perf = (typeof performance !== 'undefined')
        ? performance as unknown as { memory?: { usedJSHeapSize?: number } }
        : undefined;
    const used = perf?.memory?.usedJSHeapSize;
    return typeof used === 'number' ? Math.round(used / (1024 * 1024)) : null;
}
