// worker-bridge.ts
// Bridge between C# JSImport and Web Worker.
// Exposes a single async initializeBridge(baseHref, assetRoot) entry point;
// C# awaits its returned Promise so worker creation errors surface on the .NET side.

import { downloadStagedExport } from '@sqlitewasmblazor/worker-common';

/**
 * IMemoryView interface from dotnet runtime — view over managed Span/ArraySegment.
 */
interface IMemoryView {
    slice(): Uint8Array;
    slice(start: number): Uint8Array;
    slice(start: number, end: number): Uint8Array;
}

let worker: Worker | null = null;

/**
 * Create the Web Worker and wire up message handling.
 * Called from C# via JSImport after JSHost.ImportAsync has loaded this module.
 * Returns a resolved Promise once the Worker is constructed — the worker's own
 * "ready" signal arrives asynchronously via postMessage → OnWorkerReady.
 */
export async function initializeBridge(baseHref: string, assetRoot: string): Promise<void> {
    worker = new Worker(
        `${baseHref}${assetRoot}sqlite-wasm-worker.js`,
        { type: 'module' }
    );

    worker.postMessage({ type: 'init', baseHref, assetRoot });

    worker.onmessage = async (event) => {
        if (event.data.type === 'ready') {
            console.log('[Worker Bridge] Worker ready');
            try {
                const exports = await (globalThis as any).getDotnetRuntime(0).getAssemblyExports("SqliteWasmBlazor.dll");
                exports.SqliteWasmBlazor.SqliteWasmWorkerBridge.OnWorkerReady();
            } catch (error) {
                console.error('[Worker Bridge] Failed to call OnWorkerReady:', error);
            }
            return;
        }

        if (event.data.type === 'error') {
            console.error('[Worker Bridge] Worker error:', event.data.error);
            try {
                const exports = await (globalThis as any).getDotnetRuntime(0).getAssemblyExports("SqliteWasmBlazor.dll");
                exports.SqliteWasmBlazor.SqliteWasmWorkerBridge.OnWorkerError(event.data.error || 'Unknown worker error');
            } catch (error) {
                console.error('[Worker Bridge] Failed to call OnWorkerError:', error);
            }
            return;
        }

        if (event.data.id !== undefined) {
            try {
                const exports = await (globalThis as any).getDotnetRuntime(0).getAssemblyExports("SqliteWasmBlazor.dll");

                if (event.data.rawBinary && event.data.data instanceof Uint8Array) {
                    exports.SqliteWasmBlazor.SqliteWasmWorkerBridge.OnWorkerResponseRawBinary(
                        event.data.id,
                        event.data.data
                    );
                } else if (event.data.binary && event.data.data instanceof Uint8Array) {
                    exports.SqliteWasmBlazor.SqliteWasmWorkerBridge.OnWorkerResponseBinary(
                        event.data.id,
                        event.data.data
                    );
                } else {
                    const messageJson = JSON.stringify(event.data);
                    exports.SqliteWasmBlazor.SqliteWasmWorkerBridge.OnWorkerResponse(messageJson);
                }
            } catch (error) {
                console.error('[Worker Bridge] Failed to call C# callback:', error);
                try {
                    const exports = await (globalThis as any).getDotnetRuntime(0).getAssemblyExports("SqliteWasmBlazor.dll");
                    const errorJson = JSON.stringify({
                        id: event.data.id,
                        data: { success: false, error: `Bridge callback failed: ${error}` }
                    });
                    exports.SqliteWasmBlazor.SqliteWasmWorkerBridge.OnWorkerResponse(errorJson);
                } catch {
                    // Last resort — runtime unavailable, can't notify C#.
                }
            }
        }
    };

    worker.onerror = (error) => {
        console.error('[Worker Bridge] Worker error event:', error);
    };
}

/** Send a JSON request to the worker (C# → worker). */
export function sendToWorker(messageJson: string): void {
    if (!worker) {
        throw new Error('Worker not initialized');
    }

    const message = JSON.parse(messageJson);
    worker.postMessage(message);
}

// ----------------------------------------------------------------------
// BlobSession — chunked C# → JS Blob construction.
//
// Lets the C# side stream a large body (a picked file, a delta payload,
// anything) into the JS layer one chunk at a time, without ever
// materialising the whole body in WASM linear memory. The bridge holds
// each appended chunk as a Blob part; consumer bridges added in later
// phases (encrypted-disk import, plain DB import, etc.) compose
// `new Blob(parts)` to feed `blob.stream()` to the worker — the Blob is
// a virtual concatenation, so per-part disk-backing on Safari keeps JS
// heap bounded regardless of total session size.
//
// Identity is C#-owned: the caller allocates `sessionId` from its
// existing request-id counter and is solely responsible for the Open →
// Append × N → consumer-call → Discard sequence. JS holds nothing across
// session boundaries.
// ----------------------------------------------------------------------

/**
 * Open buffers, keyed by C#-issued session id. Lifetime: caller-owned;
 * dropped explicitly via {@link blobSessionDiscard} (or implicitly by a
 * consumer bridge that consumes-and-clears in one step).
 */
const blobSessions = new Map<number, BlobPart[]>();

/** Allocate a fresh session's part list. Throws on duplicate id. */
export function blobSessionOpen(sessionId: number): void {
    if (blobSessions.has(sessionId)) {
        throw new Error(`blobSessionOpen: sessionId ${sessionId} is already open`);
    }
    blobSessions.set(sessionId, []);
}

/**
 * Append <paramref name="chunkView"/> to the session's part list.
 *
 * The chunk is detached into a fresh `Uint8Array` via `.slice()` and wrapped
 * in a `Blob` part — at part sizes above ~50 MB Safari swaps the parts out
 * of JS heap automatically. <paramref name="isLast"/> is informational
 * (forwarded to the debug log when enabled); the consumer bridge knows
 * when its own end-of-stream condition is met.
 */
export function blobSessionAppend(
    sessionId: number,
    chunkView: IMemoryView,
    isLast: boolean,
): void {
    const parts = blobSessions.get(sessionId);
    if (!parts) {
        throw new Error(`blobSessionAppend: unknown sessionId ${sessionId}`);
    }
    parts.push(new Blob([chunkView.slice() as Uint8Array<ArrayBuffer>]));
    void isLast; // reserved for future flow-control / observability
}

/** Idempotent drop. Safe to call from a finally-block. */
export function blobSessionDiscard(sessionId: number): void {
    blobSessions.delete(sessionId);
}

/**
 * Internal helper for same-module consumer bridges (encrypted-disk import,
 * plain DB import). Returns the live part list — caller must NOT mutate it.
 * Building `new Blob(parts)` is O(parts.length) and creates a virtual view
 * the consumer can `.stream()` (multiple times if needed for two-pass
 * preflight+commit flows).
 */
export function blobSessionParts(sessionId: number): BlobPart[] {
    const parts = blobSessions.get(sessionId);
    if (!parts) {
        throw new Error(`blobSessionParts: unknown sessionId ${sessionId}`);
    }
    return parts;
}

// Called from C# to send binary data to worker (import operations)
// Optional header: small binary (nonce+key) sent alongside large payload without copying payload.
export function sendBinaryToWorker(memoryView: IMemoryView, metadataJson: string, headerView?: IMemoryView): void {
    if (!worker) {
        throw new Error('Worker not initialized');
    }

    const data = memoryView.slice();
    const metadata = JSON.parse(metadataJson);

    if (headerView) {
        const header = headerView.slice();
        // Transfer both buffers — header carries CryptoHeader private-key
        // material; transferring synchronously detaches the JS-side copy on
        // the main thread so no readable reference survives postMessage.
        worker.postMessage(
            { ...metadata, binaryHeader: header.buffer, binaryPayload: data.buffer },
            [data.buffer, header.buffer]
        );
    } else {
        worker.postMessage(
            { ...metadata, binaryPayload: data.buffer },
            [data.buffer]
        );
    }
}

export const logger = {
    setLogLevel(level: number): void {
        if (!worker) {
            console.warn('[Worker Bridge] Worker not initialized, cannot set log level');
            return;
        }
        worker.postMessage({
            type: 'setLogLevel',
            level: level
        });
    }
};

// Staged export downloads live in worker-common so both bridges share one
// staging-directory name and one filename → content-type mapping; see
// staged-download.ts for why the content type decides whether iOS Safari
// keeps the filename we ask for.
export { downloadStagedExport };

(globalThis as any).sqliteWasmWorker = {
    initializeBridge,
    sendToWorker,
    sendBinaryToWorker,
    blobSessionOpen,
    blobSessionAppend,
    blobSessionDiscard,
    downloadStagedExport
};

(globalThis as any).__sqliteWasmLogger = logger;
