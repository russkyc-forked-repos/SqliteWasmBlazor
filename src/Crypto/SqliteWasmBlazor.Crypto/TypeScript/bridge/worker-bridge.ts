// worker-bridge.ts
// Bridge between C# JSImport and Web Worker.
// Exposes a single async initializeBridge(baseHref, assetRoot) entry point;
// C# awaits its returned Promise so worker creation errors surface on the .NET side.

import { base64ToBytes } from '@sqlitewasmblazor/crypto-core';

import {
    packArrayHeader,
    packBinHeader,
    packStr,
    packUint,
} from './msgpack-stream';

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
 * JS-side stream handler registry — separate from the C# request-id space.
 * Streaming worker calls (currently only the encrypted-disk export) post a
 * sequence of `streamChunk` messages followed by `streamDone`, all keyed by
 * `streamId`. Until streamDone arrives the handler stays installed,
 * accumulating per-DB output as standalone Blobs so each one can be
 * dropped from JS heap and disk-backed by Safari independently.
 *
 * `streamId` is allocated from a negative-int counter so it never collides
 * with the C#-side `_nextRequestId` (which only increments positively).
 */
interface StreamHandler {
    onChunk(name: string, data: Uint8Array, isFirst: boolean, isLast: boolean): void;
    onDone(result?: number): void;
    onError(message: string): void;
}
const streamHandlers = new Map<number, StreamHandler>();
let nextStreamId = -1;

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

        // Streaming responses — keyed by `streamId`, dispatched JS-side to
        // a handler in `streamHandlers`. Worker emits a sequence of
        // streamChunk → ... → streamDone (or streamError) all under the
        // same streamId. C# never sees these messages directly.
        if (event.data.streamId !== undefined) {
            const handler = streamHandlers.get(event.data.streamId);
            if (!handler) {
                console.warn(
                    '[Worker Bridge] Stream message for unknown streamId',
                    event.data.streamId);
                return;
            }
            if (event.data.streamChunk === true) {
                const isFirst = event.data.isFirst === undefined ? true : !!event.data.isFirst;
                const isLast = event.data.isLast === undefined ? true : !!event.data.isLast;
                handler.onChunk(
                    event.data.name as string,
                    event.data.data as Uint8Array,
                    isFirst,
                    isLast,
                );
            } else if (event.data.streamDone === true) {
                handler.onDone(
                    typeof event.data.result === 'number' ? event.data.result : undefined);
            } else if (event.data.streamError === true) {
                handler.onError(
                    typeof event.data.error === 'string' ? event.data.error : 'unknown stream error');
            } else {
                console.warn('[Worker Bridge] Unknown stream message shape', event.data);
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

// ---------------------------------------------------------------------------
// Chunked encrypted-disk export — worker → main thread → Blob download.
//
// Worker emits per-DB ciphertext as a sequence of `streamChunk` messages
// keyed by negative streamId. Bridge aggregates per-DB chunks into
// per-file Blobs (browser disk-backs them on Safari), then composes the
// final v3 envelope as a virtual-concat Blob of:
//
//   <MessagePack header bytes (array(8) + 7 small fields)>
//   <per-file array headers + name + bin headers + file Blob parts>
//
// C# never sees a managed byte[] of the envelope. The download fires
// from the bridge via anchor click; C# awaits a boolean result.
// ---------------------------------------------------------------------------

/**
 * JSImport entry point — called by C# `ExportDiskToDownloadAsync`. Drives
 * the worker stream, composes the envelope Blob, triggers the download.
 * Returns `true` on completion.
 */
export function exportDiskToDownload(
    filename: string,
    metadataJson: string,
    kWrapView: IMemoryView,
): Promise<boolean> {
    return _assembleEnvelopeStreamed(metadataJson, kWrapView).then((blob) => {
        triggerEnvelopeDownload(filename, blob);
        return true;
    });
}

/**
 * Shared assembly path: drive the worker streaming export, accumulate each
 * rekeyed DB as a standalone Blob on the main thread, compose the
 * MessagePack envelope Blob via the positional encoder.
 */
function _assembleEnvelopeStreamed(
    metadataJson: string,
    kWrapView: IMemoryView,
): Promise<Blob> {
    if (!worker) {
        return Promise.reject(new Error('Worker not initialized'));
    }
    const meta = JSON.parse(metadataJson) as {
        version: number;
        aadVersion: string;
        prfSaltBase64: string;
        ephemeralPublicKey: string;
        wrappedContentKeyCiphertext: string;
        wrappedContentKeyNonce: string;
        credentialIdHint: string;
    };

    const streamId = nextStreamId--;
    const kWrap = kWrapView.slice();
    const fileParts: { name: string; size: number; blob: Blob }[] = [];
    // Worker streams each DB as N slot-batch chunks. Aggregate by name
    // until isLast, then compose a per-DB Blob from the chunk list. Map
    // preserves insertion order so file order in the envelope matches
    // worker's listDatabases() order.
    const pendingFiles = new Map<string, { chunks: Blob[]; totalSize: number }>();

    return new Promise((resolve, reject) => {
        streamHandlers.set(streamId, {
            onChunk(name, data, isFirst, isLast) {
                let pending = pendingFiles.get(name);
                if (isFirst) {
                    if (pending !== undefined) {
                        reject(new Error(
                            `exportDiskToDownload: chunk(isFirst=true) for ${name} ` +
                            `but ${pending.chunks.length} chunks already accumulated`));
                        return;
                    }
                    pending = { chunks: [], totalSize: 0 };
                    pendingFiles.set(name, pending);
                }
                if (pending === undefined) {
                    reject(new Error(
                        `exportDiskToDownload: chunk for ${name} without prior isFirst`));
                    return;
                }
                // Wrap each rekeyed chunk as its own Blob. Dropping the
                // Uint8Array reference (returning from this callback) lets
                // Safari hold the bytes in a disk-backed Blob instead of
                // pinning them in JS heap — that's the central memory win.
                // Cast: postMessage payload is Uint8Array<ArrayBufferLike> in TS 5.x,
                // but BlobPart requires the more specific ArrayBuffer. The runtime
                // buffer is always ArrayBuffer (worker constructs the chunk via
                // exportFileSlice → new Uint8Array(length)).
                pending.chunks.push(new Blob([data as Uint8Array<ArrayBuffer>]));
                pending.totalSize += data.length;
                if (isLast) {
                    fileParts.push({
                        name,
                        size: pending.totalSize,
                        blob: new Blob(pending.chunks),
                    });
                    pendingFiles.delete(name);
                }
            },
            onDone() {
                streamHandlers.delete(streamId);
                try {
                    resolve(composeEnvelopeBlob(meta, fileParts));
                } catch (e) {
                    reject(e instanceof Error ? e : new Error(String(e)));
                }
            },
            onError(message) {
                streamHandlers.delete(streamId);
                reject(new Error(message));
            },
        });

        // Transfer K_wrap into the worker — the buffer detaches from the
        // main side immediately, matching the existing sendBinaryToWorker
        // ownership semantics. `data: { type }` matches the legacy
        // WorkerRequest shape the worker's onmessage destructures.
        worker!.postMessage(
            {
                streamId,
                data: { type: 'exportDiskStream' },
                binaryPayload: kWrap.buffer,
            },
            [kWrap.buffer],
        );
    });
}

/**
 * Compose the v3 EncryptedDiskEnvelope wire shape as a Blob — small
 * positional header bytes + per-DB Blob parts. Wire layout matches the
 * MessagePack-CSharp [Key(N)] positional record decoded by
 * <c>ImportDiskAsync</c>:
 *
 *   [0] Version (uint)
 *   [1] AadVersion (str)
 *   [2] PrfSalt (bin, 32 bytes)
 *   [3] EphemeralPublicKey (str, Base64)
 *   [4] WrappedContentKeyCiphertext (str, Base64)
 *   [5] WrappedContentKeyNonce (str, Base64)
 *   [6] CredentialIdHint (str, Base64)
 *   [7] Files (array of [name(str), bytes(bin)])
 *
 * Returns a virtual-concatenation Blob — the per-DB segments stay
 * referenced as standalone Blob parts so the browser can disk-back them.
 */
function composeEnvelopeBlob(
    meta: {
        version: number;
        aadVersion: string;
        prfSaltBase64: string;
        ephemeralPublicKey: string;
        wrappedContentKeyCiphertext: string;
        wrappedContentKeyNonce: string;
        credentialIdHint: string;
    },
    fileParts: { name: string; size: number; blob: Blob }[],
): Blob {
    const decodedSalt = base64ToBytes(meta.prfSaltBase64);
    if (decodedSalt.length !== 32) {
        throw new Error(
            `composeEnvelopeBlob: prfSalt must decode to 32 bytes (got ${decodedSalt.length})`);
    }
    // Push the ArrayBuffer itself (always a valid BlobPart) so we don't
    // fight TS's `Uint8Array<ArrayBufferLike>` vs `Uint8Array<ArrayBuffer>`
    // distinction that BlobPart enforces in TS 5.x.
    const prfBuf = new ArrayBuffer(decodedSalt.length);
    new Uint8Array(prfBuf).set(decodedSalt);
    const parts: BlobPart[] = [];
    parts.push(packArrayHeader(8));
    parts.push(packUint(meta.version));
    parts.push(...packStr(meta.aadVersion));
    parts.push(packBinHeader(decodedSalt.length));
    parts.push(prfBuf);
    parts.push(...packStr(meta.ephemeralPublicKey));
    parts.push(...packStr(meta.wrappedContentKeyCiphertext));
    parts.push(...packStr(meta.wrappedContentKeyNonce));
    parts.push(...packStr(meta.credentialIdHint));
    parts.push(packArrayHeader(fileParts.length));
    for (const f of fileParts) {
        parts.push(packArrayHeader(2));
        parts.push(...packStr(f.name));
        parts.push(packBinHeader(f.size));
        parts.push(f.blob);
    }
    return new Blob(parts, { type: 'application/x-msgpack' });
}

// ---------------------------------------------------------------------------
// BlobSession — chunked C# → JS Blob primitive (Crypto-bundle copy).
//
// The Crypto-plane bridge bundle ships standalone; it doesn't import the
// Base-plane bundle at runtime. So the BlobSession primitive lives in
// both bundles, registered under the same `sqliteWasmWorker.blobSession*`
// names, and the C# JSImport reaches whichever bundle the consumer
// happens to have loaded. Behaviour is byte-identical to the Base-plane
// implementation; see SqliteWasmBlazor.csproj's bridge for the canonical
// commentary.
// ---------------------------------------------------------------------------

const blobSessions = new Map<number, BlobPart[]>();

export function blobSessionOpen(sessionId: number): void {
    if (blobSessions.has(sessionId)) {
        throw new Error(`blobSessionOpen: sessionId ${sessionId} is already open`);
    }
    blobSessions.set(sessionId, []);
}

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
    void isLast;
}

export function blobSessionDiscard(sessionId: number): void {
    blobSessions.delete(sessionId);
}

function blobSessionPartsRef(sessionId: number): BlobPart[] {
    const parts = blobSessions.get(sessionId);
    if (!parts) {
        throw new Error(`blobSessionPartsRef: no parts list for sessionId ${sessionId}`);
    }
    return parts;
}

// ---------------------------------------------------------------------------
// Chunked encrypted-disk import — C# → BlobSession → worker.
//
// The C# side has already streamed the picked file's bytes into the
// JS-side BlobSession via the Base-plane chunked-push primitive (one
// ArrayPool chunk at a time). This glue builds a virtual-concat Blob from
// those parts and hands it to the worker's import-streamed.ts handlers
// via the streamHandler protocol.
//
// Two-pass: same parts list is rebuilt into a fresh Blob for preflight,
// then again for commit (Blob.stream() is one-shot per Blob, so we mint
// a new view between passes). The parts stay live in `blobSessions`
// until C# calls BlobSessionDiscard in its finally-block.
// ---------------------------------------------------------------------------

/** JSImport entry — preflight: AEAD-verify slot 0 of every file under K_wrap. */
export function importDiskStreamPreflightFromSession(
    sessionId: number,
    kWrapView: IMemoryView,
): Promise<number> {
    return _sendImportDiskStreamSession(
        'importDiskStreamPreflight', sessionId, kWrapView);
}

/** JSImport entry — commit: re-stream the envelope, decrypt under K_wrap, re-encrypt under globalKey. */
export function importDiskStreamCommitFromSession(
    sessionId: number,
    kWrapView: IMemoryView,
): Promise<number> {
    return _sendImportDiskStreamSession(
        'importDiskStreamCommit', sessionId, kWrapView);
}

/**
 * JSImport entry — multi-DB plain export. Drives the worker's <c>.dbs</c>
 * envelope assembly (Plain: verbatim per file; Encrypted+Unlocked: decrypt
 * per file), accumulates the streamed envelope bytes into one Blob,
 * triggers anchor-click download. C# never sees the bytes.
 */
export function exportDatabasesToDownload(
    filename: string,
    dbNamesJson: string,
): Promise<boolean> {
    if (!worker) {
        return Promise.reject(new Error('Worker not initialized'));
    }
    const dbNames = JSON.parse(dbNamesJson) as string[];
    if (!Array.isArray(dbNames) || dbNames.length === 0) {
        return Promise.reject(new Error('exportDatabasesToDownload: dbNames must be non-empty array'));
    }
    const streamId = nextStreamId--;
    const chunks: Blob[] = [];
    return new Promise((resolve, reject) => {
        streamHandlers.set(streamId, {
            onChunk(_name, data) {
                chunks.push(new Blob([data as Uint8Array<ArrayBuffer>]));
            },
            onDone() {
                streamHandlers.delete(streamId);
                try {
                    triggerEnvelopeDownload(filename, new Blob(chunks));
                    resolve(true);
                } catch (e) {
                    reject(e instanceof Error ? e : new Error(String(e)));
                }
            },
            onError(message) {
                streamHandlers.delete(streamId);
                reject(new Error(message));
            },
        });
        worker!.postMessage({
            streamId,
            data: { type: 'exportDatabasesToSession', databases: dbNames },
        });
    });
}

/**
 * JSImport entry — multi-DB plain import. Composes the BlobSession parts
 * into a Blob, posts to the worker's <c>importDatabasesFromSession</c>
 * handler, which wipes the pool then writes each envelope file via the
 * chunked SAH path (Plain: verbatim; Encrypted+Unlocked: rekey-on-write).
 */
export function importDatabasesFromSession(
    sessionId: number,
): Promise<number> {
    if (!worker) {
        return Promise.reject(new Error('Worker not initialized'));
    }
    const parts = blobSessionPartsRef(sessionId);
    const blob = new Blob(parts);
    const streamId = nextStreamId--;
    return new Promise((resolve, reject) => {
        streamHandlers.set(streamId, {
            onChunk() {
                streamHandlers.delete(streamId);
                reject(new Error('Unexpected streamChunk during importDatabasesFromSession'));
            },
            onDone(result) {
                streamHandlers.delete(streamId);
                resolve(typeof result === 'number' ? result : 0);
            },
            onError(message) {
                streamHandlers.delete(streamId);
                reject(new Error(message));
            },
        });
        worker!.postMessage({
            streamId,
            data: { type: 'importDatabasesFromSession' },
            blob,
        });
    });
}

/**
 * JSImport entry — single-DB plain export. Drives the worker's per-DB
 * chunked export (Plain disk: verbatim; Encrypted+Unlocked: decrypt to
 * plain pages), accumulates chunks into a single Blob (no envelope
 * wrapper — raw .db bytes a SQLite tool can open), triggers anchor-click
 * download. C# never sees the bytes.
 */
export function exportDatabaseToDownload(
    filename: string,
    dbName: string,
): Promise<boolean> {
    if (!worker) {
        return Promise.reject(new Error('Worker not initialized'));
    }
    const streamId = nextStreamId--;
    const chunks: Blob[] = [];
    return new Promise((resolve, reject) => {
        streamHandlers.set(streamId, {
            onChunk(_name, data) {
                chunks.push(new Blob([data as Uint8Array<ArrayBuffer>]));
            },
            onDone() {
                streamHandlers.delete(streamId);
                try {
                    triggerEnvelopeDownload(filename, new Blob(chunks));
                    resolve(true);
                } catch (e) {
                    reject(e instanceof Error ? e : new Error(String(e)));
                }
            },
            onError(message) {
                streamHandlers.delete(streamId);
                reject(new Error(message));
            },
        });
        worker!.postMessage({
            streamId,
            data: { type: 'exportDatabaseToSession', database: dbName },
        });
    });
}

/**
 * JSImport entry — single-DB plain import. Streams a single picked .db file
 * from the BlobSession to the worker; the worker dispatches by hasGlobalKey()
 * (Encrypted+Unlocked rekeys on write, Plain writes verbatim). The Encrypted+
 * Locked case is the C# caller's responsibility — the model gates the button.
 */
export function importDatabaseFromSession(
    sessionId: number,
    dbName: string,
): Promise<number> {
    if (!worker) {
        return Promise.reject(new Error('Worker not initialized'));
    }
    const parts = blobSessionPartsRef(sessionId);
    const blob = new Blob(parts);
    const streamId = nextStreamId--;
    return new Promise((resolve, reject) => {
        streamHandlers.set(streamId, {
            onChunk() {
                streamHandlers.delete(streamId);
                reject(new Error(`Unexpected streamChunk during importDatabaseFromSession`));
            },
            onDone(result) {
                streamHandlers.delete(streamId);
                resolve(typeof result === 'number' ? result : 0);
            },
            onError(message) {
                streamHandlers.delete(streamId);
                reject(new Error(message));
            },
        });
        worker!.postMessage({
            streamId,
            data: { type: 'importDatabaseFromSession', database: dbName },
            blob,
        });
    });
}

function _sendImportDiskStreamSession(
    type: 'importDiskStreamPreflight' | 'importDiskStreamCommit',
    sessionId: number,
    kWrapView: IMemoryView,
): Promise<number> {
    if (!worker) {
        return Promise.reject(new Error('Worker not initialized'));
    }
    const parts = blobSessionPartsRef(sessionId);
    const blob = new Blob(parts);
    const kWrap = kWrapView.slice();
    const streamId = nextStreamId--;
    return new Promise((resolve, reject) => {
        streamHandlers.set(streamId, {
            onChunk() {
                streamHandlers.delete(streamId);
                reject(new Error(`Unexpected streamChunk during ${type}`));
            },
            onDone(result) {
                streamHandlers.delete(streamId);
                if (typeof result !== 'number') {
                    reject(new Error(`${type} streamDone missing result`));
                    return;
                }
                resolve(result);
            },
            onError(message) {
                streamHandlers.delete(streamId);
                reject(new Error(message));
            },
        });
        worker!.postMessage(
            {
                streamId,
                data: { type },
                blob,
                binaryPayload: kWrap.buffer,
            },
            [kWrap.buffer],
        );
    });
}

function triggerEnvelopeDownload(filename: string, envelope: Blob): void {
    const url = URL.createObjectURL(envelope);
    try {
        const link = document.createElement('a');
        link.href = url;
        link.download = filename;
        link.style.display = 'none';
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
    } finally {
        URL.revokeObjectURL(url);
    }
}

(globalThis as any).sqliteWasmWorker = {
    initializeBridge,
    sendToWorker,
    sendBinaryToWorker,
    exportDiskToDownload,
    blobSessionOpen,
    blobSessionAppend,
    blobSessionDiscard,
    importDiskStreamPreflightFromSession,
    importDiskStreamCommitFromSession,
    importDatabaseFromSession,
    importDatabasesFromSession,
    exportDatabaseToDownload,
    exportDatabasesToDownload,
};

(globalThis as any).__sqliteWasmLogger = logger;
