// worker-bridge.ts
// Bridge between C# JSImport and Web Worker.
// Exposes a single async initializeBridge(baseHref, assetRoot) entry point;
// C# awaits its returned Promise so worker creation errors surface on the .NET side.

import { base64ToBytes } from '@sqlitewasmblazor/crypto-core';
import {
    deleteStagedExportFile,
    downloadStagedExport,
    stagedExportFile,
    triggerDownload,
} from '@sqlitewasmblazor/worker-common';

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
    // Write side of the dotnet MemoryView — copies `source` into the managed
    // Span this view wraps. Only valid while the originating synchronous
    // JSImport call is on the stack (the Span is unpinned on return).
    set(source: Uint8Array, targetOffset?: number): void;
    readonly byteLength: number;
}

let worker: Worker | null = null;

/**
 * JS-side stream handler registry — separate from the C# request-id space.
 * Streaming worker calls answer with exactly one `streamDone` (or
 * `streamError`) keyed by `streamId`. Export operations never send bytes
 * through postMessage: the worker writes its output into an OPFS staging
 * file and streamDone carries the staging file name (plus a per-file
 * offset table for the disk export), which the bridge lifts as a
 * disk-backed File for delivery.
 *
 * `streamId` is allocated from a negative-int counter so it never collides
 * with the C#-side `_nextRequestId` (which only increments positively).
 */
interface StagedFileEntry {
    name: string;
    offset: number;
    size: number;
}
interface StreamHandler {
    onDone(result?: number, stagingFile?: string, files?: StagedFileEntry[]): void;
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
        // a handler in `streamHandlers`. Worker answers each streaming
        // request with one streamDone (or streamError) under the same
        // streamId. C# never sees these messages directly.
        if (event.data.streamId !== undefined) {
            const handler = streamHandlers.get(event.data.streamId);
            if (!handler) {
                console.warn(
                    '[Worker Bridge] Stream message for unknown streamId',
                    event.data.streamId);
                return;
            }
            if (event.data.streamDone === true) {
                handler.onDone(
                    typeof event.data.result === 'number' ? event.data.result : undefined,
                    typeof event.data.stagingFile === 'string' ? event.data.stagingFile : undefined,
                    Array.isArray(event.data.files)
                        ? event.data.files as StagedFileEntry[]
                        : undefined,
                );
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
// Chunked encrypted-disk export — worker → OPFS staging → File download.
//
// The worker rekeys every DB and writes the ciphertext contiguously into
// an OPFS staging file, then reports the staging name plus a per-file
// { name, offset, size } table via streamDone. The bridge lifts the
// staging file as a disk-backed File and composes the final v3 envelope
// as a virtual-concat Blob of:
//
//   <MessagePack header bytes (array(8) + 7 small fields)>
//   <per-file array headers + name + bin headers + File.slice() parts>
//
// The ciphertext never occupies main-thread memory — File.slice() parts
// stay disk-backed, which is what keeps iPhone Safari alive on large
// exports (Blobs constructed from ArrayBuffers are memory-backed in
// WebKit). C# never sees a managed byte[] of the envelope. The download
// fires from the bridge via anchor click; C# awaits a boolean result.
// ---------------------------------------------------------------------------

/**
 * JSImport entry point — called by C# `ExportPoolToDownloadAsync`. Drives
 * the worker staging export, composes the envelope Blob, triggers the
 * download. Returns `true` on completion. The staging file backing the
 * download is collected by the worker's init sweep next session.
 */
export function exportDiskToDownload(
    filename: string,
    metadataJson: string,
    kWrapView: IMemoryView,
): Promise<boolean> {
    return _assembleEnvelopeStaged(metadataJson, kWrapView).then(({ blob }) => {
        triggerDownload(filename, blob);
        return true;
    });
}

// ---------------------------------------------------------------------------
// Test/diagnostic export-to-bytes — assembles the identical v3 envelope Blob
// as {@link exportDiskToDownload}, then makes the bytes drainable by C#
// instead of triggering the anchor-click download. The whole envelope is
// materialised in memory (the very thing the streaming download path avoids),
// so this exists ONLY so in-page round-trip tests can feed a real export back
// into the guided import. `Task<byte[]>` is not a supported JS-interop return
// shape, so the protocol is: stash bytes → return length → C# reads chunks
// into a MemoryView → discard. See the C# JSImports
// `SqliteWasmWorkerBridge.{ExportPoolToBytesSessionAsync,ReadExportBytes,DiscardExportBytes}`.
// ---------------------------------------------------------------------------

const exportByteStash = new Map<number, Uint8Array>();

export function exportDiskToBytesSession(
    metadataJson: string,
    kWrapView: IMemoryView,
    sessionId: number,
): Promise<number> {
    if (exportByteStash.has(sessionId)) {
        return Promise.reject(new Error(
            `exportDiskToBytesSession: sessionId ${sessionId} already in use`));
    }
    return _assembleEnvelopeStaged(metadataJson, kWrapView)
        .then(async ({ blob, stagingFile }) => {
            const buf = await blob.arrayBuffer();
            // Bytes fully materialised — the staging file is no longer
            // referenced and can be deleted immediately.
            await deleteStagedExportFile(stagingFile);
            const bytes = new Uint8Array(buf);
            exportByteStash.set(sessionId, bytes);
            return bytes.length;
        });
}

export function readExportBytes(
    sessionId: number,
    offset: number,
    destView: IMemoryView,
): number {
    const bytes = exportByteStash.get(sessionId);
    if (!bytes) {
        throw new Error(`readExportBytes: unknown sessionId ${sessionId}`);
    }
    const remaining = bytes.length - offset;
    if (remaining <= 0) {
        return 0;
    }
    const n = Math.min(destView.byteLength, remaining);
    destView.set(bytes.subarray(offset, offset + n));
    return n;
}

export function discardExportBytes(sessionId: number): void {
    exportByteStash.delete(sessionId);
}

/**
 * Shared assembly path: drive the worker staging export, lift the staging
 * file as a disk-backed File, compose the MessagePack envelope Blob from
 * header bytes plus per-DB File.slice() segments via the positional
 * encoder. The returned staging file name lets the test-only bytes path
 * delete the staging entry once it has materialised the bytes.
 */
function _assembleEnvelopeStaged(
    metadataJson: string,
    kWrapView: IMemoryView,
): Promise<{ blob: Blob; stagingFile: string }> {
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

    return new Promise((resolve, reject) => {
        streamHandlers.set(streamId, {
            onDone(_result, stagingFile, files) {
                streamHandlers.delete(streamId);
                if (stagingFile === undefined || files === undefined) {
                    reject(new Error(
                        'exportDiskToStaging: streamDone missing stagingFile/files'));
                    return;
                }
                stagedExportFile(stagingFile)
                    .then((file) => {
                        // File.slice() segments stay backed by the OPFS
                        // entry — composing them into the envelope Blob
                        // adds no main-thread memory.
                        const fileParts = files.map((f) => ({
                            name: f.name,
                            size: f.size,
                            blob: file.slice(f.offset, f.offset + f.size),
                        }));
                        resolve({
                            blob: composeEnvelopeBlob(meta, fileParts),
                            stagingFile,
                        });
                    })
                    .catch((e: unknown) => {
                        reject(e instanceof Error ? e : new Error(String(e)));
                    });
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
                data: { type: 'exportDiskToStaging' },
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
 * Returns a virtual-concatenation Blob — the per-DB segments are
 * File.slice() parts backed by the OPFS staging entry, so the payload
 * bytes never occupy main-thread memory.
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
 * JSImport entry — multi-DB plain export. The worker assembles the
 * complete <c>.dbs</c> envelope (Plain: verbatim per file;
 * Encrypted+Unlocked: decrypt per file) in an OPFS staging file; the
 * bridge downloads the disk-backed File via anchor click. C# never sees
 * the bytes and the envelope never occupies main-thread memory.
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
    return new Promise((resolve, reject) => {
        streamHandlers.set(streamId, {
            onDone(_result, stagingFile) {
                streamHandlers.delete(streamId);
                if (stagingFile === undefined) {
                    reject(new Error('exportDatabasesToStaging: streamDone missing stagingFile'));
                    return;
                }
                stagedExportFile(stagingFile)
                    .then((file) => {
                        triggerDownload(filename, file);
                        resolve(true);
                    })
                    .catch((e: unknown) => {
                        reject(e instanceof Error ? e : new Error(String(e)));
                    });
            },
            onError(message) {
                streamHandlers.delete(streamId);
                reject(new Error(message));
            },
        });
        worker!.postMessage({
            streamId,
            data: { type: 'exportDatabasesToStaging', databases: dbNames },
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
 * JSImport entry — single-DB plain export. The worker writes the per-DB
 * export (Plain disk: verbatim; Encrypted+Unlocked: decrypt to plain
 * pages) into an OPFS staging file — no envelope wrapper, raw .db bytes a
 * SQLite tool can open — and the bridge downloads the disk-backed File
 * via anchor click. C# never sees the bytes.
 */
export function exportDatabaseToDownload(
    filename: string,
    dbName: string,
): Promise<boolean> {
    if (!worker) {
        return Promise.reject(new Error('Worker not initialized'));
    }
    const streamId = nextStreamId--;
    return new Promise((resolve, reject) => {
        streamHandlers.set(streamId, {
            onDone(_result, stagingFile) {
                streamHandlers.delete(streamId);
                if (stagingFile === undefined) {
                    reject(new Error('exportDatabaseToStaging: streamDone missing stagingFile'));
                    return;
                }
                stagedExportFile(stagingFile)
                    .then((file) => {
                        triggerDownload(filename, file);
                        resolve(true);
                    })
                    .catch((e: unknown) => {
                        reject(e instanceof Error ? e : new Error(String(e)));
                    });
            },
            onError(message) {
                streamHandlers.delete(streamId);
                reject(new Error(message));
            },
        });
        worker!.postMessage({
            streamId,
            data: { type: 'exportDatabaseToStaging', database: dbName },
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

// Staged export downloads live in worker-common so both bridges share one
// staging-directory name and one filename → content-type mapping; see
// staged-download.ts for why the content type decides whether iOS Safari
// keeps the filename we ask for.
export { downloadStagedExport };

(globalThis as any).sqliteWasmWorker = {
    initializeBridge,
    sendToWorker,
    sendBinaryToWorker,
    exportDiskToDownload,
    exportDiskToBytesSession,
    readExportBytes,
    discardExportBytes,
    blobSessionOpen,
    blobSessionAppend,
    blobSessionDiscard,
    importDiskStreamPreflightFromSession,
    importDiskStreamCommitFromSession,
    importDatabaseFromSession,
    importDatabasesFromSession,
    exportDatabaseToDownload,
    exportDatabasesToDownload,
    downloadStagedExport,
};

(globalThis as any).__sqliteWasmLogger = logger;
