// stream-bridge.ts
// The streaming half of the C# ⇄ worker protocol, main-thread side.
//
// A streaming request is one the worker answers with a single `streamDone`
// (or `streamError`) keyed by `streamId` rather than the C#-side request id.
// Its payload never travels through postMessage: an export writes its output
// into an OPFS staging file and reports the name, an import reads a file the
// session already staged. That is what keeps the bytes off the main thread —
// a Blob built from ArrayBuffers is memory-backed in WebKit, and a large
// transfer built that way is what closed the pool's access handles on iOS.
//
// `streamId` is allocated from a negative counter so it can never collide
// with the C#-side request id space, which only increments positively.

import {stagedExportFile, triggerDownload} from './staged-download.js';
import {logger} from './sqlite-logger.js';

const MODULE_NAME = 'Worker Bridge';

/** One database's extent inside a multi-database staging file. */
export interface StagedFileEntry {
    name: string;
    offset: number;
    size: number;
}

/** What the worker reports when a streaming request finishes. */
export interface StreamDone {
    result?: number;
    stagingFile?: string;
    files?: StagedFileEntry[];
}

/** How a streaming request reaches the worker. */
export type StreamPost = (message: unknown, transfer?: Transferable[]) => void;

export interface StreamRouter {
    /**
     * Dispatch a worker message carrying a `streamId`. Returns false for
     * anything else, so the caller falls through to the request/response
     * path it shares the port with.
     */
    dispatch(data: unknown): boolean;

    /**
     * Run one streaming request. {@link build} composes the message from the
     * allocated streamId; {@link settle} turns the worker's streamDone into
     * the result. Anything either throws rejects the returned promise, as
     * does a `streamError` from the worker.
     */
    request<T>(
        build: (streamId: number) => { message: unknown; transfer?: Transferable[] },
        settle: (done: StreamDone) => T | Promise<T>,
    ): Promise<T>;
}

interface StreamHandler {
    onDone(done: StreamDone): void;

    onError(message: string): void;
}

/**
 * Create a router over {@link post}. One per bridge — the handler registry
 * and the id counter are per-worker state.
 */
export function createStreamRouter(post: StreamPost): StreamRouter {
    const handlers = new Map<number, StreamHandler>();
    let nextStreamId = -1;

    return {
        dispatch(data: unknown): boolean {
            const message = data as {
                streamId?: unknown;
                streamDone?: unknown;
                streamError?: unknown;
                result?: unknown;
                stagingFile?: unknown;
                files?: unknown;
                error?: unknown;
            };
            if (typeof message?.streamId !== 'number') {
                return false;
            }

            const handler = handlers.get(message.streamId);
            if (!handler) {
                logger.warn(
                    MODULE_NAME, 'Stream message for unknown streamId', message.streamId);
                return true;
            }

            if (message.streamDone === true) {
                handler.onDone({
                    result: typeof message.result === 'number' ? message.result : undefined,
                    stagingFile: typeof message.stagingFile === 'string'
                        ? message.stagingFile
                        : undefined,
                    files: Array.isArray(message.files)
                        ? message.files as StagedFileEntry[]
                        : undefined,
                });
            } else if (message.streamError === true) {
                handler.onError(
                    typeof message.error === 'string' ? message.error : 'unknown stream error');
            } else {
                logger.warn(MODULE_NAME, 'Unknown stream message shape', message);
            }
            return true;
        },

        request<T>(
            build: (streamId: number) => { message: unknown; transfer?: Transferable[] },
            settle: (done: StreamDone) => T | Promise<T>,
        ): Promise<T> {
            const streamId = nextStreamId--;
            return new Promise<T>((resolve, reject) => {
                handlers.set(streamId, {
                    onDone(done) {
                        handlers.delete(streamId);
                        try {
                            resolve(settle(done));
                        } catch (e: unknown) {
                            reject(e instanceof Error ? e : new Error(String(e)));
                        }
                    },
                    onError(message) {
                        handlers.delete(streamId);
                        reject(new Error(message));
                    },
                });

                let sent: { message: unknown; transfer?: Transferable[] };
                try {
                    sent = build(streamId);
                } catch (e: unknown) {
                    handlers.delete(streamId);
                    reject(e instanceof Error ? e : new Error(String(e)));
                    return;
                }
                post(sent.message, sent.transfer);
            });
        },
    };
}

// ---------------------------------------------------------------------------
// The two plane-neutral operations that ride the router. Both bridges expose
// them verbatim as JSImport entry points; only the router differs, because
// only the worker behind it differs.
// ---------------------------------------------------------------------------

/**
 * Multi-database plain export. The worker assembles the complete `.dbs`
 * envelope in an OPFS staging file (Plain: verbatim per file;
 * Encrypted+Unlocked: decrypted per file) and the bridge downloads the
 * disk-backed File. C# never sees the bytes.
 */
export function exportDatabasesToDownload(
    streams: StreamRouter,
    filename: string,
    dbNamesJson: string,
): Promise<boolean> {
    const dbNames = JSON.parse(dbNamesJson) as string[];
    if (!Array.isArray(dbNames) || dbNames.length === 0) {
        return Promise.reject(new Error(
            'exportDatabasesToDownload: dbNames must be non-empty array'));
    }
    return streams.request(
        (streamId) => ({
            message: {streamId, data: {type: 'exportDatabasesToStaging', databases: dbNames}},
        }),
        async (done) => {
            if (done.stagingFile === undefined) {
                throw new Error('exportDatabasesToStaging: streamDone missing stagingFile');
            }
            triggerDownload(filename, await stagedExportFile(done.stagingFile));
            return true;
        });
}

/**
 * Multi-database plain import. Points the worker at the `.dbs` envelope
 * staged under {@link sessionId}; it writes each file in it through the
 * chunked SAH path (Plain: verbatim; Encrypted+Unlocked: rekey-on-write).
 * The pool is wiped first unless {@link keepExisting} says C# has parked the
 * previous content itself and will restore or drop it after inspecting the
 * import.
 */
export function importDatabasesFromSession(
    streams: StreamRouter,
    sessionId: number,
    keepExisting: boolean,
): Promise<number> {
    return streams.request(
        (streamId) => ({
            message: {
                streamId,
                data: {type: 'importDatabasesFromSession', sessionId, keepExisting},
            },
        }),
        (done) => typeof done.result === 'number' ? done.result : 0);
}
