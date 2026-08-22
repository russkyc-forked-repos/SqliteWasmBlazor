// Export a `.dbs` envelope into an OPFS staging file — a MessagePack array
// of `[name, bytes]` pairs, one per database, no compression.
//
// The bytes go pool slot -> staging file in chunks and never exist in one
// piece: the browser saves from the finished staging entry as a disk-backed
// File, so peak memory is one chunk whatever the databases weigh.
//
// Plane-neutral: encryption enters only through `crypto`, whose `toPlain`
// turns a batch of on-disk slots into plain pages. Plane 1 leaves it
// undefined and the slots are already plain.

import {clearBytes} from './memory.js';
import {
    openExportStaging,
    type ExportStagingFile,
} from './export-staging.js';
import {packArrayHeader, packBinHeader, packStr} from './msgpack-stream.js';
import {
    CHUNK_SLOTS,
    PLAIN_SLOT_SIZE,
    PHYSICAL_SLOT_SIZE,
    type PoolUtilLike,
} from './import-sink.js';

export interface EnvelopeExportCrypto {
    /**
     * Take the key for the duration of the export. The export wipes what this
     * returns — a lock can clear the registry while the export is running.
     */
    snapshotKey(): Uint8Array;

    /** Decrypt one batch of on-disk slots into plain pages. */
    toPlain(chunk: Uint8Array, dbPath: string, slotIndexBase: number, key: Uint8Array): Uint8Array;
}

export interface EnvelopeExportDeps {
    poolUtil: PoolUtilLike & {
        getFileSize(name: string): number;
        exportFileSlice(name: string, offset: number, length: number): Uint8Array;
    };

    /**
     * Close one database so its slots are a consistent snapshot. Any result
     * the worker's own close returns is ignored.
     */
    closeDatabase(dbName: string): Promise<unknown>;

    /** Undefined on a pool with no encryption. */
    crypto?: EnvelopeExportCrypto;
}

const WHAT = 'exportDatabasesToStaging';

/**
 * Write the named databases into a staging file and return it. The caller
 * hands the staging name back to the bridge, which lifts it as a File.
 */
export async function exportDatabasesToStaging(
    dbNames: readonly string[],
    deps: EnvelopeExportDeps,
): Promise<ExportStagingFile> {
    const {poolUtil, closeDatabase, crypto} = deps;

    if (dbNames.length === 0) {
        throw new Error(`${WHAT}: dbNames must be non-empty`);
    }

    const encrypted = crypto !== undefined;
    const sourceSlotSize = encrypted ? PHYSICAL_SLOT_SIZE : PLAIN_SLOT_SIZE;

    const fileNames = poolUtil.getFileNames();
    for (const dbName of dbNames) {
        const dbPath = `/databases/${dbName}`;
        if (!fileNames.includes(dbPath)) {
            throw new Error(`${WHAT}: no existing DB at ${dbPath}`);
        }
    }

    // Close every DB up front so the SAH snapshot is consistent across the
    // envelope (no slot reads racing with SQLite writes from an open ctx).
    for (const dbName of dbNames) {
        await closeDatabase(dbName);
    }

    const globalKey = crypto?.snapshotKey();
    const staging = await openExportStaging();
    try {
        // Outer array header. Per file: array(2) + str(name) + bin(plainSize).
        staging.write(packArrayHeader(dbNames.length));

        for (const dbName of dbNames) {
            const dbPath = `/databases/${dbName}`;
            const fileSize = poolUtil.getFileSize(dbPath);
            if (fileSize === 0 || fileSize % sourceSlotSize !== 0) {
                throw new Error(
                    `${WHAT}: ${dbName} length ${fileSize} is not a non-zero ` +
                    `multiple of slot size ${sourceSlotSize} ` +
                    `(pool state=${encrypted ? 'encrypted' : 'plain'}).`);
            }
            const totalSlots = fileSize / sourceSlotSize;

            staging.write(packArrayHeader(2));
            for (const part of packStr(dbName)) {
                staging.write(part);
            }
            staging.write(packBinHeader(totalSlots * PLAIN_SLOT_SIZE));

            for (let slotBase = 0; slotBase < totalSlots; slotBase += CHUNK_SLOTS) {
                const slotCount = Math.min(CHUNK_SLOTS, totalSlots - slotBase);
                const sourceOffset = slotBase * sourceSlotSize;
                const sourceBytes = slotCount * sourceSlotSize;

                let sourceChunk: Uint8Array | null = null;
                let plainChunk: Uint8Array | null = null;
                try {
                    sourceChunk = poolUtil.exportFileSlice(dbPath, sourceOffset, sourceBytes);
                    if (crypto !== undefined && globalKey !== undefined) {
                        plainChunk = crypto.toPlain(sourceChunk, dbPath, slotBase, globalKey);
                    } else {
                        plainChunk = sourceChunk;
                        sourceChunk = null;
                    }
                    staging.write(plainChunk);
                } finally {
                    if (sourceChunk !== null) {
                        clearBytes(sourceChunk);
                    }
                    if (plainChunk !== null) {
                        clearBytes(plainChunk);
                    }
                }
            }
        }

        staging.finish();
        return staging;
    } catch (err) {
        await staging.abort();
        throw err;
    } finally {
        if (globalKey !== undefined) {
            clearBytes(globalKey);
        }
    }
}
