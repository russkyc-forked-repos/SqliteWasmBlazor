// Import a `.dbs` envelope — a MessagePack array of `[name, bytes]` pairs,
// one per database, no compression.
//
// Two passes over the same Blob. Pass 1 walks the whole envelope and
// validates every entry without touching the pool; pass 2 re-streams it and
// writes. That ordering is the guarantee: an envelope that is truncated,
// mis-sized, or not made of SQLite files is refused while the existing pool
// is still intact. `blob.stream()` is re-callable on the same Blob, so the
// bytes never live concatenated in JS heap.
//
// Plane-neutral: encryption enters only through `crypto`, which plane 2
// supplies and plane 1 leaves undefined.

import {clearBytes} from './memory.js';
import {MULTI_IMPORT_TMP_SUFFIX} from './pool-naming.js';
import {
    BufferedStreamReader,
    readArrayHeader,
    readBinHeader,
    readStr,
} from './msgpack-stream.js';
import {
    assertImportFileCount,
    hasSqliteMagic,
    CHUNK_SLOTS,
    PLAIN_SLOT_SIZE,
    PHYSICAL_SLOT_SIZE,
    SQLITE_MAGIC_HEADER,
    type PoolUtilLike,
} from './import-sink.js';

export interface EnvelopeImportCrypto {
    /**
     * Take the key for the duration of the commit pass. The import wipes what
     * this returns — the pool's registry can be cleared by a lock while the
     * import is still running, so the commit works from its own copy.
     */
    snapshotKey(): Uint8Array;

    rekey(chunk: Uint8Array, dbPath: string, slotIndexBase: number, key: Uint8Array): Uint8Array;
}

export interface EnvelopeImportDeps {
    poolUtil: PoolUtilLike;

    /**
     * Close every open database before the commit pass. An open OFile
     * captures its SAH at xOpen, so a handle that survives an unlink or a
     * slot swap keeps reading and writing a slot the pool has already handed
     * back to the free list — and the next writeFileSlice can hand that slot
     * to another file.
     */
    closeAllDatabases(): Promise<void>;

    /** Undefined on a pool with no encryption. */
    crypto?: EnvelopeImportCrypto;
}

const WHAT = 'importDatabasesFromSession';

/** Pass 1 — validate the entire envelope before touching the pool. */
async function validateEnvelope(blob: Blob): Promise<void> {
    const probe = new BufferedStreamReader(blob.stream().getReader());
    try {
        const fileCount = await readArrayHeader(probe);
        assertImportFileCount(fileCount, WHAT);
        for (let i = 0; i < fileCount; i++) {
            const tupleLen = await readArrayHeader(probe);
            if (tupleLen !== 2) {
                throw new Error(`${WHAT}: file ${i} must be array(2), got array(${tupleLen})`);
            }
            const name = await readStr(probe);
            const plainSize = await readBinHeader(probe);
            assertPlainSize(name, plainSize);
            const head = await probe.read(SQLITE_MAGIC_HEADER.length);
            if (!hasSqliteMagic(head)) {
                throw new Error(
                    `${WHAT}: file '${name}' does not start with the SQLite magic ` +
                    `header — refusing to import a non-plain source.`);
            }
            // skip() throws on premature EOF, so a truncated envelope is also
            // caught here, pre-wipe.
            await probe.skip(plainSize - SQLITE_MAGIC_HEADER.length);
        }
    } finally {
        probe.releaseLock();
    }
}

function assertPlainSize(name: string, plainSize: number): void {
    if (plainSize === 0 || plainSize % PLAIN_SLOT_SIZE !== 0) {
        throw new Error(
            `${WHAT}: file '${name}' plain length ${plainSize} is not a non-zero ` +
            `multiple of ${PLAIN_SLOT_SIZE}.`);
    }
}

/**
 * Write the envelope's databases into the pool.
 *
 * `keepExisting` false wipes the pool first — the replace-everything
 * contract, whose user-facing confirmation the caller owns. True leaves it
 * alone, which is what a validated import needs: the caller has parked the
 * previous content and decides its fate once it has seen what arrived.
 */
export async function importDatabasesFromEnvelope(
    blob: Blob,
    keepExisting: boolean,
    deps: EnvelopeImportDeps,
): Promise<void> {
    const {poolUtil, closeAllDatabases, crypto} = deps;

    await validateEnvelope(blob);

    // Pass 2 — commit. Only reached once the whole envelope has validated.
    await closeAllDatabases();

    if (!keepExisting) {
        for (const name of poolUtil.listDatabases()) {
            try {
                poolUtil.unlink(`/databases/${name}`);
            } catch { /* best-effort */
            }
        }
    }

    const reader = new BufferedStreamReader(blob.stream().getReader());
    const globalKey = crypto?.snapshotKey();
    try {
        const fileCount = await readArrayHeader(reader);

        for (let i = 0; i < fileCount; i++) {
            const tupleLen = await readArrayHeader(reader);
            if (tupleLen !== 2) {
                throw new Error(`${WHAT}: file ${i} must be array(2), got array(${tupleLen})`);
            }
            const name = await readStr(reader);
            const plainSize = await readBinHeader(reader);
            assertPlainSize(name, plainSize);

            const dbPath = `/databases/${name}`;
            const tempPath = `${dbPath}${MULTI_IMPORT_TMP_SUFFIX}`;
            if (poolUtil.getFileNames().includes(tempPath)) {
                try {
                    poolUtil.unlink(tempPath);
                } catch { /* best-effort */
                }
            }

            const totalSlots = plainSize / PLAIN_SLOT_SIZE;
            for (let slotBase = 0; slotBase < totalSlots; slotBase += CHUNK_SLOTS) {
                const slotCount = Math.min(CHUNK_SLOTS, totalSlots - slotBase);
                const plainChunk = await reader.read(slotCount * PLAIN_SLOT_SIZE);

                if (slotBase === 0
                    && !hasSqliteMagic(plainChunk.subarray(0, SQLITE_MAGIC_HEADER.length))) {
                    clearBytes(plainChunk);
                    throw new Error(
                        `${WHAT}: file '${name}' does not start with the SQLite magic ` +
                        `header — refusing to import a non-plain source.`);
                }

                if (crypto === undefined || globalKey === undefined) {
                    try {
                        poolUtil.writeFileSlice(tempPath, slotBase * PLAIN_SLOT_SIZE, plainChunk);
                    } finally {
                        clearBytes(plainChunk);
                    }
                } else {
                    let encryptedChunk: Uint8Array | null = null;
                    try {
                        encryptedChunk = crypto.rekey(plainChunk, dbPath, slotBase, globalKey);
                        poolUtil.writeFileSlice(
                            tempPath, slotBase * PHYSICAL_SLOT_SIZE, encryptedChunk);
                    } finally {
                        clearBytes(plainChunk);
                        if (encryptedChunk !== null) {
                            clearBytes(encryptedChunk);
                        }
                    }
                }
            }

            poolUtil.atomicReplaceFile(tempPath, dbPath);
        }
    } finally {
        reader.releaseLock();
        if (globalKey !== undefined) {
            clearBytes(globalKey);
        }
    }
}
