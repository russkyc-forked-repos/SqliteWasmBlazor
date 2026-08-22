// Chunked import sink for a single plain .db file, plus the small pieces
// every import path shares (the Files-count bound, the PoolImportResult
// codes, and the structural pool contract).
//
// Plane-neutral by construction: encryption enters only as an optional
// transform the caller supplies. With no key the sink writes plain pages,
// which is exactly what a pool with no encryption needs.

import {clearBytes} from './memory.js';
import {
    POOL_IMPORT_TMP_SUFFIX,
    SINGLE_IMPORT_TMP_SUFFIX,
} from './pool-naming.js';

const SECTOR_SIZE = 4096;


/**
 * Upper bound on the Files array of any import envelope. The SAH pool
 * starts at capacity 25 and grows on demand, but no legitimate export
 * comes anywhere near this — the bound exists so a crafted count can't
 * drive the import loops.
 */
export const MAX_IMPORT_FILES = 256;

/**
 * Validate an untrusted Files-array count before using it as a loop
 * bound. Rejects zero, non-integers, and anything above
 * {@link MAX_IMPORT_FILES}. (Negative values can no longer arrive —
 * readArrayHeader coerces unsigned — but reject them anyway.)
 */
export function assertImportFileCount(count: number, context: string): void {
    if (!Number.isInteger(count) || count <= 0 || count > MAX_IMPORT_FILES) {
        throw new Error(
            `${context}: envelope Files count ${count} is outside 1..${MAX_IMPORT_FILES} — refusing import.`);
    }
}

/**
 * Mirrors the C# `PoolImportResult` enum so callers can branch on the
 * Promise resolution without crossing string boundaries.
 */
export const PoolImportResult = Object.freeze({
    OK: 0,
    WRONG_KEY: 1,
    EXISTING_DB_REFUSED: 2,
} as const);

export type PoolImportResultCode = typeof PoolImportResult[keyof typeof PoolImportResult];

export interface PoolUtilLike {
    listDatabases(): string[];

    getFileNames(): string[];

    importDb(path: string, data: Uint8Array, opaque?: boolean): unknown;

    writeFileSlice(name: string, offset: number, bytes: Uint8Array): void;

    atomicReplaceFile(srcName: string, dstName: string): true;

    unlink(filename: string): boolean;
}

export const PLAIN_SLOT_SIZE = 4096;

// Stride of one encrypted slot on disk: the 4096-byte page plus its AEAD
// nonce and tag. The sink needs it to place a rekeyed batch, even though it
// never performs the sealing itself.
export const PHYSICAL_SLOT_SIZE = SECTOR_SIZE + 12 + 16; // 4124

/**
 * Plain pages per write batch. 256 × 4096 B = 1 MB in, ~1.03 MB out once
 * encrypted — both well under the per-op JS heap budget on mobile Safari.
 */
export const CHUNK_SLOTS = 256;

export const SQLITE_MAGIC_HEADER = Uint8Array.from([
    0x53, 0x51, 0x4c, 0x69, 0x74, 0x65, 0x20, 0x66,  // "SQLite f"
    0x6f, 0x72, 0x6d, 0x61, 0x74, 0x20, 0x33, 0x00,  // "ormat 3\0"
]);

export function hasSqliteMagic(bytes: Uint8Array): boolean {
    if (bytes.length < SQLITE_MAGIC_HEADER.length) {
        return false;
    }
    for (let i = 0; i < SQLITE_MAGIC_HEADER.length; i++) {
        if (bytes[i] !== SQLITE_MAGIC_HEADER[i]) {
            return false;
        }
    }
    return true;
}

/**
 * Sink for a single plain .db file pushed in from C#, chunk by chunk.
 *
 * The bytes never exist anywhere in one piece: each chunk arrives as a
 * transferred ArrayBuffer, is written into the pool's temp slot, and is
 * wiped. Peak worker memory is one slot batch plus one incoming chunk —
 * about 2 MB whatever the file's size. Backpressure is the round trip
 * itself: C# awaits each append, so a chunk is on the wire only once the
 * one before it is on disk.
 *
 * State-aware dispatch by <paramref name="globalKey"/>:
 *
 *   undefined → write plain pages verbatim (sources from a SQLite tool
 *               or another SqliteWasmBlazor instance on a Plain disk)
 *   Uint8Array → rekey each batch to encrypted slots under globalKey and
 *               write at the encrypted offset. After commit the file is
 *               slot-format ciphertext under globalKey.
 *
 * Either way the bytes land in a temp slot that only becomes the database
 * at {@link DatabaseImportSink.commit}, so an import that fails or is
 * abandoned part-way leaves dbName exactly as it was.
 *
 * SQLite magic check on the first 16 bytes guards against a source whose
 * length divides by 4096 by coincidence.
 */
export interface DatabaseImportSink {
    /** Take the next chunk of the source file. Any length. */
    append(bytes: Uint8Array): void;

    /** Promote the temp slot over the database. Refuses a short source. */
    commit(): void;

    /** Drop the temp slot. Idempotent; safe after commit. */
    abort(): void;
}

export function createDatabaseImportSink(
    dbName: string,
    plainSize: number,
    poolUtil: PoolUtilLike,
    globalKey: Uint8Array | undefined,
    rekeyFn: ((chunk: Uint8Array, dbPath: string, slotIndexBase: number, key: Uint8Array) => Uint8Array) | undefined,
): DatabaseImportSink {
    if (plainSize === 0 || plainSize % PLAIN_SLOT_SIZE !== 0) {
        throw new Error(
            `importDatabase: ${dbName} length ${plainSize} is not a non-zero ` +
            `multiple of the plain page size ${PLAIN_SLOT_SIZE}.`);
    }
    if (globalKey !== undefined && rekeyFn === undefined) {
        throw new Error(
            `importDatabase: globalKey supplied but no rekey fn — caller bug.`);
    }

    const dbPath = `/databases/${dbName}`;
    const tempPath = `${dbPath}${SINGLE_IMPORT_TMP_SUFFIX}`;

    // A temp slot from an attempt that never finished. unlink is a no-op on
    // a missing path; anything found here is unreachable by definition.
    if (poolUtil.getFileNames().includes(tempPath)) {
        try {
            poolUtil.unlink(tempPath);
        } catch { /* best-effort */
        }
    }

    // Batch buffer — a whole slot batch is rekeyed and written at once, so
    // chunk boundaries from C# need not line up with slot boundaries.
    const batch = new Uint8Array(CHUNK_SLOTS * PLAIN_SLOT_SIZE);
    let batchLen = 0;
    let slotBase = 0;
    let received = 0;
    let committed = false;

    function flushBatch(): void {
        if (batchLen === 0) {
            return;
        }
        if (batchLen % PLAIN_SLOT_SIZE !== 0) {
            throw new Error(
                `importDatabase: ${dbName} batch of ${batchLen} bytes is not a ` +
                `multiple of the plain page size ${PLAIN_SLOT_SIZE}.`);
        }
        const plainChunk = batch.subarray(0, batchLen);
        // First bytes of the file, whatever the chunking was. Nothing has
        // been written yet, so a source that is not a plain SQLite file is
        // refused before it costs a slot.
        if (slotBase === 0 && !hasSqliteMagic(plainChunk)) {
            clearBytes(plainChunk);
            throw new Error(
                `importDatabase: ${dbName} does not start with the SQLite ` +
                `magic header — refusing to import a non-plain source.`);
        }
        if (globalKey === undefined) {
            poolUtil.writeFileSlice(tempPath, slotBase * PLAIN_SLOT_SIZE, plainChunk);
        } else {
            let encryptedChunk: Uint8Array | null = null;
            try {
                encryptedChunk = rekeyFn!(plainChunk, dbPath, slotBase, globalKey);
                poolUtil.writeFileSlice(
                    tempPath, slotBase * PHYSICAL_SLOT_SIZE, encryptedChunk);
            } finally {
                if (encryptedChunk !== null) {
                    clearBytes(encryptedChunk);
                }
            }
        }
        slotBase += batchLen / PLAIN_SLOT_SIZE;
        // Plaintext pages — wipe before the buffer is reused.
        clearBytes(plainChunk);
        batchLen = 0;
    }

    return {
        append(bytes: Uint8Array): void {
            if (committed) {
                throw new Error(`importDatabase: append after commit for ${dbName}.`);
            }
            if (received + bytes.length > plainSize) {
                throw new Error(
                    `importDatabase: ${dbName} source is longer than the declared ` +
                    `${plainSize} bytes.`);
            }
            received += bytes.length;

            let offset = 0;
            while (offset < bytes.length) {
                const take = Math.min(batch.length - batchLen, bytes.length - offset);
                batch.set(bytes.subarray(offset, offset + take), batchLen);
                batchLen += take;
                offset += take;
                if (batchLen === batch.length) {
                    flushBatch();
                }
            }
        },

        commit(): void {
            if (committed) {
                throw new Error(`importDatabase: commit twice for ${dbName}.`);
            }
            if (received !== plainSize) {
                throw new Error(
                    `importDatabase: ${dbName} source ended at ${received} of ` +
                    `${plainSize} bytes; it is truncated.`);
            }
            flushBatch();
            poolUtil.atomicReplaceFile(tempPath, dbPath);
            committed = true;
        },

        abort(): void {
            batch.fill(0);
            batchLen = 0;
            if (committed) {
                return;
            }
            try {
                poolUtil.unlink(tempPath);
            } catch { /* best-effort */
            }
        },
    };
}
