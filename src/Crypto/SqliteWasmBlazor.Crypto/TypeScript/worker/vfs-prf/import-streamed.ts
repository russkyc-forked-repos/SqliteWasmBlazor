// Streaming import path for the asymmetric encrypted-disk envelope (v3).
//
// Consumes a Blob holding the full MessagePack EncryptedDiskEnvelope via
// blob.stream() — never materialises the whole envelope as a single buffer
// in the worker. Two passes (run separately from C#):
//
//   1. Preflight: walk the file table, AEAD-decrypt EVERY slot of EVERY
//      file under K_wrap with `prf-vfs-v1|{dbPath}|{slot}` AAD,
//      discarding the plaintext. Slot-0 tag failure means WRONG_KEY;
//      a later slot's failure means a tampered/corrupt envelope and
//      throws. Either way: abort with no writes done.
//   2. Commit:    re-stream the envelope, write each file as a temp slot
//      via writeFileSlice chunk-by-chunk under globalKey, then promote
//      all temps via atomicReplaceFile only after every file has been
//      fully decrypted + rewritten.
//
// Why two passes: preflight preserves the wipe-after-validate invariant
// the legacy whole-buffer import had — the caller wipes the existing
// pool only after preflight has authenticated every byte of ciphertext
// in the envelope, so a crafted/corrupt envelope can never destroy the
// existing disk. blob.stream() is re-callable on the same Blob, so we
// re-open between passes — the bytes never live concatenated in JS heap.

import {
    encryptChaCha20Poly1305,
    decryptChaCha20Poly1305,
    clearBytes,
} from '@sqlitewasmblazor/crypto-core';
import { buildPageAad } from './aad.js';
import {
    BufferedStreamReader,
    readArrayHeader,
    readBinHeader,
    readStr,
    readUint,
} from '../../bridge/msgpack-stream.js';

const SECTOR_SIZE = 4096;
const PAGE_NONCE_LEN = 12;
const PAGE_TAG_LEN = 16;
const PHYSICAL_SLOT_SIZE = SECTOR_SIZE + PAGE_NONCE_LEN + PAGE_TAG_LEN; // 4124

const ENVELOPE_VERSION = 3;
const ENVELOPE_ARRAY_LEN = 8;
const ENVELOPE_AAD_VERSION = 'v1';
const PRF_SALT_LEN = 32;

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
export const DiskImportResult = Object.freeze({
    OK: 0,
    WRONG_KEY: 1,
    EXISTING_DB_REFUSED: 2,
} as const);

export type DiskImportResultCode = typeof DiskImportResult[keyof typeof DiskImportResult];

interface PoolUtilLike {
    listDatabases(): string[];
    getFileNames(): string[];
    importDb(path: string, data: Uint8Array, opaque?: boolean): unknown;
    writeFileSlice(name: string, offset: number, bytes: Uint8Array): void;
    atomicReplaceFile(srcName: string, dstName: string): true;
    unlink(filename: string): boolean;
}

const PLAIN_SLOT_SIZE = 4096;
const SQLITE_MAGIC_HEADER = Uint8Array.from([
    0x53, 0x51, 0x4c, 0x69, 0x74, 0x65, 0x20, 0x66,  // "SQLite f"
    0x6f, 0x72, 0x6d, 0x61, 0x74, 0x20, 0x33, 0x00,  // "ormat 3\0"
]);

function hasSqliteMagic(bytes: Uint8Array): boolean {
    if (bytes.length < SQLITE_MAGIC_HEADER.length) { return false; }
    for (let i = 0; i < SQLITE_MAGIC_HEADER.length; i++) {
        if (bytes[i] !== SQLITE_MAGIC_HEADER[i]) { return false; }
    }
    return true;
}

/**
 * Stream a single plain .db file from a Blob into the SAH pool. State-
 * aware dispatch by <paramref name="globalKey"/>:
 *
 *   undefined → write plain pages verbatim (sources from a SQLite tool
 *               or another SqliteWasmBlazor instance on a Plain disk)
 *   Uint8Array → rekey each chunk to encrypted slots under globalKey,
 *               write to the SAH at the encrypted offset. After commit
 *               the file is slot-format ciphertext under globalKey
 *               (same shape as importDatabasePlain's whole-buffer path
 *               but with no byte[] resident in the worker).
 *
 * Either way the on-disk file goes through temp slot + atomicReplaceFile,
 * so a mid-loop failure leaves the existing dbName untouched.
 *
 * SQLite magic check on the first chunk's first 16 bytes guards against
 * a corrupt source whose length divides by 4096 by coincidence.
 */
export async function importDatabaseFromBlob(
    blob: Blob,
    dbName: string,
    poolUtil: PoolUtilLike,
    globalKey: Uint8Array | undefined,
    rekeyFn: ((chunk: Uint8Array, dbPath: string, slotIndexBase: number, key: Uint8Array) => Uint8Array) | undefined,
): Promise<void> {
    if (blob.size === 0 || blob.size % PLAIN_SLOT_SIZE !== 0) {
        throw new Error(
            `importDatabaseFromBlob: ${dbName} length ${blob.size} is not a non-zero ` +
            `multiple of the plain page size ${PLAIN_SLOT_SIZE}.`);
    }
    if (globalKey !== undefined && rekeyFn === undefined) {
        throw new Error(
            `importDatabaseFromBlob: globalKey supplied but no rekey fn — caller bug.`);
    }

    const dbPath = `/databases/${dbName}`;
    const tempPath = `${dbPath}.single-import-tmp`;
    const totalSlots = blob.size / PLAIN_SLOT_SIZE;

    if (poolUtil.getFileNames().includes(tempPath)) {
        try { poolUtil.unlink(tempPath); } catch { /* best-effort */ }
    }

    const CHUNK_SLOTS = 256;
    const PHYSICAL_SLOT_SIZE = 4124;
    const reader = new BufferedStreamReader(blob.stream().getReader());

    try {
        for (let slotBase = 0; slotBase < totalSlots; slotBase += CHUNK_SLOTS) {
            const slotCount = Math.min(CHUNK_SLOTS, totalSlots - slotBase);
            const plainBytes = slotCount * PLAIN_SLOT_SIZE;
            const plainChunk = await reader.read(plainBytes);

            // SQLite magic check on the first chunk's first 16 bytes.
            if (slotBase === 0 && !hasSqliteMagic(plainChunk)) {
                clearBytes(plainChunk);
                throw new Error(
                    `importDatabaseFromBlob: ${dbName} does not start with the SQLite ` +
                    `magic header — refusing to import a non-plain source.`);
            }

            if (globalKey === undefined) {
                // Plain target — write verbatim at the plain offset.
                poolUtil.writeFileSlice(tempPath, slotBase * PLAIN_SLOT_SIZE, plainChunk);
                clearBytes(plainChunk);
            } else {
                // Encrypted target — rekey then write at the encrypted offset.
                let encryptedChunk: Uint8Array | null = null;
                try {
                    encryptedChunk = rekeyFn!(plainChunk, dbPath, slotBase, globalKey);
                    poolUtil.writeFileSlice(
                        tempPath,
                        slotBase * PHYSICAL_SLOT_SIZE,
                        encryptedChunk!);
                } finally {
                    clearBytes(plainChunk);
                    if (encryptedChunk !== null) { clearBytes(encryptedChunk); }
                }
            }
        }

        poolUtil.atomicReplaceFile(tempPath, dbPath);
    } catch (err) {
        try { poolUtil.unlink(tempPath); } catch { /* best-effort */ }
        throw err;
    } finally {
        reader.releaseLock();
    }
}

/**
 * Forward-skip past the envelope's leading metadata block (everything
 * before <c>Files</c>) on a fresh stream. After this call the reader is
 * positioned exactly at the <c>Files</c> array header.
 *
 * The metadata is shape-validated (version + AadVersion + PrfSalt length)
 * but otherwise discarded — the caller already supplies the unwrapped
 * K_wrap and the recipient PRF + ECIES context lives in the recipient's
 * own config, not the envelope.
 */
async function consumeEnvelopeMetadata(reader: BufferedStreamReader): Promise<void> {
    const arrLen = await readArrayHeader(reader);
    if (arrLen !== ENVELOPE_ARRAY_LEN) {
        throw new Error(
            `importDiskStreamed: expected envelope array(${ENVELOPE_ARRAY_LEN}), got array(${arrLen})`);
    }
    const version = await readUint(reader);
    if (version !== ENVELOPE_VERSION) {
        throw new Error(
            `importDiskStreamed: unsupported envelope Version=${version} (expected ${ENVELOPE_VERSION})`);
    }
    const aadVersion = await readStr(reader);
    if (aadVersion !== ENVELOPE_AAD_VERSION) {
        throw new Error(
            `importDiskStreamed: unsupported AadVersion='${aadVersion}' (expected '${ENVELOPE_AAD_VERSION}')`);
    }
    const prfSaltLen = await readBinHeader(reader);
    if (prfSaltLen !== PRF_SALT_LEN) {
        throw new Error(
            `importDiskStreamed: PrfSalt must be ${PRF_SALT_LEN} bytes, got ${prfSaltLen}`);
    }
    await reader.skip(prfSaltLen);
    // Discard remaining metadata strings: EphPub, WrapCt, WrapNonce,
    // CredIdHint. Caller already supplies the unwrapped K_wrap; these
    // strings are envelope-self-describing fields for future cross-app
    // import paths.
    await readStr(reader);
    await readStr(reader);
    await readStr(reader);
    await readStr(reader);
}

/**
 * Decrypt a physical slot under <paramref name="key"/> with AAD bound to
 * <paramref name="aad"/>. Returns the freshly decrypted plaintext;
 * caller wipes it.
 */
function decryptSlot(slot: Uint8Array, key: Uint8Array, aad: Uint8Array): Uint8Array {
    const ct = slot.subarray(0, SECTOR_SIZE);
    const nonce = slot.subarray(SECTOR_SIZE, SECTOR_SIZE + PAGE_NONCE_LEN);
    const tag = slot.subarray(SECTOR_SIZE + PAGE_NONCE_LEN, PHYSICAL_SLOT_SIZE);
    const cipherPlusTag = new Uint8Array(SECTOR_SIZE + PAGE_TAG_LEN);
    cipherPlusTag.set(ct, 0);
    cipherPlusTag.set(tag, SECTOR_SIZE);
    return decryptChaCha20Poly1305({ ciphertext: cipherPlusTag, nonce }, key, aad);
}

/**
 * Write a freshly AEAD-sealed slot (`ct(4096) ‖ nonce(12) ‖ tag(16)`) at
 * <paramref name="dstStart"/> within <paramref name="out"/>.
 */
function writeEncryptedSlot(
    plaintext: Uint8Array,
    key: Uint8Array,
    aad: Uint8Array,
    out: Uint8Array,
    dstStart: number,
): void {
    const enc = encryptChaCha20Poly1305(plaintext, key, aad);
    out.set(enc.ciphertext.subarray(0, SECTOR_SIZE), dstStart);
    out.set(enc.nonce, dstStart + SECTOR_SIZE);
    out.set(enc.ciphertext.subarray(SECTOR_SIZE), dstStart + SECTOR_SIZE + PAGE_NONCE_LEN);
}

/**
 * Pass 1 — preflight. Walks the envelope's Files array and AEAD-verifies
 * EVERY slot of EVERY file under K_wrap, discarding the plaintext.
 * Returns OK only if the entire envelope authenticates; WRONG_KEY on a
 * slot-0 tag failure (the key doesn't match); throws on a later slot's
 * tag failure (slot 0 authenticated, so the key is right — the envelope
 * itself is tampered or corrupt). No writes happen in this pass.
 *
 * Full coverage here is the security invariant, not an optimisation
 * choice: the caller (C# service) wipes the existing pool between
 * preflight and commit, so anything preflight does not authenticate
 * could otherwise fail in commit AFTER the user's data is gone. A
 * crafted envelope that authenticates slot 0 but corrupts slot k>0
 * must be rejected here, while the existing disk is still intact.
 */
export async function importDiskStreamPreflight(
    blob: Blob,
    kWrap: Uint8Array,
): Promise<DiskImportResultCode> {
    const reader = new BufferedStreamReader(blob.stream().getReader());
    try {
        await consumeEnvelopeMetadata(reader);
        const fileCount = await readArrayHeader(reader);
        assertImportFileCount(fileCount, 'importDiskStreamed[preflight]');
        for (let i = 0; i < fileCount; i++) {
            const tupleLen = await readArrayHeader(reader);
            if (tupleLen !== 2) {
                throw new Error(
                    `importDiskStreamed[preflight]: EncryptedDiskFile must be array(2), got array(${tupleLen})`);
            }
            const name = await readStr(reader);
            const binLen = await readBinHeader(reader);
            if (binLen === 0 || binLen % PHYSICAL_SLOT_SIZE !== 0) {
                throw new Error(
                    `importDiskStreamed[preflight]: file '${name}' length ${binLen} is not a positive multiple of slot size ${PHYSICAL_SLOT_SIZE}`);
            }
            const dbPath = `/databases/${name}`;
            const totalSlots = binLen / PHYSICAL_SLOT_SIZE;
            for (let slotIdx = 0; slotIdx < totalSlots; slotIdx++) {
                const slot = await reader.read(PHYSICAL_SLOT_SIZE);
                const aad = buildPageAad(dbPath, slotIdx);
                try {
                    const plaintext = decryptSlot(slot, kWrap, aad);
                    clearBytes(plaintext);
                } catch {
                    // Only the very first slot of the envelope says anything
                    // about the key. Once any slot has authenticated, the key
                    // is right and a later failure means the envelope itself
                    // is tampered or corrupt.
                    if (i === 0 && slotIdx === 0) {
                        return DiskImportResult.WRONG_KEY;
                    }
                    throw new Error(
                        `importDiskStreamed[preflight]: file '${name}' slot ${slotIdx} failed AEAD ` +
                        `authentication although earlier slots verified — envelope is tampered ` +
                        `or corrupt; refusing import (existing disk untouched).`);
                } finally {
                    clearBytes(slot);
                }
            }
        }
        return DiskImportResult.OK;
    } finally {
        reader.releaseLock();
    }
}

/**
 * Pass 2 — commit. Re-streams the envelope and for each file decrypts
 * every slot under K_wrap + re-encrypts under <paramref name="globalKey"/>,
 * writing each rekeyed slot batch via `writeFileSlice` to a temp slot in
 * the SAH pool. Temp → final promotion is deferred until EVERY file has
 * been staged, so a mid-envelope failure unlinks the temps and modifies
 * no final dbPath. JS heap peak per file is one chunk (~1 MB) regardless
 * of DB size.
 *
 * Caller (C# service) must have already wiped the pool and registered
 * <paramref name="globalKey"/> as the worker's globalKey before calling
 * this — typically via WipePoolAsync + EnterEncryptedAsync.
 */
export async function importDiskStreamCommit(
    blob: Blob,
    kWrap: Uint8Array,
    globalKey: Uint8Array,
    poolUtil: PoolUtilLike,
): Promise<void> {
    // 256 physical slots ≈ 1 MB rekey buffer per chunk write to the temp
    // SAH. Matches the constant in encryptDatabaseInPlace.
    const COMMIT_CHUNK_SLOTS = 256;
    const reader = new BufferedStreamReader(blob.stream().getReader());
    const tempPaths: string[] = [];
    // Deferred promotion list: temp → final renames happen only after
    // every file in the envelope has been fully decrypted, rekeyed and
    // written. A failure anywhere before that point unlinks the temps
    // and leaves no final dbPath modified.
    const pendingPromotions: Array<{ tempPath: string; dbPath: string; name: string }> = [];
    try {
        await consumeEnvelopeMetadata(reader);
        const fileCount = await readArrayHeader(reader);
        assertImportFileCount(fileCount, 'importDiskStreamed[commit]');
        for (let i = 0; i < fileCount; i++) {
            const tupleLen = await readArrayHeader(reader);
            if (tupleLen !== 2) {
                throw new Error(
                    `importDiskStreamed[commit]: EncryptedDiskFile must be array(2), got array(${tupleLen})`);
            }
            const name = await readStr(reader);
            const binLen = await readBinHeader(reader);
            if (binLen === 0 || binLen % PHYSICAL_SLOT_SIZE !== 0) {
                throw new Error(
                    `importDiskStreamed[commit]: file '${name}' length ${binLen} is not a positive multiple of slot size ${PHYSICAL_SLOT_SIZE}`);
            }
            const dbPath = `/databases/${name}`;
            const tempPath = `${dbPath}.import-tmp`;
            if (poolUtil.getFileNames().includes(tempPath)) {
                try { poolUtil.unlink(tempPath); } catch { /* best-effort */ }
            }
            tempPaths.push(tempPath);
            const totalSlots = binLen / PHYSICAL_SLOT_SIZE;
            let chunkBuf: Uint8Array | null = null;
            for (let slotBase = 0; slotBase < totalSlots; slotBase += COMMIT_CHUNK_SLOTS) {
                const slotCount = Math.min(COMMIT_CHUNK_SLOTS, totalSlots - slotBase);
                chunkBuf = new Uint8Array(slotCount * PHYSICAL_SLOT_SIZE);
                try {
                    for (let s = 0; s < slotCount; s++) {
                        const slot = await reader.read(PHYSICAL_SLOT_SIZE);
                        const slotIdx = slotBase + s;
                        const aad = buildPageAad(dbPath, slotIdx);
                        const plaintext = decryptSlot(slot, kWrap, aad);
                        try {
                            writeEncryptedSlot(plaintext, globalKey, aad, chunkBuf, s * PHYSICAL_SLOT_SIZE);
                        } finally {
                            clearBytes(plaintext);
                            clearBytes(slot);
                        }
                    }
                    poolUtil.writeFileSlice(tempPath, slotBase * PHYSICAL_SLOT_SIZE, chunkBuf);
                } finally {
                    clearBytes(chunkBuf);
                    chunkBuf = null;
                }
            }
            pendingPromotions.push({ tempPath, dbPath, name });
        }
        // Every file decrypted, rekeyed and staged — promote them all.
        // Doing this only after the full envelope has been processed means
        // a mid-envelope failure (truncation, AEAD, write error) never
        // leaves the pool with a partial mix of old and new DBs.
        for (const { tempPath, dbPath, name } of pendingPromotions) {
            poolUtil.atomicReplaceFile(tempPath, dbPath);
        }
    } catch (error) {
        // Unlink any staged temp that wasn't promoted. Promotion runs as
        // the last step, so a pre-promotion failure cleans up everything;
        // a failure inside the promotion loop itself (rename error) keeps
        // already-promoted dbPaths and unlinks the rest.
        for (const tempPath of tempPaths) {
            try { poolUtil.unlink(tempPath); } catch { /* best-effort */ }
        }
        throw error;
    } finally {
        reader.releaseLock();
    }
}
