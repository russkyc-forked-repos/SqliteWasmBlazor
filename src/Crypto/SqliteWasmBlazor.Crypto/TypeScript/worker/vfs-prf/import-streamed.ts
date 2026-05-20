// Streaming import path for the asymmetric encrypted-disk envelope (v3).
//
// Consumes a Blob holding the full MessagePack EncryptedDiskEnvelope via
// blob.stream() — never materialises the whole envelope as a single buffer
// in the worker. Two passes (run separately from C#):
//
//   1. Preflight: walk the file table, AEAD-decrypt slot 0 of each file
//      under K_wrap with `prf-vfs-v1|{dbPath}|0` AAD. Tag failure means
//      WRONG_KEY; abort with no writes done.
//   2. Commit:    re-stream the envelope, write each file as a temp slot
//      via writeFileSlice chunk-by-chunk under globalKey, then
//      atomicReplaceFile to promote.
//
// Why two passes: preflight preserves the wipe-after-validate invariant
// the legacy whole-buffer import had. blob.stream() is re-callable on
// the same Blob, so we re-open between passes — the bytes never live
// concatenated in JS heap.

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
import { debugLog } from '../debug-log.js';

const SECTOR_SIZE = 4096;
const PAGE_NONCE_LEN = 12;
const PAGE_TAG_LEN = 16;
const PHYSICAL_SLOT_SIZE = SECTOR_SIZE + PAGE_NONCE_LEN + PAGE_TAG_LEN; // 4124

const ENVELOPE_VERSION = 3;
const ENVELOPE_ARRAY_LEN = 8;
const ENVELOPE_AAD_VERSION = 'v1';
const PRF_SALT_LEN = 32;

/**
 * Mirrors the C# `DiskImportResult` enum so callers can branch on the
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
 * Pass 1 — preflight. Walks the envelope's Files array, AEAD-verifies
 * slot 0 of each file under K_wrap. Returns OK if every file's slot 0
 * authenticates; WRONG_KEY on the first tag failure (no writes happen
 * in this pass). Caller (C# service) must hold off any pool-mutating
 * operation until this returns OK.
 */
export async function importDiskStreamPreflight(
    blob: Blob,
    kWrap: Uint8Array,
    traceOp?: string,
): Promise<DiskImportResultCode> {
    const reader = new BufferedStreamReader(blob.stream().getReader());
    try {
        await consumeEnvelopeMetadata(reader);
        const fileCount = await readArrayHeader(reader);
        if (traceOp) { debugLog(traceOp, 'preflight.files', { count: fileCount }); }
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
            if (traceOp) { debugLog(traceOp, 'preflight.file', { name, bytes: binLen }); }
            const slot0 = await reader.read(PHYSICAL_SLOT_SIZE);
            const dbPath = `/databases/${name}`;
            const aad = buildPageAad(dbPath, 0);
            try {
                const plaintext = decryptSlot(slot0, kWrap, aad);
                clearBytes(plaintext);
            } catch {
                return DiskImportResult.WRONG_KEY;
            } finally {
                clearBytes(slot0);
            }
            // Discard slots 1..N-1 of this file (preflight only checks
            // slot 0; tag-correctness on slot 0 is sufficient evidence the
            // K_wrap matches the file's encryption key — the file's
            // remaining slots share the same key by construction).
            await reader.skip(binLen - PHYSICAL_SLOT_SIZE);
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
 * the SAH pool, then atomic-promotes temp → final path. JS heap peak per
 * file is one chunk (~1 MB) regardless of DB size.
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
    traceOp?: string,
): Promise<void> {
    // 256 physical slots ≈ 1 MB rekey buffer per chunk write to the temp
    // SAH. Matches the constant in encryptDatabaseInPlace.
    const COMMIT_CHUNK_SLOTS = 256;
    const reader = new BufferedStreamReader(blob.stream().getReader());
    const tempPaths: string[] = [];
    try {
        await consumeEnvelopeMetadata(reader);
        const fileCount = await readArrayHeader(reader);
        if (traceOp) { debugLog(traceOp, 'commit.files', { count: fileCount }); }
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
            if (traceOp) { debugLog(traceOp, 'commit.file.start', { name, slots: totalSlots }); }
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
                    if (traceOp) { debugLog(traceOp, 'commit.chunk', { name, slotBase, slotCount }); }
                } finally {
                    clearBytes(chunkBuf);
                    chunkBuf = null;
                }
            }
            // Atomic-promote temp → dbPath. From this point on, dbPath
            // points at the freshly-imported encrypted DB; any later
            // file's failure leaves earlier files committed (same
            // window the legacy multi-file import had).
            if (traceOp) { debugLog(traceOp, 'commit.atomicReplace', { name }); }
            poolUtil.atomicReplaceFile(tempPath, dbPath);
            if (traceOp) { debugLog(traceOp, 'commit.file.done', { name }); }
        }
    } catch (error) {
        // Unlink any temp slot we created but didn't promote. Already-
        // promoted dbPaths are committed and stay.
        for (const tempPath of tempPaths) {
            try { poolUtil.unlink(tempPath); } catch { /* best-effort */ }
        }
        throw error;
    } finally {
        reader.releaseLock();
    }
}
