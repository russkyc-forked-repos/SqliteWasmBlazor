// Regression tests for the streaming .eds import's wipe-after-validate
// invariant. The caller (C# service) wipes the existing pool between
// preflight and commit, so preflight MUST authenticate the entire
// envelope — a crafted file that authenticates slot 0 but corrupts a
// later slot, or a bogus Files count that skips the verify loop, must
// be rejected while the existing disk is still intact.
//
// Everything here runs on synthetic envelopes built from the same pack
// helpers the exporter uses — no OPFS/SQLite needed. Blob.stream() is
// available in the Node test runtime.

import { describe, it, expect } from 'vitest';
import { encryptChaCha20Poly1305 } from '@sqlitewasmblazor/crypto-core';
import {
    packArrayHeader,
    packBinHeader,
    packStr,
    packUint,
} from '@sqlitewasmblazor/worker-common';
import { buildPageAad } from '../aad.js';
import {
    importPoolStreamPreflight,
    importPoolStreamCommit,
} from '../import-streamed.js';
import {PoolImportResult, MAX_IMPORT_FILES} from '@sqlitewasmblazor/worker-common';

const SECTOR_SIZE = 4096;
const PAGE_NONCE_LEN = 12;
const PHYSICAL_SLOT_SIZE = 4124;

function makeKey(seed: number): Uint8Array {
    const k = new Uint8Array(32);
    for (let i = 0; i < 32; i++) k[i] = (seed + i) & 0xff;
    return k;
}

/** Seal one 4096-byte plaintext page into a 4124-byte physical slot. */
function sealSlot(
    plaintext: Uint8Array,
    key: Uint8Array,
    dbPath: string,
    slotIndex: number,
): Uint8Array {
    const enc = encryptChaCha20Poly1305(plaintext, key, buildPageAad(dbPath, slotIndex));
    const slot = new Uint8Array(PHYSICAL_SLOT_SIZE);
    slot.set(enc.ciphertext.subarray(0, SECTOR_SIZE), 0);
    slot.set(enc.nonce, SECTOR_SIZE);
    slot.set(enc.ciphertext.subarray(SECTOR_SIZE), SECTOR_SIZE + PAGE_NONCE_LEN);
    return slot;
}

/** Compose a v3 EncryptedPoolEnvelope from already-sealed slot bytes. */
function buildEnvelope(
    files: Array<{ name: string; slots: Uint8Array[] }>,
    filesHeaderOverride?: Uint8Array,
): Blob {
    const parts: Uint8Array[] = [
        packArrayHeader(8),
        packUint(3),                            // Version
        ...packStr('v1'),                       // AadVersion
        packBinHeader(32), new Uint8Array(32),  // PrfSalt
        ...packStr('ephPub'),                   // EphemeralPublicKey
        ...packStr('wrapCt'),                   // WrappedContentKeyCiphertext
        ...packStr('wrapNonce'),                // WrappedContentKeyNonce
        ...packStr('credIdHint'),               // CredentialIdHint
        filesHeaderOverride ?? packArrayHeader(files.length),
    ];
    for (const f of files) {
        const body = new Uint8Array(f.slots.length * PHYSICAL_SLOT_SIZE);
        f.slots.forEach((s, i) => body.set(s, i * PHYSICAL_SLOT_SIZE));
        parts.push(packArrayHeader(2), ...packStr(f.name), packBinHeader(body.length), body);
    }
    return new Blob(parts as BlobPart[]);
}

function makeFile(
    name: string,
    slotCount: number,
    key: Uint8Array,
): { name: string; slots: Uint8Array[] } {
    const dbPath = `/databases/${name}`;
    const slots: Uint8Array[] = [];
    for (let i = 0; i < slotCount; i++) {
        const page = new Uint8Array(SECTOR_SIZE).fill((i + 1) & 0xff);
        slots.push(sealSlot(page, key, dbPath, i));
    }
    return { name, slots };
}

describe('importPoolStreamPreflight (full-envelope verification)', () => {
    it('returns OK for a fully valid multi-file envelope', async () => {
        const kWrap = makeKey(1);
        const blob = buildEnvelope([
            makeFile('a.db', 3, kWrap),
            makeFile('b.db', 2, kWrap),
        ]);
        await expect(importPoolStreamPreflight(blob, kWrap))
            .resolves.toBe(PoolImportResult.OK);
    });

    it('returns WRONG_KEY when the very first slot fails to authenticate', async () => {
        const kWrap = makeKey(1);
        const blob = buildEnvelope([makeFile('a.db', 2, kWrap)]);
        await expect(importPoolStreamPreflight(blob, makeKey(2)))
            .resolves.toBe(PoolImportResult.WRONG_KEY);
    });

    it('throws (not WRONG_KEY, not OK) when a non-zero slot is corrupted', async () => {
        // The vuln-1 regression: slot 0 authenticates, slot 1 is flipped.
        // The old slot-0-only preflight returned OK here and the caller
        // wiped the existing disk before commit discovered the corruption.
        const kWrap = makeKey(1);
        const file = makeFile('a.db', 3, kWrap);
        file.slots[1][100] ^= 0x01;
        const blob = buildEnvelope([file]);
        await expect(importPoolStreamPreflight(blob, kWrap))
            .rejects.toThrow(/tampered or corrupt/);
    });

    it('throws when a later file is corrupted even if its slot 0 fails', async () => {
        // Slot 0 of file 2 failing is tamper evidence, not key evidence —
        // file 1 already authenticated under the same key.
        const kWrap = makeKey(1);
        const good = makeFile('a.db', 1, kWrap);
        const bad = makeFile('b.db', 1, kWrap);
        bad.slots[0][0] ^= 0x01;
        const blob = buildEnvelope([good, bad]);
        await expect(importPoolStreamPreflight(blob, kWrap))
            .rejects.toThrow(/tampered or corrupt/);
    });

    it('throws on a truncated envelope (premature EOF) instead of OK', async () => {
        const kWrap = makeKey(1);
        const full = buildEnvelope([makeFile('a.db', 3, kWrap)]);
        const truncated = full.slice(0, full.size - PHYSICAL_SLOT_SIZE);
        await expect(importPoolStreamPreflight(truncated, kWrap))
            .rejects.toThrow(/stream ended/);
    });

    it('rejects an empty Files array', async () => {
        const blob = buildEnvelope([]);
        await expect(importPoolStreamPreflight(blob, makeKey(1)))
            .rejects.toThrow(/Files count 0/);
    });

    it('rejects a crafted array32 count with bit 31 set (vuln-2 regression)', async () => {
        // 0xdd ff ff ff ff — pre-fix this decoded to -1, the verify loop
        // never ran, and preflight returned OK with nothing authenticated.
        const bogusHeader = Uint8Array.of(0xdd, 0xff, 0xff, 0xff, 0xff);
        const blob = buildEnvelope([], bogusHeader);
        await expect(importPoolStreamPreflight(blob, makeKey(1)))
            .rejects.toThrow(/Files count 4294967295/);
    });

    it(`rejects counts above MAX_IMPORT_FILES (${MAX_IMPORT_FILES})`, async () => {
        const header = packArrayHeader(MAX_IMPORT_FILES + 1);
        const blob = buildEnvelope([], header);
        await expect(importPoolStreamPreflight(blob, makeKey(1)))
            .rejects.toThrow(/refusing import/);
    });
});

describe('importPoolStreamCommit (deferred promotion)', () => {
    interface Call { op: string; args: unknown[] }

    function makePoolUtil(calls: Call[]) {
        return {
            listDatabases: () => [] as string[],
            getFileNames: () => [] as string[],
            importDb: () => undefined,
            writeFileSlice: (name: string, offset: number, bytes: Uint8Array) => {
                calls.push({ op: 'writeFileSlice', args: [name, offset, bytes.length] });
            },
            atomicReplaceFile: (src: string, dst: string) => {
                calls.push({ op: 'atomicReplaceFile', args: [src, dst] });
                return true as const;
            },
            unlink: (name: string) => {
                calls.push({ op: 'unlink', args: [name] });
                return true;
            },
        };
    }

    it('promotes files only after the whole envelope has been staged', async () => {
        const kWrap = makeKey(1);
        const globalKey = makeKey(9);
        const calls: Call[] = [];
        const blob = buildEnvelope([
            makeFile('a.db', 2, kWrap),
            makeFile('b.db', 1, kWrap),
        ]);
        await importPoolStreamCommit(blob, kWrap, globalKey, makePoolUtil(calls));
        const firstPromote = calls.findIndex(c => c.op === 'atomicReplaceFile');
        const lastWrite = calls.map(c => c.op).lastIndexOf('writeFileSlice');
        expect(firstPromote).toBeGreaterThan(lastWrite);
        expect(calls.filter(c => c.op === 'atomicReplaceFile').map(c => c.args[1]))
            .toEqual(['/databases/a.db', '/databases/b.db']);
    });

    it('promotes nothing when a later file fails AEAD mid-commit', async () => {
        // Defense in depth below preflight: even if commit sees a bad
        // slot, no final dbPath may have been replaced.
        const kWrap = makeKey(1);
        const globalKey = makeKey(9);
        const calls: Call[] = [];
        const good = makeFile('a.db', 1, kWrap);
        const bad = makeFile('b.db', 2, kWrap);
        bad.slots[1][0] ^= 0x01;
        const blob = buildEnvelope([good, bad]);
        await expect(
            importPoolStreamCommit(blob, kWrap, globalKey, makePoolUtil(calls)),
        ).rejects.toThrow();
        expect(calls.some(c => c.op === 'atomicReplaceFile')).toBe(false);
        // Both temps cleaned up.
        expect(calls.filter(c => c.op === 'unlink').map(c => c.args[0]))
            .toEqual(expect.arrayContaining([
                '/databases/a.db.import-tmp',
                '/databases/b.db.import-tmp',
            ]));
    });
});
