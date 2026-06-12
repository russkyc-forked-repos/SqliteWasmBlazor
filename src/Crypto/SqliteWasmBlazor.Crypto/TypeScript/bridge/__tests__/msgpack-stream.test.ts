// Round-trip via msgpackr's `unpack` to confirm the header-only packers
// produce bytes compatible with MessagePack-CSharp's positional [Key(N)]
// decoder. Without this, an envelope-shape mismatch would silently
// corrupt every export.

import { describe, it, expect } from 'vitest';
import { unpack } from 'msgpackr';
import {
    packArrayHeader,
    packBinHeader,
    packStr,
    packUint,
    BufferedStreamReader,
    readArrayHeader,
    readBinHeader,
    readStr,
} from '../msgpack-stream';

function concat(parts: Uint8Array[]): Uint8Array {
    const total = parts.reduce((n, p) => n + p.length, 0);
    const out = new Uint8Array(total);
    let off = 0;
    for (const p of parts) {
        out.set(p, off);
        off += p.length;
    }
    return out;
}

describe('msgpack-stream packers', () => {
    it('packUint encodes 0..2^32 boundaries correctly', () => {
        for (const n of [0, 1, 127, 128, 255, 256, 65535, 65536, 4294967295]) {
            const bytes = packUint(n);
            expect(unpack(bytes)).toBe(n);
        }
    });

    it('packArrayHeader followed by elements round-trips through unpack', () => {
        const parts: Uint8Array[] = [
            packArrayHeader(3),
            packUint(1),
            packUint(2),
            packUint(3),
        ];
        expect(unpack(concat(parts))).toEqual([1, 2, 3]);
    });

    it('packStr round-trips short and long strings', () => {
        for (const s of ['', 'abc', 'x'.repeat(30), 'x'.repeat(50), 'x'.repeat(400)]) {
            const bytes = concat([...packStr(s)]);
            expect(unpack(bytes)).toBe(s);
        }
    });

    it('packBinHeader followed by body bytes round-trips', () => {
        for (const len of [0, 1, 32, 255, 256, 4096, 65536]) {
            const body = new Uint8Array(len);
            for (let i = 0; i < len; i++) {
                body[i] = i & 0xff;
            }
            const bytes = concat([packBinHeader(len), body]);
            const decoded = unpack(bytes);
            // msgpackr decodes bin as Uint8Array
            expect(decoded).toBeInstanceOf(Uint8Array);
            expect(decoded.length).toBe(len);
            for (let i = 0; i < len; i++) {
                if (decoded[i] !== body[i]) {
                    throw new Error(`bin byte mismatch at offset ${i}: ${decoded[i]} vs ${body[i]}`);
                }
            }
        }
    });

    it('composed envelope shape (array of 8) matches MessagePack-CSharp [Key(N)] decoder expectations', () => {
        const prfSalt = new Uint8Array(32);
        prfSalt[0] = 0xaa; prfSalt[31] = 0xbb;
        const file0Body = new Uint8Array([1, 2, 3, 4]);

        const parts: Uint8Array[] = [
            packArrayHeader(8),
            packUint(3),                    // version
            ...packStr('v1'),               // aadVersion
            packBinHeader(prfSalt.length), prfSalt, // prfSalt
            ...packStr('ephPubB64=='),      // ephemeralPublicKey
            ...packStr('wrapCtB64=='),      // wrappedContentKeyCiphertext
            ...packStr('wrapNonceB64'),     // wrappedContentKeyNonce
            ...packStr('credIdHintB64'),    // credentialIdHint
            packArrayHeader(1),             // files
            packArrayHeader(2),             //   one EncryptedDiskFile
            ...packStr('TodoDb.db'),        //     name
            packBinHeader(file0Body.length), file0Body, // bytes
        ];
        const wire = concat(parts);
        const decoded = unpack(wire) as unknown[];

        expect(decoded.length).toBe(8);
        expect(decoded[0]).toBe(3);
        expect(decoded[1]).toBe('v1');
        expect((decoded[2] as Uint8Array).length).toBe(32);
        expect((decoded[2] as Uint8Array)[0]).toBe(0xaa);
        expect((decoded[2] as Uint8Array)[31]).toBe(0xbb);
        expect(decoded[3]).toBe('ephPubB64==');
        expect(decoded[4]).toBe('wrapCtB64==');
        expect(decoded[5]).toBe('wrapNonceB64');
        expect(decoded[6]).toBe('credIdHintB64');
        const files = decoded[7] as unknown[];
        expect(files.length).toBe(1);
        const file0 = files[0] as unknown[];
        expect(file0[0]).toBe('TodoDb.db');
        expect((file0[1] as Uint8Array).length).toBe(4);
    });
});

// The stream readers decode lengths from UNTRUSTED import files. A 32-bit
// length with bit 31 set must come back as a large positive number, never
// a negative one — a negative array count silently skips count-driven
// loops downstream (the import-preflight bypass), and negative bin/str
// lengths relied on incidental downstream checks.
describe('msgpack-stream readers: 32-bit lengths are unsigned', () => {
    function readerFor(bytes: Uint8Array<ArrayBuffer>): BufferedStreamReader {
        return new BufferedStreamReader(new Blob([bytes]).stream().getReader());
    }

    it('readArrayHeader decodes array32 with bit 31 set as positive', async () => {
        const r = readerFor(Uint8Array.of(0xdd, 0xff, 0xff, 0xff, 0xff));
        await expect(readArrayHeader(r)).resolves.toBe(4294967295);
        const r2 = readerFor(Uint8Array.of(0xdd, 0x80, 0x00, 0x00, 0x00));
        await expect(readArrayHeader(r2)).resolves.toBe(2147483648);
    });

    it('readBinHeader decodes bin32 with bit 31 set as positive', async () => {
        const r = readerFor(Uint8Array.of(0xc6, 0x80, 0x00, 0x00, 0x01));
        await expect(readBinHeader(r)).resolves.toBe(2147483649);
    });

    it('readStr treats a bit-31 str32 length as a huge read, not a negative one', async () => {
        // The decoded length is internal to readStr; the observable signal
        // is the failure mode of reading ~2 GiB from a 0-byte stream: EOF
        // (or allocation failure), never the read(n < 0) guard that the
        // signed decode used to hit.
        const r = readerFor(Uint8Array.of(0xdb, 0x80, 0x00, 0x00, 0x00));
        const err = await readStr(r).then(
            () => { throw new Error('readStr unexpectedly resolved'); },
            (e: unknown) => e,
        );
        expect(String(err)).not.toMatch(/must be >= 0/);
        expect(String(err)).toMatch(/stream ended|allocat|Invalid|length/i);
    });

    it('readArrayHeader still decodes uncontroversial 32-bit lengths', async () => {
        const r = readerFor(Uint8Array.of(0xdd, 0x00, 0x01, 0x00, 0x00));
        await expect(readArrayHeader(r)).resolves.toBe(65536);
    });
});
