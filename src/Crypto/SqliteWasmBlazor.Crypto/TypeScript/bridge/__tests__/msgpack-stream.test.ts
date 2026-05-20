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
