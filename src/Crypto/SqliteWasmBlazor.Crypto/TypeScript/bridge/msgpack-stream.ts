// Header-only MessagePack packers for streaming envelope composition.
//
// The chunked encrypted-disk export emits per-DB ciphertext as standalone
// JS Blobs (browser disk-backs them on Safari). We assemble the final
// envelope by concatenating MessagePack header bytes with the per-DB
// Blobs via `new Blob([headerBytes, ...blobParts, ...])` — the resulting
// Blob is a virtual concatenation, the bytes never live as one buffer in
// JS heap.
//
// MessagePack-CSharp deserialises [Key(N)] positional records as fixed-
// length arrays, so we need just enough header emission to write that
// outer array shape:
//
//   array(8) ‖ uint(version) ‖ str(aadVer) ‖ bin(prfSalt) ‖
//   str(ephPub) ‖ str(wrapCt) ‖ str(wrapNonce) ‖ str(credIdHint) ‖
//   array(N) ‖ [ array(2) ‖ str(name) ‖ bin(N) ‖ <blob ciphertext> ] × N
//
// These helpers cover the four header types that appear in that shape;
// the variable-length bytes (string body, salt body, blob body) are
// emitted by the caller (or referenced as Blob parts).
//
// Wire format reference: https://github.com/msgpack/msgpack/blob/master/spec.md

/** Encode <paramref name="n"/> as an unsigned integer (positive fixint / uint8/16/32/64). */
export function packUint(n: number): Uint8Array<ArrayBuffer> {
    if (!Number.isInteger(n) || n < 0) {
        throw new Error(`packUint: expected non-negative integer, got ${n}`);
    }
    if (n <= 0x7f) {
        return Uint8Array.of(n);
    }
    if (n <= 0xff) {
        return Uint8Array.of(0xcc, n);
    }
    if (n <= 0xffff) {
        return Uint8Array.of(0xcd, (n >>> 8) & 0xff, n & 0xff);
    }
    if (n <= 0xffffffff) {
        return Uint8Array.of(
            0xce,
            (n >>> 24) & 0xff,
            (n >>> 16) & 0xff,
            (n >>> 8) & 0xff,
            n & 0xff,
        );
    }
    // 64-bit: encode as uint64. Use BigInt to avoid JS Number precision loss
    // above 2^53. The envelope sizes we care about fit in uint32, but the
    // primitive should still reject silently-truncated values.
    throw new Error(`packUint: values above 2^32 require uint64 encoding (got ${n})`);
}

/** Emit a MessagePack array header for <paramref name="n"/> elements. */
export function packArrayHeader(n: number): Uint8Array<ArrayBuffer> {
    if (!Number.isInteger(n) || n < 0) {
        throw new Error(`packArrayHeader: expected non-negative integer, got ${n}`);
    }
    if (n <= 15) {
        return Uint8Array.of(0x90 | n);
    }
    if (n <= 0xffff) {
        return Uint8Array.of(0xdc, (n >>> 8) & 0xff, n & 0xff);
    }
    if (n <= 0xffffffff) {
        return Uint8Array.of(
            0xdd,
            (n >>> 24) & 0xff,
            (n >>> 16) & 0xff,
            (n >>> 8) & 0xff,
            n & 0xff,
        );
    }
    throw new Error(`packArrayHeader: too many elements (${n})`);
}

/**
 * Emit a MessagePack bin header for a payload of <paramref name="byteLength"/>
 * bytes. The body bytes follow as a separate Uint8Array / Blob part; this
 * function emits only the type byte + length prefix.
 */
export function packBinHeader(byteLength: number): Uint8Array<ArrayBuffer> {
    if (!Number.isInteger(byteLength) || byteLength < 0) {
        throw new Error(`packBinHeader: expected non-negative integer, got ${byteLength}`);
    }
    if (byteLength <= 0xff) {
        return Uint8Array.of(0xc4, byteLength);
    }
    if (byteLength <= 0xffff) {
        return Uint8Array.of(0xc5, (byteLength >>> 8) & 0xff, byteLength & 0xff);
    }
    if (byteLength <= 0xffffffff) {
        return Uint8Array.of(
            0xc6,
            (byteLength >>> 24) & 0xff,
            (byteLength >>> 16) & 0xff,
            (byteLength >>> 8) & 0xff,
            byteLength & 0xff,
        );
    }
    throw new Error(`packBinHeader: payload too large (${byteLength})`);
}

/**
 * Encode <paramref name="s"/> as MessagePack str (UTF-8 bytes). Returns a
 * two-element tuple of [header, utf8 body] — caller concatenates them
 * (with spread `...packStr(s)`) into the BlobPart list.
 */
export function packStr(s: string): [Uint8Array<ArrayBuffer>, Uint8Array<ArrayBuffer>] {
    const body = new TextEncoder().encode(s);
    const len = body.length;
    let header: Uint8Array<ArrayBuffer>;
    if (len <= 31) {
        header = Uint8Array.of(0xa0 | len);
    } else if (len <= 0xff) {
        header = Uint8Array.of(0xd9, len);
    } else if (len <= 0xffff) {
        header = Uint8Array.of(0xda, (len >>> 8) & 0xff, len & 0xff);
    } else if (len <= 0xffffffff) {
        header = Uint8Array.of(
            0xdb,
            (len >>> 24) & 0xff,
            (len >>> 16) & 0xff,
            (len >>> 8) & 0xff,
            len & 0xff,
        );
    } else {
        throw new Error(`packStr: utf-8 body too large (${len})`);
    }
    return [header, body];
}
