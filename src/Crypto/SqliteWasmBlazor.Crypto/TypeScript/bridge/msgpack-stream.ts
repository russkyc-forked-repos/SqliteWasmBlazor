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

// ---------------------------------------------------------------------------
// Stream reader half: pulls MessagePack values out of a ReadableStream
// without buffering the whole stream in JS heap. Used by the chunked
// encrypted-disk import path to decode the envelope's positional record +
// per-file slot ciphertext directly from a Blob.stream().
// ---------------------------------------------------------------------------

/**
 * Pull-style consumer over a `ReadableStreamDefaultReader<Uint8Array>`.
 * Maintains a single buffered chunk and yields contiguous byte ranges on
 * demand via {@link read} (returns a fresh `Uint8Array` of exactly N bytes)
 * and {@link skip} (advances by N bytes without copying).
 *
 * The implementation never accumulates more than two chunks' worth of
 * bytes at a time — chunks are read on demand from the underlying stream,
 * concatenated only when a read straddles a chunk boundary, and dropped
 * once their content has been emitted.
 */
export class BufferedStreamReader {
    private readonly reader: ReadableStreamDefaultReader<Uint8Array>;
    private current: Uint8Array | null = null;
    private offset = 0;
    private done = false;

    constructor(reader: ReadableStreamDefaultReader<Uint8Array>) {
        this.reader = reader;
    }

    /** Release the underlying reader's lock on the stream. Call in `finally`. */
    releaseLock(): void {
        try { this.reader.releaseLock(); } catch { /* best-effort */ }
    }

    /**
     * Pull exactly <paramref name="n"/> bytes from the stream and return a
     * fresh contiguous `Uint8Array`. Throws on premature EOF.
     */
    async read(n: number): Promise<Uint8Array<ArrayBuffer>> {
        if (n < 0) {
            throw new Error(`BufferedStreamReader.read: n must be >= 0, got ${n}`);
        }
        const buf = new ArrayBuffer(n);
        const out = new Uint8Array(buf);
        let filled = 0;
        while (filled < n) {
            await this.ensureBuffer();
            if (this.current === null) {
                throw new Error(
                    `BufferedStreamReader.read: stream ended after ${filled} of ${n} bytes`);
            }
            const available = this.current.length - this.offset;
            const take = Math.min(available, n - filled);
            out.set(this.current.subarray(this.offset, this.offset + take), filled);
            this.offset += take;
            filled += take;
        }
        return out;
    }

    /**
     * Advance the stream cursor by <paramref name="n"/> bytes without
     * materialising them. Throws on premature EOF.
     */
    async skip(n: number): Promise<void> {
        if (n < 0) {
            throw new Error(`BufferedStreamReader.skip: n must be >= 0, got ${n}`);
        }
        let remaining = n;
        while (remaining > 0) {
            await this.ensureBuffer();
            if (this.current === null) {
                throw new Error(
                    `BufferedStreamReader.skip: stream ended with ${remaining} of ${n} bytes left`);
            }
            const available = this.current.length - this.offset;
            const take = Math.min(available, remaining);
            this.offset += take;
            remaining -= take;
        }
    }

    private async ensureBuffer(): Promise<void> {
        if (this.current !== null && this.offset < this.current.length) {
            return;
        }
        // Drop the consumed chunk before pulling the next so the previous
        // chunk's bytes are GC-able.
        this.current = null;
        this.offset = 0;
        if (this.done) {
            return;
        }
        const { value, done } = await this.reader.read();
        if (done) {
            this.done = true;
            return;
        }
        this.current = value as Uint8Array;
    }
}

/**
 * Read a MessagePack array header from <paramref name="reader"/> and
 * return the element count. Throws if the next byte is not an array
 * marker (fixarray / array16 / array32).
 */
export async function readArrayHeader(reader: BufferedStreamReader): Promise<number> {
    const head = (await reader.read(1))[0];
    if ((head & 0xf0) === 0x90) {
        return head & 0x0f;
    }
    if (head === 0xdc) {
        const lenBytes = await reader.read(2);
        return (lenBytes[0] << 8) | lenBytes[1];
    }
    if (head === 0xdd) {
        const lenBytes = await reader.read(4);
        return (lenBytes[0] << 24) | (lenBytes[1] << 16) | (lenBytes[2] << 8) | lenBytes[3];
    }
    throw new Error(`readArrayHeader: expected array marker, got 0x${head.toString(16)}`);
}

/**
 * Read a MessagePack bin header from <paramref name="reader"/> and
 * return the body length in bytes (the body bytes are consumed by
 * subsequent {@link BufferedStreamReader.read} / {@link BufferedStreamReader.skip}
 * calls).
 */
export async function readBinHeader(reader: BufferedStreamReader): Promise<number> {
    const head = (await reader.read(1))[0];
    if (head === 0xc4) {
        return (await reader.read(1))[0];
    }
    if (head === 0xc5) {
        const lenBytes = await reader.read(2);
        return (lenBytes[0] << 8) | lenBytes[1];
    }
    if (head === 0xc6) {
        const lenBytes = await reader.read(4);
        return (lenBytes[0] << 24) | (lenBytes[1] << 16) | (lenBytes[2] << 8) | lenBytes[3];
    }
    throw new Error(`readBinHeader: expected bin marker, got 0x${head.toString(16)}`);
}

/** Read a MessagePack str (fixstr / str8 / str16 / str32) and return its UTF-8-decoded value. */
export async function readStr(reader: BufferedStreamReader): Promise<string> {
    const head = (await reader.read(1))[0];
    let len: number;
    if ((head & 0xe0) === 0xa0) {
        len = head & 0x1f;
    } else if (head === 0xd9) {
        len = (await reader.read(1))[0];
    } else if (head === 0xda) {
        const lenBytes = await reader.read(2);
        len = (lenBytes[0] << 8) | lenBytes[1];
    } else if (head === 0xdb) {
        const lenBytes = await reader.read(4);
        len = (lenBytes[0] << 24) | (lenBytes[1] << 16) | (lenBytes[2] << 8) | lenBytes[3];
    } else {
        throw new Error(`readStr: expected str marker, got 0x${head.toString(16)}`);
    }
    if (len === 0) {
        return '';
    }
    const body = await reader.read(len);
    return new TextDecoder().decode(body);
}

/** Read a MessagePack uint (positive fixint / uint8/16/32). Throws on uint64 (out of safe-int range). */
export async function readUint(reader: BufferedStreamReader): Promise<number> {
    const head = (await reader.read(1))[0];
    if ((head & 0x80) === 0x00) {
        return head;
    }
    if (head === 0xcc) {
        return (await reader.read(1))[0];
    }
    if (head === 0xcd) {
        const bytes = await reader.read(2);
        return (bytes[0] << 8) | bytes[1];
    }
    if (head === 0xce) {
        const bytes = await reader.read(4);
        // Use unsigned right-shift trick to avoid sign extension above 2^31.
        return ((bytes[0] << 24) | (bytes[1] << 16) | (bytes[2] << 8) | bytes[3]) >>> 0;
    }
    throw new Error(`readUint: expected uint marker, got 0x${head.toString(16)}`);
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
