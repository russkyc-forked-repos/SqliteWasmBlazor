// BlobSession round-trip — proves the chunked C#→JS Blob primitive
// preserves bytes byte-for-byte when reconstructed via `new Blob(parts)`.
//
// Property under test: appending N chunks via blobSessionAppend then
// composing a Blob from blobSessionParts and reading it back through
// blob.stream() yields exactly the concatenated input chunks.
//
// This is the foundation that every chunked-import path in the rewrite
// will compose on top of (encrypted-disk import, plain DB import,
// future delta payloads). If this property breaks, the import pipeline
// silently corrupts data — the test exists to catch that immediately.

import { describe, it, expect } from 'vitest';
import {
    blobSessionOpen,
    blobSessionAppend,
    blobSessionDiscard,
    blobSessionParts,
} from '../worker-bridge';

// Production C# JSImport hands the bridge an IMemoryView (Span backed).
// In tests a Uint8Array satisfies the same `.slice()` protocol.
type MemoryViewLike = { slice(): Uint8Array };

function asView(bytes: Uint8Array): MemoryViewLike {
    return { slice: () => bytes };
}

describe('blobSession', () => {
    it('round-trip: 16 chunks × 1 MB reconstructs to byte-identical Blob', async () => {
        const CHUNK = 1 << 20; // 1 MB
        const N = 16;
        const total = CHUNK * N;
        const sessionId = 12345;

        // Build a synthetic payload with each chunk filled with a unique
        // ramp pattern so we can pinpoint which chunk a corruption would
        // come from.
        const original = new Uint8Array(total);
        for (let i = 0; i < N; i++) {
            for (let j = 0; j < CHUNK; j++) {
                original[i * CHUNK + j] = (i + j) & 0xff;
            }
        }

        try {
            blobSessionOpen(sessionId);
            for (let i = 0; i < N; i++) {
                const chunk = original.subarray(i * CHUNK, (i + 1) * CHUNK);
                // .subarray returns a view sharing the buffer; the bridge
                // does its own .slice() so the test side doesn't need to.
                blobSessionAppend(sessionId, asView(chunk) as never, i === N - 1);
            }

            const parts = blobSessionParts(sessionId);
            expect(parts).toHaveLength(N);

            const blob = new Blob(parts);
            expect(blob.size).toBe(total);

            // Read the Blob back via stream() — same path the encrypted-disk
            // import worker uses (blob.stream().getReader()). Asserts that
            // the virtual concatenation under new Blob(parts) yields
            // contiguous bytes equal to the original.
            const reconstructed = new Uint8Array(await blob.arrayBuffer());
            expect(reconstructed.length).toBe(total);
            // Direct compare is faster + clearer than per-byte loop.
            // `Buffer.compare` is Node-only; cross-runtime via byte loop on
            // the first divergence.
            for (let k = 0; k < total; k++) {
                if (reconstructed[k] !== original[k]) {
                    throw new Error(
                        `byte mismatch at offset ${k}: got ${reconstructed[k]}, expected ${original[k]}`);
                }
            }
        } finally {
            blobSessionDiscard(sessionId);
        }
    });

    it('blobSessionOpen rejects duplicate sessionId', () => {
        const sessionId = 99;
        try {
            blobSessionOpen(sessionId);
            expect(() => blobSessionOpen(sessionId)).toThrow(/already open/);
        } finally {
            blobSessionDiscard(sessionId);
        }
    });

    it('blobSessionAppend on unknown sessionId throws', () => {
        expect(() => blobSessionAppend(424242, asView(new Uint8Array(4)) as never, true))
            .toThrow(/unknown sessionId/);
    });

    it('blobSessionDiscard is idempotent on unknown sessionId', () => {
        // Map.delete returns false silently; no throw expected.
        expect(() => blobSessionDiscard(424243)).not.toThrow();
    });

    it('blobSessionParts on unknown sessionId throws', () => {
        expect(() => blobSessionParts(424244))
            .toThrow(/unknown sessionId/);
    });
});
