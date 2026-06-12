import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import {
    StreamCreditGate,
    STREAM_CREDIT_WINDOW,
    ACK_WAIT_TIMEOUT_MS,
} from '../stream-credit';

describe('StreamCreditGate', () => {
    beforeEach(() => { vi.useFakeTimers(); });
    afterEach(() => { vi.useRealTimers(); });

    it('lets a full window through without any acks, numbered 1..N', async () => {
        const gate = new StreamCreditGate();
        const seqs: number[] = [];
        for (let i = 0; i < STREAM_CREDIT_WINDOW; i++) {
            seqs.push(await gate.beforeSend());
        }
        expect(seqs).toEqual(Array.from({ length: STREAM_CREDIT_WINDOW }, (_, i) => i + 1));
        expect(gate.unacked).toBe(STREAM_CREDIT_WINDOW);
        expect(gate.isDegraded).toBe(false);
    });

    it('blocks the window+1th send until an ack arrives', async () => {
        const gate = new StreamCreditGate();
        for (let i = 0; i < STREAM_CREDIT_WINDOW; i++) { await gate.beforeSend(); }

        let resolved = false;
        const pending = gate.beforeSend().then(seq => { resolved = true; return seq; });
        await vi.advanceTimersByTimeAsync(0);
        expect(resolved).toBe(false);

        gate.onAck(1);
        const seq = await pending;
        expect(resolved).toBe(true);
        expect(seq).toBe(STREAM_CREDIT_WINDOW + 1);
    });

    it('cumulative ack opens the window by the acked amount', async () => {
        const gate = new StreamCreditGate();
        for (let i = 0; i < STREAM_CREDIT_WINDOW; i++) { await gate.beforeSend(); }
        gate.onAck(5);
        expect(gate.unacked).toBe(STREAM_CREDIT_WINDOW - 5);
        // 5 more sends fit without blocking
        for (let i = 0; i < 5; i++) { await gate.beforeSend(); }
        expect(gate.unacked).toBe(STREAM_CREDIT_WINDOW);
    });

    it('stale (lower) acks never move the window backwards', async () => {
        const gate = new StreamCreditGate();
        for (let i = 0; i < 4; i++) { await gate.beforeSend(); }
        gate.onAck(3);
        gate.onAck(1); // stale — out-of-order delivery
        expect(gate.unacked).toBe(1);
    });

    it('degrades to unthrottled after the ack timeout instead of deadlocking', async () => {
        const gate = new StreamCreditGate();
        for (let i = 0; i < STREAM_CREDIT_WINDOW; i++) { await gate.beforeSend(); }

        const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
        const pending = gate.beforeSend();
        await vi.advanceTimersByTimeAsync(ACK_WAIT_TIMEOUT_MS + 1);
        const seq = await pending;
        expect(seq).toBe(STREAM_CREDIT_WINDOW + 1);
        expect(gate.isDegraded).toBe(true);
        expect(warn).toHaveBeenCalledOnce();

        // Once degraded, further sends never block again.
        for (let i = 0; i < 3 * STREAM_CREDIT_WINDOW; i++) { await gate.beforeSend(); }
        expect(gate.isDegraded).toBe(true);
        warn.mockRestore();
    });
});
