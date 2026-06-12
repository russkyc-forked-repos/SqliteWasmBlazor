// Soft backpressure for worker → main streaming exports.
//
// The export loops read OPFS slices and postMessage them as transferable
// chunks. postMessage is fire-and-forget: if the main thread stalls (heavy
// render), transferred buffers pile up in its event queue. This gate bounds
// that pile to STREAM_CREDIT_WINDOW chunks: the bridge acks every chunk it
// has consumed (wrapped into a Blob part), and the producer awaits room in
// the window before sending the next one.
//
// Failure containment: if no ack arrives within ACK_WAIT_TIMEOUT_MS the
// gate DEGRADES to the historical unthrottled behavior instead of
// deadlocking the export — a stale bridge bundle that predates the ack
// protocol must not break exports, it just loses the backpressure bound.

export const STREAM_CREDIT_WINDOW = 8;
export const ACK_WAIT_TIMEOUT_MS = 10_000;

export class StreamCreditGate {
    private sent = 0;
    private acked = 0;
    private degraded = false;
    private waiter: ((acked: boolean) => void) | null = null;

    /**
     * Await room in the unacked window, then claim the next sequence
     * number. Call once per chunk, embed the returned seq in the message.
     */
    async beforeSend(): Promise<number> {
        while (!this.degraded && this.sent - this.acked >= STREAM_CREDIT_WINDOW) {
            const gotAck = await new Promise<boolean>(resolve => {
                const timer = setTimeout(() => {
                    this.waiter = null;
                    resolve(false);
                }, ACK_WAIT_TIMEOUT_MS);
                this.waiter = (acked: boolean) => {
                    clearTimeout(timer);
                    resolve(acked);
                };
            });
            if (!gotAck) {
                this.degraded = true;
                console.warn(
                    '[stream-credit] no chunk ack within ' +
                    `${ACK_WAIT_TIMEOUT_MS}ms — receiver predates the ack ` +
                    'protocol? Degrading to unthrottled streaming.');
            }
        }
        this.sent += 1;
        return this.sent;
    }

    /** Record the receiver's cumulative ack (highest consumed seq). */
    onAck(seq: number): void {
        if (typeof seq === 'number' && seq > this.acked) {
            this.acked = seq;
        }
        if (this.waiter !== null) {
            const w = this.waiter;
            this.waiter = null;
            w(true);
        }
    }

    /** Test/diagnostic visibility. */
    get unacked(): number { return this.sent - this.acked; }
    get isDegraded(): boolean { return this.degraded; }
}
