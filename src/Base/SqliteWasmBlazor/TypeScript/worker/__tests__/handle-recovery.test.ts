// withHandleRecovery had no test for the whole of its life: the branch it
// exists for only runs when the platform closes an access handle, which no
// suite reproduces. These drive it directly.

import {describe, expect, it, vi} from 'vitest';
import {isClosedHandleError, withHandleRecovery} from '@sqlitewasmblazor/worker-common';

const closedHandle = () => new DOMException('The access handle is closed.', 'InvalidStateError');

describe('isClosedHandleError', () => {
    it('recognises only InvalidStateError', () => {
        expect(isClosedHandleError(closedHandle())).toBe(true);
        expect(isClosedHandleError(new DOMException('nope', 'NotFoundError'))).toBe(false);
        expect(isClosedHandleError(new Error('InvalidStateError'))).toBe(false);
        expect(isClosedHandleError(undefined)).toBe(false);
    });
});

describe('withHandleRecovery', () => {
    it('does not recover when the operation succeeds', async () => {
        const recover = vi.fn(async () => {});
        const op = vi.fn(() => 'ok');

        await expect(withHandleRecovery('rename', op, recover)).resolves.toBe('ok');
        expect(op).toHaveBeenCalledTimes(1);
        expect(recover).not.toHaveBeenCalled();
    });

    it('recovers once and retries after a closed handle', async () => {
        const recover = vi.fn(async () => {});
        const op = vi.fn()
            .mockImplementationOnce(() => {
                throw closedHandle();
            })
            .mockImplementationOnce(() => 'second');

        await expect(withHandleRecovery('unlink', op, recover)).resolves.toBe('second');
        expect(recover).toHaveBeenCalledTimes(1);
        expect(op).toHaveBeenCalledTimes(2);
    });

    it('retries exactly once — a still-closed handle propagates', async () => {
        const recover = vi.fn(async () => {});
        const op = vi.fn(() => {
            throw closedHandle();
        });

        await expect(withHandleRecovery('replace', op, recover)).rejects.toThrow(DOMException);
        expect(recover).toHaveBeenCalledTimes(1);
        expect(op).toHaveBeenCalledTimes(2);
    });

    it('leaves any other error untouched and never recovers', async () => {
        const recover = vi.fn(async () => {});
        const op = vi.fn(() => {
            throw new Error('src not found');
        });

        await expect(withHandleRecovery('replace', op, recover)).rejects.toThrow('src not found');
        expect(recover).not.toHaveBeenCalled();
        expect(op).toHaveBeenCalledTimes(1);
    });
});
