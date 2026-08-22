// Recovery for pool access handles the platform closed underneath us.
//
// A SAH can be closed without warning: the OS reclaims the storage layer
// under memory pressure from anything on the device, a backgrounded tab is
// suspended, storage is evicted. Every call against a dead handle then throws
// InvalidStateError, the operation's own rollback included.
//
// Only single-header operations are wrapped — rename, unlink, replace. Those
// either happened or did not, so a retry is a retry rather than a second half
// of a torn write. Reads and chunked body writes are deliberately not
// wrapped: their source stream is already consumed, and their temp slot is
// discarded by the caller's own rollback.

import {logger} from './sqlite-logger.js';
import {MODULE_NAME, openDatabases, poolUtil, pragmasSet} from './worker-state.js';

/** The shape a closed access handle surfaces as. */
export function isClosedHandleError(error: unknown): boolean {
    return error instanceof DOMException && error.name === 'InvalidStateError';
}

/**
 * Re-acquire the pool's access handles.
 *
 * Open databases go first: a connection whose SAH is dead cannot close
 * cleanly, so each close is best-effort and the cache entry is dropped either
 * way. C# reopens on demand — its open-set mirror is an optimisation, never
 * the authority. The pool rebuilds its path mapping from the slot headers, so
 * what it knows afterwards is what is actually on disk.
 */
export async function recoverPoolAccessHandles(): Promise<void> {
    if (!poolUtil) {
        throw new Error('SQLite not initialized');
    }
    for (const [dbName, db] of [...openDatabases.entries()]) {
        try {
            db.close();
        } catch (err) {
            logger.warn(MODULE_NAME, `close during handle recovery failed for ${dbName}:`, err);
        }
        openDatabases.delete(dbName);
        pragmasSet.delete(dbName);
    }
    await poolUtil.recoverAccessHandles();
    logger.warn(
        MODULE_NAME,
        `Re-acquired pool access handles after the platform closed them; ` +
        `${poolUtil.listDatabases().length} database(s) in the pool.`);
}

/**
 * Run a pool metadata operation, and if the platform has closed the access
 * handles, re-acquire them and run it once more. Anything that is not a
 * closed-handle error propagates untouched.
 */
export async function withHandleRecovery<T>(
    what: string,
    op: () => T,
    recover: () => Promise<void> = recoverPoolAccessHandles,
): Promise<T> {
    try {
        return op();
    } catch (error) {
        if (!isClosedHandleError(error)) {
            throw error;
        }
        logger.warn(
            MODULE_NAME,
            `${what}: pool access handles were closed by the platform — re-acquiring`,
            error);
        await recover();
        return op();
    }
}
