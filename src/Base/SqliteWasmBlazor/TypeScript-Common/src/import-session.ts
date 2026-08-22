// Import sessions: the chunk pump C# pushes a picked file through.
//
// A session is opened, fed one transferred ArrayBuffer at a time, and
// closed. Nothing here knows about encryption — the one plane-specific
// decision, what a database sink does with the bytes, arrives as an
// injected opener. Plane 1 hands back a sink with no key; plane 2 hands
// back one that rekeys each batch under the registered global key.
//
// Two sink kinds, because the formats need different numbers of passes:
//
//   database → one plain `.db` going into one database. Single pass, so
//              the chunks go straight into the pool's temp slot and no
//              copy of the file exists anywhere.
//   staging  → a `.dbs` or `.eds` envelope, which is validated in one
//              pass and committed in another. It lands in an OPFS staging
//              file the worker re-streams per pass.

import {clearBytes} from './memory.js';
import type {DatabaseImportSink} from './import-sink.js';
import {
    openImportStaging,
    readStagingFile,
    type ExportStagingFile,
} from './export-staging.js';

/**
 * A sink plus whatever the opener had to capture to build it. {@link dispose}
 * releases that capture — on plane 2 it wipes a snapshot of the global key,
 * which the session outlives and a lock can clear underneath it.
 */
export interface DatabaseSinkHandle {
    sink: DatabaseImportSink;

    /**
     * Release the opener's captured state. Must be idempotent: C# discards
     * from a finally-block, so a committed session is disposed by close and
     * then again by the discard that follows it.
     */
    dispose(): void;
}

export interface ImportSessionDeps {
    /**
     * Build the sink for a `database` session. The opener owns closing the
     * target first: the commit promotes a temp slot over the database via
     * atomicReplaceFile, and an OFile still holding the old SAH would keep
     * serving stale pages — and write into a slot the pool can hand to the
     * next file.
     */
    openDatabaseSink(dbName: string, plainSize: number): Promise<DatabaseSinkHandle>;

    /** Called after a database session commits, for the worker's own log. */
    onDatabaseCommitted?(dbName: string): void;
}

type ImportSession =
    | { kind: 'database'; dbName: string; handle: DatabaseSinkHandle }
    | { kind: 'staging'; staging: ExportStagingFile; finished: boolean };

export interface ImportSessionHost {
    open(sessionId: number, sink: string, dbName: string | undefined, size: number | undefined): Promise<void>;

    append(sessionId: number, chunk: Uint8Array): void;

    close(sessionId: number): void;

    discard(sessionId: number): Promise<void>;

    /**
     * The staged envelope of a session, as a File the import passes can
     * stream. Each pass lifts its own File — a stream is one-shot, the OPFS
     * entry behind it is not.
     */
    stagedFile(sessionId: number, what: string): Promise<File>;
}

export function createImportSessionHost(deps: ImportSessionDeps): ImportSessionHost {
    const sessions = new Map<number, ImportSession>();

    function take(sessionId: number, what: string): ImportSession {
        const session = sessions.get(sessionId);
        if (!session) {
            throw new Error(`${what}: no open import session ${sessionId}`);
        }
        return session;
    }

    return {
        async open(sessionId, sink, dbName, size) {
            if (sessions.has(sessionId)) {
                throw new Error(`importSessionOpen: session ${sessionId} is already open`);
            }

            if (sink === 'staging') {
                sessions.set(sessionId, {
                    kind: 'staging',
                    staging: await openImportStaging(),
                    finished: false,
                });
                return;
            }

            if (sink !== 'database') {
                throw new Error(`importSessionOpen: unknown sink '${sink}'`);
            }
            if (typeof dbName !== 'string' || dbName.length === 0) {
                throw new Error('importSessionOpen: a database sink needs data.database');
            }
            if (typeof size !== 'number') {
                throw new Error('importSessionOpen: a database sink needs data.size');
            }

            const handle = await deps.openDatabaseSink(dbName, size);
            sessions.set(sessionId, {kind: 'database', dbName, handle});
        },

        // The incoming buffer was transferred, so this worker owns it; it is
        // wiped once written because a plain `.db` chunk is plaintext pages
        // and a `.dbs`/`.eds` chunk can be either.
        append(sessionId, chunk) {
            const session = take(sessionId, 'importSessionAppend');
            try {
                if (session.kind === 'database') {
                    session.handle.sink.append(chunk);
                } else {
                    session.staging.write(chunk);
                }
            } finally {
                clearBytes(chunk);
            }
        },

        // End the source. A database session promotes its temp slot here —
        // that is the import. A staging session only closes the write handle;
        // what happens to the envelope is the pass that reads it back.
        close(sessionId) {
            const session = take(sessionId, 'importSessionClose');
            if (session.kind === 'staging') {
                session.staging.finish();
                session.finished = true;
                return;
            }
            try {
                session.handle.sink.commit();
            } finally {
                session.handle.dispose();
            }
            deps.onDatabaseCommitted?.(session.dbName);
        },

        // Drop a session and everything it staged. Idempotent — C# calls it
        // from a finally-block whether the import committed, failed, or never
        // started.
        async discard(sessionId) {
            const session = sessions.get(sessionId);
            if (!session) {
                return;
            }
            sessions.delete(sessionId);
            if (session.kind === 'database') {
                try {
                    session.handle.sink.abort();
                } finally {
                    session.handle.dispose();
                }
            } else {
                await session.staging.abort();
            }
        },

        async stagedFile(sessionId, what) {
            const session = take(sessionId, what);
            if (session.kind !== 'staging') {
                throw new Error(`${what}: session ${sessionId} is not a staging session`);
            }
            if (!session.finished) {
                throw new Error(`${what}: session ${sessionId} is still open for writing`);
            }
            return readStagingFile(session.staging.name);
        },
    };
}
