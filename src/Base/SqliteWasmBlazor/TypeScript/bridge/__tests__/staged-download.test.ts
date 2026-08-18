// Download content type — the value that decides whether the browser keeps
// the filename we ask for.
//
// A File lifted from the OPFS staging entry carries the type inferred from
// the *staging* name (`export-<ts>-<n>.bin`), and iOS Safari appends the
// extension its download manager prefers for that type when it disagrees
// with the `download` attribute: a plain export requested as
// `TodoDb-<stamp>.db` arrived as `TodoDb-<stamp>.db.bin`. Every type here
// must therefore stay unmapped in the platform registries — a type whose
// preferred extension is anything but the one in the filename brings the
// rename back.

import { describe, it, expect } from 'vitest';
import { downloadContentType } from '@sqlitewasmblazor/worker-common';

describe('downloadContentType', () => {
    it('maps every shape this library exports', () => {
        expect(downloadContentType('TodoDb-20260818-193737.db'))
            .toBe('application/x-sqlite3');
        expect(downloadContentType('databases-20260818-193737.dbs'))
            .toBe('application/x-msgpack');
        expect(downloadContentType('disk-backup-20260818-193737.eds'))
            .toBe('application/x-msgpack');
    });

    it('never answers application/octet-stream — that is what appends .bin', () => {
        for (const name of ['a.db', 'a.dbs', 'a.eds']) {
            expect(downloadContentType(name)).not.toBe('application/octet-stream');
        }
    });

    it('is case-insensitive on the extension', () => {
        expect(downloadContentType('Backup.DB')).toBe('application/x-sqlite3');
    });

    it('throws on an unregistered extension instead of guessing', () => {
        expect(() => downloadContentType('notes.sqlite')).toThrow(/no content type/);
        expect(() => downloadContentType('extensionless')).toThrow(/no content type/);
    });
});
