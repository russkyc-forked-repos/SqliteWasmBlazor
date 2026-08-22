// pool-naming.test.ts
//
// The sweep decides what a session that died mid-import left behind, and a
// wrong decision costs the user a database: dropping a park whose database
// is gone deletes the only copy of it. These cases pin the decision table.

import {describe, expect, it} from 'vitest';

import {
    IMPORT_PARK_SUFFIX,
    databaseNameForPark,
    importParkFor,
    isImportPark,
    isTempSlot,
    planPoolSweep,
} from '../pool-naming.js';

describe('park names', () => {
    it('round-trips a database name', () => {
        expect(databaseNameForPark(importParkFor('TodoDb.db'))).toBe('TodoDb.db');
    });

    it('mirrors the C# suffix', () => {
        expect(IMPORT_PARK_SUFFIX).toBe('.import-park');
        expect(importParkFor('TodoDb.db')).toBe('TodoDb.db.import-park');
    });

    it('does not treat a database as a park', () => {
        expect(isImportPark('TodoDb.db')).toBe(false);
        expect(() => databaseNameForPark('TodoDb.db')).toThrow(/not a park name/);
    });
});

describe('temp slots', () => {
    it('recognises every chunked-write suffix', () => {
        for (const name of [
            'TodoDb.db.encrypt-tmp',
            'TodoDb.db.decrypt-tmp',
            'TodoDb.db.single-import-tmp',
            'TodoDb.db.multi-import-tmp',
            'TodoDb.db.import-tmp',
        ]) {
            expect(isTempSlot(name)).toBe(true);
        }
    });

    it('leaves databases and parks alone', () => {
        expect(isTempSlot('TodoDb.db')).toBe(false);
        expect(isTempSlot('TodoDb.db.import-park')).toBe(false);
    });
});

describe('planPoolSweep', () => {
    it('does nothing to a settled pool', () => {
        expect(planPoolSweep(['TodoDb.db', 'NotesDb.db'])).toEqual([]);
    });

    it('restores a park whose database is gone', () => {
        expect(planPoolSweep(['TodoDb.db.import-park', 'NotesDb.db'])).toEqual([
            {kind: 'restore', park: 'TodoDb.db.import-park', database: 'TodoDb.db'},
        ]);
    });

    it('leaves a park whose database is present — either could be the data', () => {
        expect(planPoolSweep(['TodoDb.db', 'TodoDb.db.import-park'])).toEqual([]);
    });

    it('never drops a park', () => {
        const actions = planPoolSweep([
            'TodoDb.db', 'TodoDb.db.import-park', 'NotesDb.db.import-park',
        ]);
        expect(actions.every(action => action.kind !== 'drop')).toBe(true);
    });

    it('drops unfinished writes', () => {
        expect(planPoolSweep(['TodoDb.db', 'TodoDb.db.single-import-tmp'])).toEqual([
            {kind: 'drop', name: 'TodoDb.db.single-import-tmp'},
        ]);
    });

    it('restores the park and drops the temp the same failure left', () => {
        // What the iOS import failure leaves: the park it took, and the
        // temp slot its rollback could not unlink.
        expect(planPoolSweep([
            'TodoDb.db.import-park',
            'TodoDb.db.single-import-tmp',
            'NotesDb.db',
        ])).toEqual([
            {kind: 'restore', park: 'TodoDb.db.import-park', database: 'TodoDb.db'},
            {kind: 'drop', name: 'TodoDb.db.single-import-tmp'},
        ]);
    });

    it('restores only one park per database', () => {
        // Not a shape the pool can hold twice, but the plan must not emit
        // two restores onto one name if it ever does.
        const actions = planPoolSweep([
            'TodoDb.db.import-park', 'TodoDb.db.import-park',
        ]);
        expect(actions).toHaveLength(1);
    });
});
