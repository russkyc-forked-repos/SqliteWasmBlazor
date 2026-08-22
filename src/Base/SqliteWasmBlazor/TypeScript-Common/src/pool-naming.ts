// pool-naming.ts
//
// The pool entry names that are not databases. Two shapes:
//
//   *.import-park   the previous content of a database a validated import
//                   is replacing. Mirrors PoolNaming.ImportParkSuffix on
//                   the C# side, which owns the park/restore bookkeeping —
//                   the worker only knows the shape so it can put a park
//                   back when the session that made it never got to.
//
//   *-tmp           a chunked write in progress (encrypt/decrypt in place,
//                   single- or multi-DB import commit). Promoted over the
//                   real name by atomicReplaceFile, so an entry under one
//                   of these names is by definition an unfinished write.
//
// Both outlive their session only when a tab dies mid-flight or the
// platform closes the pool's access handles underneath a commit; the
// worker's init sweep is where they are collected.

/** Mirrors <c>PoolNaming.ImportParkSuffix</c>. */
export const IMPORT_PARK_SUFFIX = '.import-park';

/** Suffix of the temp slot a chunked plain→encrypted transition writes. */
export const ENCRYPT_TMP_SUFFIX = '.encrypt-tmp';

/** Suffix of the temp slot a chunked encrypted→plain transition writes. */
export const DECRYPT_TMP_SUFFIX = '.decrypt-tmp';

/** Suffix of the temp slot a single-DB import commit writes. */
export const SINGLE_IMPORT_TMP_SUFFIX = '.single-import-tmp';

/** Suffix of the temp slot a multi-DB (.dbs) import commit writes. */
export const MULTI_IMPORT_TMP_SUFFIX = '.multi-import-tmp';

/** Suffix of the temp slot a whole-pool (.eds) import commit writes. */
export const POOL_IMPORT_TMP_SUFFIX = '.import-tmp';

const TEMP_SLOT_SUFFIXES = [
    ENCRYPT_TMP_SUFFIX,
    DECRYPT_TMP_SUFFIX,
    SINGLE_IMPORT_TMP_SUFFIX,
    MULTI_IMPORT_TMP_SUFFIX,
    POOL_IMPORT_TMP_SUFFIX,
] as const;

/** True when <paramref name="name"/> is a parked database copy. */
export function isImportPark(name: string): boolean {
    return name.endsWith(IMPORT_PARK_SUFFIX);
}

/** The park name for <paramref name="name"/>. */
export function importParkFor(name: string): string {
    return `${name}${IMPORT_PARK_SUFFIX}`;
}

/** The database name a park belongs to. Throws on a non-park name. */
export function databaseNameForPark(park: string): string {
    if (!isImportPark(park)) {
        throw new Error(`databaseNameForPark: '${park}' is not a park name`);
    }
    return park.slice(0, park.length - IMPORT_PARK_SUFFIX.length);
}

/** True when <paramref name="name"/> is an unfinished chunked write. */
export function isTempSlot(name: string): boolean {
    return TEMP_SLOT_SUFFIXES.some(suffix => name.endsWith(suffix));
}

/** What the init sweep does to one entry. */
export type PoolSweepAction =
    | { kind: 'restore'; park: string; database: string }
    | { kind: 'drop'; name: string };

/**
 * What a session that died mid-flight left behind, and what to do with it.
 *
 * A park whose database is absent is that database — the restore that
 * would have put it back never ran, and nothing else holds those bytes —
 * so it goes back under its own name. A park whose database is present is
 * left alone: the names cannot say whether it outlived a finished import
 * or a rollback that got half-way, and the next import's parking pass
 * settles it. Parks are never dropped here.
 *
 * A temp slot is dropped. Its content only ever becomes a database through
 * atomicReplaceFile, inside the one operation that wrote it; left in place
 * it lists as a database nothing can open.
 */
export function planPoolSweep(names: readonly string[]): PoolSweepAction[] {
    const present = new Set(names.filter(name => !isImportPark(name)));
    const actions: PoolSweepAction[] = [];
    for (const name of names) {
        if (isImportPark(name)) {
            const database = databaseNameForPark(name);
            if (!present.has(database)) {
                present.add(database);
                actions.push({kind: 'restore', park: name, database});
            }
            continue;
        }
        if (isTempSlot(name)) {
            actions.push({kind: 'drop', name});
        }
    }
    return actions;
}
