// staged-download.ts
//
// Main-thread counterpart to export-staging.ts: lift a finished staging file
// as a disk-backed File and hand it to the browser as an anchor download.
// Shared by both bridges (plane 1's plain .db export, plane 2's .db/.dbs/.eds
// exports) so the staging-directory name and the download content type have
// exactly one definition.
//
// The content type is not cosmetic. A File lifted from OPFS carries the type
// the engine infers from the *staging* file's name, and iOS Safari appends
// the extension its download manager prefers for that type whenever it
// disagrees with the `download` attribute: a plain export requested as
// `TodoDb-<stamp>.db` arrived as `TodoDb-<stamp>.db.bin`. Re-typing the blob
// from the target filename is what keeps the requested name intact — the
// `.eds` envelope never showed the bug because it is composed as a fresh Blob
// with a type no platform registry maps to an extension.

import { EXPORT_STAGING_DIR } from './export-staging';

// Extension → content type for every shape this library downloads. None of
// these types map to a preferred filename extension in the platform
// registries, so no download manager rewrites the name we ask for.
const CONTENT_TYPES = new Map<string, string>([
    ['db', 'application/x-sqlite3'],
    ['dbs', 'application/x-msgpack'],
    ['eds', 'application/x-msgpack'],
]);

/**
 * Content type to publish a download under, keyed by the target filename's
 * extension. Unregistered extensions throw rather than fall back to
 * `application/octet-stream` — that fallback is precisely what makes WebKit
 * append `.bin`, and a silent rename is worse than a failed export.
 */
export function downloadContentType(filename: string): string {
    const dot = filename.lastIndexOf('.');
    const extension = dot === -1 ? '' : filename.slice(dot + 1).toLowerCase();
    const contentType = CONTENT_TYPES.get(extension);
    if (contentType === undefined) {
        throw new Error(
            `download: no content type registered for extension of '${filename}'`);
    }
    return contentType;
}

/** Lift a finished staging file as a disk-backed File object. */
export async function stagedExportFile(name: string): Promise<File> {
    const root = await navigator.storage.getDirectory();
    const dir = await root.getDirectoryHandle(EXPORT_STAGING_DIR);
    const fileHandle = await dir.getFileHandle(name);
    return fileHandle.getFile();
}

/**
 * Best-effort staging deletion — only safe once the file's bytes are fully
 * materialised elsewhere (the test-only bytes path). Download paths must NOT
 * call this: the anchor download drains the File lazily and deleting the OPFS
 * entry underneath it would corrupt the download. Their staging files are
 * collected by the worker's init sweep next session.
 */
export async function deleteStagedExportFile(name: string): Promise<void> {
    try {
        const root = await navigator.storage.getDirectory();
        const dir = await root.getDirectoryHandle(EXPORT_STAGING_DIR);
        await dir.removeEntry(name);
    } catch {
        // Sweep on next worker init collects it.
    }
}

/**
 * Anchor-click download of `content` under `filename`. The blob is re-typed
 * from the filename (see module header); `slice` keeps the OPFS backing, so
 * a disk-backed File stays disk-backed and the bytes never land in
 * main-thread memory.
 */
export function triggerDownload(filename: string, content: Blob): void {
    const typed = content.slice(0, content.size, downloadContentType(filename));
    const url = URL.createObjectURL(typed);
    try {
        const link = document.createElement('a');
        link.href = url;
        link.download = filename;
        link.style.display = 'none';
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
    } finally {
        URL.revokeObjectURL(url);
    }
}

/**
 * JSImport entry shared by both bridges — anchor-click download of a finished
 * staging file by name. The File handed to the object URL is backed by the
 * OPFS entry, so the export bytes never occupy main-thread memory (Blobs
 * constructed from ArrayBuffers are memory-backed in WebKit — the failure
 * mode the staged path exists to avoid on mobile Safari).
 */
export async function downloadStagedExport(
    stagingFile: string,
    filename: string,
): Promise<boolean> {
    triggerDownload(filename, await stagedExportFile(stagingFile));
    return true;
}
