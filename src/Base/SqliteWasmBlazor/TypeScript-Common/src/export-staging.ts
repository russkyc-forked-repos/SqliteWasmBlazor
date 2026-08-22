// export-staging.ts
//
// OPFS staging for streamed transfers. The worker writes bytes into a staging
// file through a synchronous access handle and reads them back as a File
// object — for an export the bridge lifts that File for delivery, for a
// two-pass import the worker re-streams it per pass. A File backed by an
// OPFS entry is disk-backed in every engine; Blobs constructed from
// ArrayBuffers are held in process memory by WebKit, which is what
// OOM-killed iPhone Safari on large encrypted exports — and, until the
// import path was pushed into the worker, on large imports too.
//
// Lifecycle: staging files live in EXPORT_STAGING_DIR at the OPFS root,
// outside the SAHPool's /databases tree. A staging file must not be deleted
// while a download may still be draining it (the download reads the File
// lazily), so cleanup is deferred: sweepExportStaging() removes all
// leftovers on worker init, bounding retention to one session.

// Also read by staged-download.ts, which lifts finished staging files on
// the main thread.
export const EXPORT_STAGING_DIR = 'export-staging';

let stagingSerial = 0;

export interface ExportStagingFile {
    /** Staging file name inside EXPORT_STAGING_DIR. */
    readonly name: string;
    /** Append bytes at the current end of the staging file. */
    write(bytes: Uint8Array): void;
    /** Bytes written so far == offset the next write lands at. */
    position(): number;
    /** Flush and close the handle. The file is ready for the bridge. */
    finish(): void;
    /** Close and best-effort delete after a failed export. */
    abort(): Promise<void>;
}

async function stagingDir(create: boolean): Promise<FileSystemDirectoryHandle> {
    const root = await navigator.storage.getDirectory();
    return root.getDirectoryHandle(EXPORT_STAGING_DIR, { create });
}

export async function openExportStaging(): Promise<ExportStagingFile> {
    return openStagingFile('export');
}

/**
 * Staging file for an import the C# side pushes in chunk by chunk. Same
 * primitive, other direction: the envelope lands here first so the
 * two-pass flows (`.dbs`, `.eds`) can read it twice without the whole
 * thing ever being a Blob in main-thread memory. Single-pass imports do
 * not come through here — they go straight into the pool's temp slot.
 */
export async function openImportStaging(): Promise<ExportStagingFile> {
    return openStagingFile('import');
}

async function openStagingFile(prefix: string): Promise<ExportStagingFile> {
    const dir = await stagingDir(true);
    const name = `${prefix}-${Date.now()}-${stagingSerial++}.bin`;
    const fileHandle = await dir.getFileHandle(name, { create: true });
    const sah = await fileHandle.createSyncAccessHandle();
    sah.truncate(0);
    let offset = 0;
    let open = true;
    return {
        name,
        write(bytes: Uint8Array): void {
            if (!open) {
                throw new Error('export staging: write after close');
            }
            const written = sah.write(bytes, { at: offset });
            if (written !== bytes.length) {
                throw new Error(
                    `export staging: short write ${written}/${bytes.length} at offset ${offset}`);
            }
            offset += written;
        },
        position(): number {
            return offset;
        },
        finish(): void {
            if (!open) {
                throw new Error('export staging: finish after close');
            }
            open = false;
            sah.flush();
            sah.close();
        },
        async abort(): Promise<void> {
            if (open) {
                open = false;
                try {
                    sah.close();
                } catch {
                    // Handle already invalidated — deletion below still applies.
                }
            }
            try {
                const dh = await stagingDir(false);
                await dh.removeEntry(name);
            } catch {
                // Best effort — sweepExportStaging() collects it next session.
            }
        },
    };
}

/**
 * Lift a finished staging file as a File, for a reader inside the worker.
 * Only valid after {@link ExportStagingFile.finish} — the access handle
 * holds the write side exclusively until then.
 */
export async function readStagingFile(name: string): Promise<File> {
    const dir = await stagingDir(false);
    const handle = await dir.getFileHandle(name);
    return handle.getFile();
}

/**
 * Remove every staging leftover from previous sessions. Called once on
 * worker init, before any export can open a new staging file.
 */
export async function sweepExportStaging(): Promise<void> {
    let dir: FileSystemDirectoryHandle;
    try {
        dir = await stagingDir(false);
    } catch {
        return; // nothing staged yet
    }
    const names: string[] = [];
    for await (const name of (dir as unknown as {
        keys(): AsyncIterable<string>;
    }).keys()) {
        names.push(name);
    }
    for (const name of names) {
        try {
            await dir.removeEntry(name);
        } catch {
            // A concurrent tab may still hold the file — retry next init.
        }
    }
}
