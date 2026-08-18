// export-staging.ts
//
// OPFS staging for streamed exports. The worker writes export bytes into a
// staging file through a synchronous access handle — the same primitive the
// import path uses for rekey-on-write — and the bridge lifts the finished
// file as a File object for delivery. A File backed by an OPFS entry is
// disk-backed in every engine; Blobs constructed from ArrayBuffers are held
// in process memory by WebKit, which is what OOM-killed iPhone Safari on
// large encrypted exports.
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
    const dir = await stagingDir(true);
    const name = `export-${Date.now()}-${stagingSerial++}.bin`;
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
