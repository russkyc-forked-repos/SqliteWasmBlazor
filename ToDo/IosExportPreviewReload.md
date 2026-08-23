# The export preview restarts the app on iOS

In an installed iOS web app a finished export arrives as a full-screen OS
document preview. That part is settled and correct — see
`feedback_ios_web_app_export_presentation` for the two wrong turns taken in
August 2026 and why the shipped answer was one localized caption, not an API.

What is *not* settled is the cost of dismissing that preview: the app restarts.
The user has to close the preview after saving the file, and closing it brings
back a cold boot — the Blazor runtime reloads and an encrypted pool has to be
unlocked again through the passkey.

## Why it restarts

Every export shape funnels through one function:

`src/Base/SqliteWasmBlazor/TypeScript-Common/src/staged-download.ts` →
`triggerDownload(filename, content)` — an anchor click on a `blob:` object URL,
shared by plane 1's `.db`, plane 2's `.dbs`/`.eds`, and both bridges.

In a Home Screen web app there is no downloads folder, so WebKit ignores the
`download` attribute and **navigates the top-level browsing context** to the
`blob:` href instead. The preview is not an overlay: it *is* the new top-level
document. Dismissing it back-navigates to the app URL, and a WASM app does not
come back from bfcache after that.

So the preview is platform behaviour, but the restart is a consequence of which
browsing context the navigation happens in — and that is the part nobody has
tested yet.

## The idea to try

Navigate a **child** browsing context instead of the top-level one, so the
app's document is never torn down. Two shapes:

| Shape | How | Risk |
| --- | --- | --- |
| Off-screen `iframe` | `frame.src = objectUrl` on a 1×1 off-screen frame; the frame and the object URL must outlive the call (the preview drains the File through them), so they are released when the next export arms | WebKit may silently no-op an unrenderable subframe navigation — the export would appear to do nothing |
| Auxiliary window | `window.open()` during the tap, hold the handle, set `location` when the bytes exist | An export runs for seconds, so transient activation is long gone unless the window is opened up front; iOS may route `_blank` to Safari proper, trading the restart for an app switch |

Neither removes the restart in every case: after a ~1 GB export iOS may jettison
the web app's process whatever frame navigated.

An iframe navigation also carries no `download` attribute, so the name offered
on "Save to Files" would come from the blob URL rather than from `filename` —
undoing what `downloadContentType` exists to protect. If a device run shows the
app surviving but the filename wrong, the next step is a service-worker URL with
a `Content-Disposition` header, not a third presentation mode in
`staged-download.ts`.

## What the first attempt got wrong

A working version of the iframe path existed on 2026-08-23 (dispatch inside
`triggerDownload`, mode read from a localStorage key, demo-local `MudSwitch` to
flip it, stubbed-DOM vitest coverage for the dispatch; solution build clean, 43
base + 43 crypto vitest green) and was reverted. The mechanism was not the
problem — how the experiment was carried was:

- the flag was a raw localStorage key named independently in the library's TS
  and in the demo's code-behind, which is a contract with no owner;
- the demo switch was unlocalized and demo-local, so the one surface that can
  answer the question is also the one surface that never ships;
- reading the flag needed an `IJSInProcessRuntime` cast in a page, which is not
  how anything else in this repo reaches the browser.

Next time, decide first where a presentation mode belongs — a service seam on
the download surface, or nothing at all and a straight switch of the mechanism
after one device run.

## The other branch: make the restart cheap

If the subframe experiment fails, the restart is not the end of it. What it
actually costs today is worth knowing:

- the staging file survives (cleanup is `sweepExportStaging()` on the next
  worker init);
- the route survives (back-navigation restores the URL);
- what is lost is the WASM boot and the unlock — the PRF-derived key lives in
  worker memory by design and must never be persisted.

A marker written before an export could let the next boot raise the unlock
ceremony straight away, so the user lands on Face ID instead of on a locked page
with a button to press.

## Verification

Device-only, like the rest of this area: vitest and Playwright cannot see any of
it. A stubbed-DOM unit test can prove the dispatch (that the flag is read, that
the anchor path is untouched by default, that the subframe path does not revoke
the object URL the preview still has to drain) — nothing more.
