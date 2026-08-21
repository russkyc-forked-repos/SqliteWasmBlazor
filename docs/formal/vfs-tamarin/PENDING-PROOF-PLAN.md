# Interactive-proof plan — CLOSED, all three resolved

> **Historical.** All three statements this plan targeted are resolved; no
> hand-guided session is needed. Kept for the reasoning, which explains why
> the phrasings below diverge and what replaced them. The plan text after
> this section is the original and was never executed.

Resolution:

| original | outcome |
|---|---|
| `pending_encrypt_in_place_no_live_key` | replaced in `76d3a34` by the weaker, provable `encrypt_in_place_no_live_key` |
| `pending_export_encrypt_no_live_key` | replaced in `76d3a34` by the weaker, provable `export_encrypt_no_live_key` |
| `pending_decrypt_no_clear_before_use` | replaced by `no_claim_held_after_clear`, verified in 12 steps — **stronger**, not weaker |

The third is the interesting one. Its divergence was never the inductive
descent this plan assumed. The premise carried `path`, `cipher_blob` and
`plain_blob`, which sends the backward search down
`PendingDecryptOp -> BeginDecryptInPlace -> the file-state claim`; the
`deprioFiles` tactic matches `FileState|FileVer` and never the pending ops,
so ranking could not help. Restated over `ClaimKeyState` — no file variables,
and covering all seven rules that claim a held key rather than
`CommitDecryptInPlace` alone — it closes in 12 steps and 0.33 s.

Measured as diverging past ten minutes each, before the file-free restatement:
`deprioFiles` alone; a tactic additionally deprioritising `PendingDecryptOp` /
`PendingEncryptOp`; the key-side lemmas injected as `[reuse]`; and the
file-free form without its two helper rungs.

Note also that `76d3a34` dropped the `pending_` prefix from
`decrypt_no_clear_before_use` without weakening the statement. `verify.sh`
skips `pending_*`, so that single rename put an unprovable lemma into the
gate and left it red from 2026-06-30 until the restatement.

---

## Original plan (not executed)

Mechanics (GUI, remote box, RTS flags) live in `HOWTO.md` §4–§6; this file is
only *what to do once the GUI is open*.

> Do not run the prover on the dev Mac (memory pressure → thrash). Use the
> rented-box recipe in HOWTO §6, then tunnel the interactive port (HOWTO §4).

## What is actually open

All three are the **strong per-key forms** whose closing step is an induction
over the key timeline that diverges under every built-in heuristic,
`use_induction`, budget bounding, and the `deprioFiles` tactic (campaign
`a23715a..HEAD`). The weaker forms are already machine-verified and are the
ladder these proofs climb:

| pending | already-proven sibling to lean on |
|---|---|
| `pending_decrypt_no_clear_before_use` | the `not (Ex clear before use)` conjunct of `export_plain_key_live_since_install` (verified, 12 steps, `[heuristic={deprioFiles}]`) |
| `pending_encrypt_in_place_no_live_key` | `encrypt_in_place_keyslot_emptied_since_install` (verified) |
| `pending_export_encrypt_no_live_key` | `export_encrypt_keyslot_emptied_since_install` (verified) |

Supporting invariants available as `[reuse]`/lemmas:
`empty_event_is_init_or_clear`, `key_installed_once`, `clear_requires_install`,
`key_event_rooted_at_init`.

The gap between sibling and pending is always the same: the sibling proves
"*some* key was emptied/cleared in the window"; the pending form asserts "*THE*
installed key `k` was cleared in the window". Bridging *some → the* is the
inductive descent that loops.

## Order of attack (easiest first)

### 1. `pending_decrypt_no_clear_before_use` — try this first

This is structurally the **negative half already proven** for export-plain
(`not (Ex #t2. GlobalKeyCleared(Device, k_old) @ #t2 & #t2 < #t)`), just for
`DecryptInPlaceAccepted` instead of `ExportPlainAccepted`. First move is the
cheapest possible: give it the **same annotation** that closed export-plain and
see if it falls out without manual stepping:

```
lemma pending_decrypt_no_clear_before_use [heuristic={deprioFiles}]:
```

If that still loops, step it in the GUI exactly like `export_plain`: the proof
contradicts a `GlobalKeyCleared(k_old)` before the use against
`decrypt_key_installed_before_use` (install precedes use) + `clear_requires_install`
+ `key_installed_once`. Watch the constraint store; the FileState goals are the
fan-out, and `deprioFiles` should already be sinking them.

### 2 & 3. `pending_{encrypt_in_place,export_encrypt}_no_live_key`

Same shape; prove one, mirror to the other (`export_encrypt` carries the
`deprioFiles` heuristic on its sibling — start it there too).

Goal: from `GlobalKeyInstalled(Device, k) @ #t1 < #t`, show
`Ex #t2. GlobalKeyCleared(Device, k) @ #t2 & #t1 < #t2 < #t`.

Interactive recipe:

1. **Apply the sibling.** Instantiate `*_keyslot_emptied_since_install` to get
   `Ex #c. KeyState(Device,'empty','none') @ #c & #t1 < #c < #t` — an empty-slot
   event strictly inside the window.
2. **Resolve the empty event.** Apply `empty_event_is_init_or_clear` to `#c`:
   it is either `DeviceInitialized` or `Ex k'. GlobalKeyCleared(Device,k') @ #c`.
   Kill the init branch with `key_event_rooted_at_init` + `key_installed_once`
   (the unique init precedes the install at `#t1`, but `#t1 < #c`, contradiction).
3. **The hard step — identify the cleared key.** You now have
   `GlobalKeyCleared(Device, k') @ #c` for *some* `k'`. Need `k' = k`. This is
   where automation loops: the prover keeps the `k' = k` disequality open and
   explores both. Manually **case-split on `k' = k`**:
   - `k' = k`: done — `#c` is the witness.
   - `k' ≠ k`: derive a second install/clear pair for `k'` and descend. This is
     the induction; bound it by `key_installed_once` (each key installs once) +
     `clear_requires_install` (every clear has a prior install) so the
     "another key" chain is finite and the slot can only be re-occupied by `k`
     between `#t1` and `#t`.

The manual lever the heuristics lack: **rank goals on the matching key `k`
first**, deprioritize `GlobalKeyInstalled/Cleared` goals on *other* keys, and
keep `FileState|FileVer` sunk (as `deprioFiles` already does). In the GUI this
is just choosing which constraint to expand; the autoprover picks wrong.

## Promote the manual ranking to an oracle

If a stable goal ordering closes the proof interactively, codify it so the gate
can replay it unattended — two options, in order of preference:

1. **Refine the in-theory tactic.** Extend `deprioFiles` (or add a sibling
   tactic) with `prio`/`deprio` `regex` blocks that float `GlobalKeyCleared`/
   `GlobalKeyInstalled` on the goal key and sink the off-key ones. Annotate the
   three lemmas `[heuristic={...}]`. This keeps everything in the `.spthy`.
2. **External oracle script** (`--heuristic=O --oracle-name=...`): a small
   ranker reading goal lines on stdin, emitting the priority order observed in
   the GUI. Heavier (an extra file in the gate), use only if a pure tactic
   can't express the off-key deprioritization.

## Done criteria

- The three `lemma pending_*` either renamed off the `pending_` prefix (so
  `verify.sh`'s grep counts them) or kept-named but added to the gate, and
  `verify.sh` + `mutation-check.sh` still green end-to-end on the box.
- Whatever ranking/oracle closed them committed alongside, with the step counts
  recorded in `README.md`'s verification-status block (replace the "Open: …"
  paragraph).
- A mutation twin per newly-proven lemma (break the clear→install ordering) to
  prove it isn't vacuous — matching the existing `mutation-check.sh` discipline.
