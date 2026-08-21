# PRF VFS Tamarin Model

This folder contains Tamarin models for the PRF-keyed VFS implementation in
`src/Crypto/SqliteWasmBlazor.Crypto/TypeScript/worker/vfs-prf`, its in-place
conversion wrapper, and the pool-level PRF cache/import lifecycle.

## Files

- `vfs.spthy` models the per-slot AEAD, global-key registration, verification, and
  rekey primitive. Encrypted rekey sources arrive over the attacker-controlled
  disk channel (`In()`), mirroring how the implementation feeds `rekeySlots`
  from exported OPFS bytes — so the rekey-soundness lemmas are authenticity
  theorems against forged input, not restatements of the write rule. Rekey to
  plain releases the plaintext to the attacker (`Out`), making the secrecy
  lemma's plain-export escape clause load-bearing.
- `vfs-inplace-lifecycle.spthy` models the operational wrapper around export
  and in-place conversion: source-shape preconditions, worker global-key
  lifecycle, temp/backup replacement, rollback, and pool-level
  decrypt-to-plain key purge. The worker's single global-key slot is one
  linear token per device (unique-init restriction), so the temporal lemmas
  genuinely exercise the install/clear state machine rather than re-reading
  labels off the rule that fired.
- `vfs-cache-import-lifecycle.spthy` models PRF seed / JS key-cache expiry,
  `KeyCacheStrategy.NONE` one-shot consumption, manifest-MAC-verified unlock,
  lock-on-expiry, deferred manifest persistence, and whole-pool import
  wipe-after-validate (full-source validation gating the destructive pool
  wipe; invalid sources rejected with pool state preserved).

## Scope

The model covers the encrypted at-rest channel:

- worker-wide VFS global-key registration,
- page AAD binding to version, path, and slot index,
- encrypted xWrite/xRead over a public attacker-controlled disk channel,
- slot-0 `verifyEncryptionKey` soundness,
- one bounded current-to-next key rotation,
- plain-to-encrypted, encrypted-to-plain, encrypted-to-encrypted, and
  plain-to-plain rekey events,
- legacy/cross-version ciphertext rejection,
- symbolic nonce freshness.
- PRF cache expiry clearing the worker global key and preserving the hint,
- `KeyCacheStrategy.NONE` consuming the C# seed entry on first key use
  (the JS-side bundle is modelled as session-lifetime under every strategy —
  see `vfs-cache-import-lifecycle.spthy` for the rule shape and the
  `crypto-vfs.md` "NONE" note for the runtime rationale),
- manifest MAC verification before unlock acceptance,
- whole-pool plain (.zip / .dbs) and cipher-envelope (.eds) import
  acceptance/rejection by current pool state, per-file content kind, and
  pre-destructive validation: the pool wipe (`PoolWiped`) fires only after
  the entire source has validated read-only, and a tampered / truncated /
  crafted source is rejected with pool state, hint, and globalKey intact.

Plain VFS mode and rekey-to-plain are represented as events, not confidentiality
claims. The implementation returns plain bytes to the trusted caller in those
modes; the at-rest attacker proof is about encrypted disk material.

## Lemmas

Run `docs/formal/verify.sh` (all lemmas must report `verified`), then
`docs/formal/mutation-check.sh` (deliberately broken model copies must
FALSIFY their lemma — the anti-vacuity check). Equivalent manual run:

```sh
tamarin-prover --prove docs/formal/vfs-tamarin/vfs.spthy
tamarin-prover --prove docs/formal/vfs-tamarin/vfs-inplace-lifecycle.spthy
tamarin-prover --prove docs/formal/vfs-tamarin/vfs-cache-import-lifecycle.spthy
```

Every theory also carries `sanity_*` exists-trace lemmas: the universal
security lemmas would verify vacuously over unreachable events, so each
event they quantify over has a reachability witness.

Expected `vfs.spthy` summary:

- `key_secrecy`
- `encrypted_slot_secrecy_unless_plain_exported`
- `encrypted_read_authenticity`
- `verify_key_match_sound`
- `rekey_encrypted_to_plain_sound`
- `rekey_encrypted_to_encrypted_sound`
- `legacy_ciphertexts_not_read_as_v1`
- `nonce_never_reused`
- `sanity_encrypted_write_reachable` … `sanity_rekey_encrypted_to_encrypted_reachable` (5 exists-trace)

Expected `vfs-inplace-lifecycle.spthy` summary:

- `key_install_requires_empty_global_key`
- `export_encrypt_requires_plain_without_global_key`
- `encrypt_in_place_requires_plain_without_global_key`
- `export_plain_requires_encrypted_global_key`
- `export_rekey_requires_encrypted_global_key`
- `decrypt_in_place_requires_encrypted_global_key`
- `decrypt_success_keeps_global_key_until_pool_leave`
- `decrypt_failure_keeps_global_key`
- `leave_encrypted_clears_global_key`
- `replacement_failure_restores_original`
- `encrypt_failure_restores_plain_original`
- `decrypt_failure_restores_encrypted_original`
- `encrypt_success_poststate`
- `decrypt_success_poststate`
- `sanity_encrypt_commit_reachable` … `sanity_leave_encrypted_reachable` (8 exists-trace)
- Temporal key-state lemmas (verified): `export_plain_key_live_since_install`
  (original strength), `decrypt_key_installed_before_use` +
  `clear_requires_install` (decrypt split),
  `encrypt_in_place_keyslot_emptied_since_install` /
  `export_encrypt_keyslot_emptied_since_install` (slot-emptied form),
  `empty_event_is_init_or_clear`, `key_installed_once`,
  `key_event_rooted_at_init` (reuse invariants — placement before/after
  matters, see in-file comments)
- Use-after-clear (verified): `no_claim_held_after_clear` — no rule may
  claim a held key after that key was cleared — carried by
  `cleared_key_never_held_again` and `clear_requires_install [reuse]`.
  Stated over `ClaimKeyState` so it covers all seven rules that claim a
  held key, and so its proof carries no file variables; the decrypt-only
  phrasing does, and diverges down the `PendingDecryptOp` chain.

Expected `vfs-cache-import-lifecycle.spthy` summary:

- `hint_write_is_after_pool_encryption`
- `unlock_requires_seed_cache`
- `accepted_unlock_requires_manifest_mac_verified`
- `rejected_unlock_clears_global_key`
- `none_seed_is_consumed_exactly_once`
- `cache_expiry_locks_and_clears_global_key`
- `cache_expiry_preserves_hint`
- `plain_zip_import_accept_requires_preflight`
- `unlocked_plain_zip_import_stays_encrypted`
- `locked_plain_zip_import_breaks_to_plain`
- `cipher_import_accept_requires_preflight`
- `guided_cipher_import_from_plain_ends_unlocked`
- `guided_cipher_import_from_locked_ends_unlocked`
- `plain_pool_rejects_cipher_import`
- `pool_wipe_requires_validated_source`
- `rejected_import_preserves_pool_state`
- `rejected_import_never_wipes`
- `sanity_unlock_accept_reachable` … `sanity_pool_wipe_reachable` (11 exists-trace)

## Verification status

**ALL GREEN** — Tamarin 1.12.0, 2026-08-21: **74 lemmas verified**
(vfs 14, inplace-lifecycle 32, cache-import-lifecycle 28). **No
`pending_*` statements remain** — see `PENDING-PROOF-PLAN.md` for how all
three were resolved.

The gate was red from 2026-06-30 to 2026-08-21. `76d3a34`, a commit about
a WAL checkpoint fix in the worker, dropped the `pending_` prefix from
`decrypt_no_clear_before_use` without weakening it; `verify.sh` skips
`pending_*`, so that rename alone put an unprovable phrasing into the gate.
It is now replaced by `no_claim_held_after_clear`, which is stronger.

The temporal lemmas use the in-theory `deprioFiles` tactic where annotated —
file-state claim goals are irrelevant to the key-timeline arguments and
deprioritizing them is the difference between 12-step proofs and
non-termination. Note the limit of that tactic: it matches `FileState|FileVer`
and **not** the linear `PendingDecryptOp` / `PendingEncryptOp` facts, so a
lemma whose premise carries file variables still descends the pending-op
chain no matter how goals are ranked. Keep key-timeline lemmas free of file
variables rather than trying to rank the file goals away.

### Running the gate on macOS

`verify.sh` passes one `--prove=<lemma>` per lemma in a single invocation.
At ~30 lemmas that argument list overflows the 255-byte filename limit and
tamarin dies with `openFile: invalid argument (File name too long)` — which
is not a proof failure. A bare `--prove` instead shares one proof search
across all lemmas and exhausts a 16 GB heap. Prove in batches of ~6.

When batching, note that each batch's `summary of summaries` lists **every**
lemma in the theory; the ones not selected in that batch read `analysis
incomplete`. Filter to the batch's own selection or the result reads as a
mass failure when it is nothing of the kind.

Provenance of the two non-obvious ingredients:

- `vfs.spthy` needs `cipher_sources [sources]`: without it the
  In()-sourced rekey rules send the backward search into unbounded
  `RekeyEncryptedToEncrypted` source-chains (observed: >58 min / >45 GB,
  non-terminating).
- The lifecycle theories use the claim/event/restriction state encoding
  (2026-06-12 rework). The original token-passing encoding diverged in
  precomputation for ANY exists-trace lemma — including on the ef336d2
  rule set — i.e. the historical `verified` stamps never required the
  prover to construct a single execution trace.

Open: nothing. The temporal key-state lemmas that were excluded from the
gate are all resolved — two replaced by provable weaker forms in `76d3a34`,
the third by the file-free `no_claim_held_after_clear`, which is stronger
than the statement it replaces.
