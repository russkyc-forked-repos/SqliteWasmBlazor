# PRF VFS Tamarin Model

This folder contains Tamarin models for the PRF-keyed VFS implementation in
`src/Crypto/SqliteWasmBlazor.Crypto/TypeScript/worker/vfs-prf`, its in-place
conversion wrapper, and the disk-level PRF cache/import lifecycle.

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
  lifecycle, temp/backup replacement, rollback, and disk-level
  decrypt-to-plain key purge. The worker's single global-key slot is one
  linear token per device (unique-init restriction), so the temporal lemmas
  genuinely exercise the install/clear state machine rather than re-reading
  labels off the rule that fired.
- `vfs-cache-import-lifecycle.spthy` models PRF seed / JS key-cache expiry,
  `KeyCacheStrategy.NONE` one-shot consumption, manifest-MAC-verified unlock,
  lock-on-expiry, deferred manifest persistence, and whole-disk import
  wipe-after-validate (full-source validation gating the destructive pool
  wipe; invalid sources rejected with disk state preserved).

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
- whole-disk plain (.zip / .dbs) and cipher-envelope (.eds) import
  acceptance/rejection by current disk state, per-file content kind, and
  pre-destructive validation: the pool wipe (`PoolWiped`) fires only after
  the entire source has validated read-only, and a tampered / truncated /
  crafted source is rejected with disk state, hint, and globalKey intact.

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
- `decrypt_success_keeps_global_key_until_disk_leave`
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
- `pending_*` (3) — the strong per-key forms (nested-quantifier
  no-live-key pair, decrypt no-clear half); stated, NOT verified, skipped
  by verify.sh. Their inductive descent diverges under all built-in
  heuristics, use_induction, budget bounding, and the deprioFiles tactic;
  a hand-guided interactive proof is the known remaining route.

Expected `vfs-cache-import-lifecycle.spthy` summary:

- `hint_write_is_after_disk_encryption`
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
- `plain_disk_rejects_cipher_import`
- `pool_wipe_requires_validated_source`
- `rejected_import_preserves_disk_state`
- `rejected_import_never_wipes`
- `sanity_unlock_accept_reachable` … `sanity_pool_wipe_reachable` (11 exists-trace)

## Verification status

**ALL GREEN** — Tamarin 1.12.0 + maude 3.5.1, 2026-06-13, full
`verify.sh` + `mutation-check.sh` gate in ~65 s on a 4-CPU / 21 GB box
(heap cap 10G): 72 lemmas verified (vfs 14, inplace-lifecycle 30,
cache-import-lifecycle 28), 3/3 mutations falsified. 3 `pending_*`
statements remain open by design (see above). The temporal lemmas use the
in-theory `deprioFiles` tactic where annotated — file-state claim goals
are irrelevant to the key-timeline arguments and deprioritizing them is
the difference between 12-step proofs and non-termination.

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

Open: the 4 `pending_*` temporal key-state lemmas in
`vfs-inplace-lifecycle.spthy` (each proves in <100 steps assuming the
reuse invariants, but the closing induction diverges under all built-in
heuristics; needs a goal-ranking oracle). They are stated, documented,
and excluded from the gate.
