#!/usr/bin/env bash
# Mutation checks: deliberately break the models and assert that the
# corresponding security lemma is FALSIFIED. A lemma that cannot fail when
# its mechanism is removed proves nothing — this script is the anti-vacuity
# companion to verify.sh (which asserts everything passes on the intact
# models).
#
# Each mutation is applied to a temp copy; the repo files are never touched.
# Memory note: same as verify.sh — run on a machine with a few GB free.
set -u
cd "$(dirname "$0")/../.."

# UTF-8 comments in the theories vs GHC locale decoding — see verify.sh.
export LC_ALL=C.UTF-8 LANG=C.UTF-8

# Hard heap cap on shared boxes — see verify.sh.
RTS=${TAMARIN_RTS:-"+RTS -M16G -RTS"}

SRC=docs/formal/vfs-tamarin
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
FAIL=0

# run_mutation <name> <theory> <lemma> <perl -0pe substitution>
run_mutation() {
    local name="$1" theory="$2" lemma="$3" subst="$4"
    local mut="$TMP/${theory}.spthy"
    cp "$SRC/${theory}.spthy" "$mut"
    perl -0pi -e "$subst" "$mut"
    if cmp -s "$SRC/${theory}.spthy" "$mut"; then
        echo "MUTATION NOT APPLIED (pattern drifted?): $name"
        FAIL=1
        return
    fi
    local result
    result=$(tamarin-prover --prove="$lemma" $RTS "$mut" 2>&1 \
        | sed -n '/summary of summaries/,$p' | grep -F "$lemma ")
    if echo "$result" | grep -q 'falsified'; then
        echo "OK   $name: $lemma falsified as expected"
    else
        echo "FAIL $name: expected '$lemma' to be falsified, got: ${result:-<no summary>}"
        FAIL=1
    fi
}

# 1. Drop the dbPath binding from the encrypted read pattern -> a ciphertext
#    written for one path is accepted as a read of another (cross-DB swap).
run_mutation "vfs/read-ignores-path" vfs encrypted_read_authenticity \
    's/aad\(.v1., \$Path, \$SlotIdx\), <nonce, m>>, k\)/aad('"'"'v1'"'"', \$OtherPath, \$SlotIdx), <nonce, m>>, k)/'

# 2. Make the invalid-envelope rejection path wipe the pool -> the
#    wipe-after-validate invariant must break.
run_mutation "cache-import/reject-wipes" vfs-cache-import-lifecycle rejected_import_never_wipes \
    "s/ImportRejected\(\\\$Device, 'encryptedUnlocked', \\\$Kind, 'invalidEnvelope'\)/ImportRejected(\\\$Device, 'encryptedUnlocked', \\\$Kind, 'invalidEnvelope'), PoolWiped(\\\$Device)/"

# 3. Make the encrypt rollback "restore" the ciphertext instead of the
#    plain original -> the restores-original guarantee must break.
run_mutation "inplace/rollback-restores-wrong-blob" vfs-inplace-lifecycle replacement_failure_restores_original \
    "s/OriginalRestored\(D, P, 'plain', plain_blob\)/OriginalRestored(D, P, 'plain', cipher_blob)/"

if [ "$FAIL" -eq 0 ]; then
    echo "ALL MUTATION CHECKS PASSED"
else
    echo "MUTATION CHECKS FAILED"
fi
exit "$FAIL"
