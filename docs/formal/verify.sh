#!/usr/bin/env bash
# Verify all Tamarin theories: every lemma must report `verified`.
#
# Run from anywhere; needs tamarin-prover (1.12.0+) and maude on PATH.
# Memory note: proof search is RAM-hungry relative to the theory size —
# run this on a machine with a few GB free, not a memory-constrained one.
#
#   ./docs/formal/verify.sh            # all three theories
#   ./docs/formal/verify.sh vfs        # just vfs.spthy
set -u
cd "$(dirname "$0")/../.."

# The theories contain UTF-8 in comments; GHC's runtime decodes files with
# the ambient locale and hard-fails under POSIX/C (common on fresh cloud
# boxes): "hGetContents: invalid argument".
export LC_ALL=C.UTF-8 LANG=C.UTF-8

# Hard heap cap (HOWTO §5): on shared boxes tamarin must fail politely, not
# squeeze co-tenant workloads. Override e.g. TAMARIN_RTS="+RTS -M40G -RTS".
RTS=${TAMARIN_RTS:-"+RTS -M16G -RTS"}

THEORIES=(${1:-vfs vfs-inplace-lifecycle vfs-cache-import-lifecycle})
FAIL=0

for t in "${THEORIES[@]}"; do
    f="docs/formal/vfs-tamarin/${t}.spthy"
    echo "=== ${f}"
    # Prove every lemma except pending_* (stated but known-unprovable
    # without an oracle — see the theory's comments).
    lemmas=$(grep -E '^lemma ' "$f" | sed -E 's/^lemma +([A-Za-z0-9_]+).*/\1/' | grep -v '^pending_')
    args=""
    for l in $lemmas; do args="$args --prove=$l"; done
    summary=$(tamarin-prover $args $RTS "$f" 2>&1 | sed -n '/summary of summaries/,$p')
    if [ -z "$summary" ]; then
        echo "    ERROR: no summary produced (crash / OOM?)"
        FAIL=1
        continue
    fi
    echo "$summary" | grep -E '\(all-traces\)|\(exists-trace\)'
    bad=$(echo "$summary" | grep -E '\(all-traces\)|\(exists-trace\)' | grep -v '^  pending_' | grep -cv ': verified')
    if [ "$bad" -ne 0 ]; then
        echo "    FAIL: ${bad} lemma(s) not verified"
        FAIL=1
    fi
done

if [ "$FAIL" -eq 0 ]; then
    echo "ALL THEORIES VERIFIED"
else
    echo "VERIFICATION FAILED"
fi
exit "$FAIL"
