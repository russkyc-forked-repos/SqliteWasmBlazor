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

THEORIES=(${1:-vfs vfs-inplace-lifecycle vfs-cache-import-lifecycle})
FAIL=0

for t in "${THEORIES[@]}"; do
    f="docs/formal/vfs-tamarin/${t}.spthy"
    echo "=== ${f}"
    summary=$(tamarin-prover --prove "$f" 2>&1 | sed -n '/summary of summaries/,$p')
    if [ -z "$summary" ]; then
        echo "    ERROR: no summary produced (crash / OOM?)"
        FAIL=1
        continue
    fi
    echo "$summary" | grep -E '\(all-traces\)|\(exists-trace\)'
    bad=$(echo "$summary" | grep -E '\(all-traces\)|\(exists-trace\)' | grep -cv ': verified')
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
