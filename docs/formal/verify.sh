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

# Lemmas are proven in batches rather than all in one invocation. Two limits
# force this, and neither reports itself as a proof problem:
#   - One --prove= per lemma in a single call makes tamarin derive an output
#     filename from the argument list. Past ~30 lemmas that overflows the
#     255-byte filename limit and it dies with "openFile: invalid argument
#     (File name too long)".
#   - A bare --prove shares one proof search across every lemma and exhausts
#     a 16 GB heap on vfs-inplace-lifecycle.
BATCH=${TAMARIN_BATCH:-6}

THEORIES=(${1:-vfs vfs-inplace-lifecycle vfs-cache-import-lifecycle})
FAIL=0

for t in "${THEORIES[@]}"; do
    f="docs/formal/vfs-tamarin/${t}.spthy"
    echo "=== ${f}"
    # Prove every lemma except pending_* (stated but known-unprovable
    # without an oracle — see the theory's comments). None are pending as
    # of 2026-08-21; the filter stays for when one is added.
    lemmas=($(grep -E '^lemma ' "$f" | sed -E 's/^lemma +([A-Za-z0-9_]+).*/\1/' | grep -v '^pending_'))
    total=${#lemmas[@]}
    for ((i=0; i<total; i+=BATCH)); do
        sel=("${lemmas[@]:i:BATCH}")
        args=""
        for l in "${sel[@]}"; do args="$args --prove=$l"; done
        summary=$(tamarin-prover $args $RTS "$f" 2>&1 | sed -n '/summary of summaries/,$p')
        if [ -z "$summary" ]; then
            echo "    ERROR: no summary produced (crash / OOM?)"
            FAIL=1
            continue
        fi
        # Every batch's summary lists EVERY lemma in the theory; the ones this
        # batch did not select read "analysis incomplete". Report only what
        # this batch actually proved, or the output reads as a mass failure.
        for l in "${sel[@]}"; do
            line=$(echo "$summary" | grep -E "^  ${l} \(")
            if [ -z "$line" ]; then
                echo "    FAIL: ${l} produced no summary line"
                FAIL=1
                continue
            fi
            echo "$line"
            case "$line" in
                *": verified"*) ;;
                *) echo "    FAIL: ${l} not verified"; FAIL=1 ;;
            esac
        done
    done
done

if [ "$FAIL" -eq 0 ]; then
    echo "ALL THEORIES VERIFIED"
else
    echo "VERIFICATION FAILED"
fi
exit "$FAIL"
