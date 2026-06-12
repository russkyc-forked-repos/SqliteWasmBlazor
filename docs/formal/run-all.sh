#!/usr/bin/env bash
# Detached full proof run: verify.sh + mutation-check.sh with a live log.
#
# Designed for a remote/shared box: writes everything (incl. a box-health
# line every 30 s) to run.log next to this script's parent dir, so a local
# viewer can `tail -F` it. Run detached:
#
#   nohup ./docs/formal/run-all.sh >/dev/null 2>&1 &
#
# Heap cap / niceness: inherits TAMARIN_RTS (default +RTS -M16G -RTS, see
# verify.sh) and runs everything under `nice -n 10` — co-tenant friendly.
set -u
cd "$(dirname "$0")"
LOG="${PROOF_LOG:-/tmp/proofrun/run.log}"
mkdir -p "$(dirname "$LOG")"
: > "$LOG"

ts() { date '+%H:%M:%S'; }
say() { echo "[$(ts)] $*" >> "$LOG"; }

say "proof run start · $(tamarin-prover --version 2>/dev/null | grep -o 'tamarin-prover [0-9.]*' | head -1 || echo 'tamarin missing') · heap cap ${TAMARIN_RTS:-+RTS -M16G -RTS} · nice 10"

# Box-health heartbeat while the run is live (avail mem GB · 1/5/15 load).
(
    while sleep 30; do
        if [ -r /proc/loadavg ]; then
            echo "[$(ts)] · box: $(free -g | awk '/^Mem:/{print $7}')G avail · load $(cut -d' ' -f1-3 /proc/loadavg)" >> "$LOG"
        else
            echo "[$(ts)] · box: load $(uptime | sed 's/.*load averages*: //')" >> "$LOG"
        fi
    done
) &
MON=$!
trap 'kill "$MON" 2>/dev/null' EXIT

say "── verify.sh (all lemmas must be verified) ──"
nice -n 10 ./verify.sh >> "$LOG" 2>&1
V=$?
say "verify.sh exit=$V"

say "── mutation-check.sh (broken models must falsify) ──"
nice -n 10 ./mutation-check.sh >> "$LOG" 2>&1
M=$?
say "mutation-check.sh exit=$M"

if [ "$V" -eq 0 ] && [ "$M" -eq 0 ]; then
    say "FINAL: ALL GREEN — every lemma verified, every mutation falsified"
else
    say "FINAL: FAILURES (verify=$V mutation=$M) — see above"
fi
exit $(( V + M ))
