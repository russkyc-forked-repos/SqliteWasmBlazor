#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# h100 (Hyperstack) Tamarin proof-run live log. Tails /tmp/proofrun/run.log —
# docs/formal/run-all.sh writes there (local time · gate phase · per-lemma
# verdicts · box mem/load heartbeat every 30 s).
#   • Ctrl-C = closes ONLY this viewer. The run is nohup-DETACHED on the box
#              and keeps going. Closed it? Double-click this file again.
#   • Finished runs end with a "FINAL: …" line; the log stays readable after.
# ─────────────────────────────────────────────────────────────────────────────
trap 'printf "\n\033[33m[viewer closed — proof run keeps going on h100. Re-open: double-click live_proofs_h100.command]\033[0m\n"; exit 0' INT TERM
clear
box=$(ssh -o LogLevel=ERROR -o ConnectTimeout=10 h100 hostname 2>/dev/null || echo "unreachable")
printf '\033[1;36m════ %s  (ssh h100) · /tmp/proofrun/run.log ════\033[0m\n' "$box"
printf '\033[2m(Ctrl-C = close this viewer only — the proof run keeps going on the box)\033[0m\n\n'
ssh -o LogLevel=ERROR h100 'tail -n 40 -F /tmp/proofrun/run.log 2>/dev/null'
