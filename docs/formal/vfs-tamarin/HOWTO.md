# How to run the VFS Tamarin proofs

Practical guide for this folder's three theories (`vfs.spthy`,
`vfs-inplace-lifecycle.spthy`, `vfs-cache-import-lifecycle.spthy` — ~80 lemmas
total). The model documentation lives in `README.md`; this file is only about
*running* things.

## 1. Install

Tamarin needs two binaries on `PATH`: `tamarin-prover` (1.12.0+ per
`verify.sh`) and `maude`.

**macOS (Homebrew):**
```bash
brew install tamarin-prover/tap/tamarin-prover   # pulls maude as dependency
```

**Linux (e.g. a rented box):**
```bash
sudo apt-get install -y maude
# tamarin: grab the static release binary
wget -qO /usr/local/bin/tamarin-prover \
  https://github.com/tamarin-prover/tamarin-prover/releases/latest/download/tamarin-prover-linux64 \
  && chmod +x /usr/local/bin/tamarin-prover
tamarin-prover --version    # sanity: also reports whether maude is found
```

## 2. The standard gate — run everything

```bash
./docs/formal/verify.sh                       # all three theories
./docs/formal/verify.sh vfs                   # just vfs.spthy
```

Every lemma must report `verified` in the `summary of summaries`; the script
exits non-zero otherwise. This is the CI-style check — run it after ANY change
to a `.spthy` or to the modeled TypeScript.

## 3. Working on a single lemma

```bash
# prove one lemma only (= exact name; trailing * = prefix match)
tamarin-prover --prove=rekey_soundness docs/formal/vfs-tamarin/vfs.spthy
tamarin-prover --prove='secrecy*'      docs/formal/vfs-tamarin/vfs.spthy
```

Output per lemma is `verified (N steps)`, `falsified — found trace`, or it
hangs (see §5). `falsified` prints the attack trace — read it bottom-up: the
last `!KU(...)` facts show what the attacker had to know.

## 4. Interactive mode (the GUI) — for debugging a lemma

```bash
tamarin-prover interactive docs/formal/vfs-tamarin/
# then open http://localhost:3001
```

- Pick the theory → pick the lemma → step through the proof tree manually
  ("autoprove" per branch with `a`, single steps by clicking constraints).
- This is the tool for understanding WHY a lemma fails or loops: watch which
  rule instantiations multiply.
- On a remote box, tunnel the port instead of exposing it:
  `ssh -L 3001:localhost:3001 <box>` and browse locally.

## 5. Memory, threads, timeouts

- Proof search is RAM-hungry relative to theory size; these three theories are
  small (a few GB suffice — they run on any 8 GB machine; the dev Mac under
  memory pressure was the exception, hence the remote-box recipe in §6).
- Haskell runtime flags control parallelism/heap:
  ```bash
  tamarin-prover --prove +RTS -N8 -M40G -RTS file.spthy
  #                       -N8 = 8 cores · -M40G = hard heap cap (fail, not thrash)
  ```
- A lemma that runs minutes on these theories is effectively looping —
  interrupt and inspect interactively rather than waiting.

## 6. Running on a rented box (memory-constrained local machine)

Any cheap CPU box works; the proofs use no GPU. Pattern:

```bash
# from the repo root — copy ONLY the formal dir, run, fetch the summary
scp -r docs/formal <box>:/tmp/formal
ssh <box> 'cd /tmp && sudo apt-get install -y -qq maude && \
  wget -qO /usr/local/bin/tamarin-prover <release-url> && chmod +x /usr/local/bin/tamarin-prover && \
  ./formal/verify.sh' | tee /tmp/tamarin-summary.txt
```

The theories are self-contained — no other repo files needed. `verify.sh`'s
exit code is the verdict; keep the summary file with the commit that changed
the model.

## 7. Workflow rules of thumb

- **Model change → full gate** (`verify.sh`), not just the lemma you touched:
  restrictions interact, a "helping" restriction can silently weaken a
  different lemma's statement.
- **`verified` is only as strong as the statement** — when a lemma proves
  suspiciously fast after an edit, check you didn't vacuify it (Tamarin
  reports vacuously-true `exists-trace` lemmas as `falsified`; use
  `exists-trace` twins for the main theorems where the README notes them).
- **Keep lemma names stable** — `verify.sh` greps the summary; renames look
  like coverage loss in diffs.
- The `sources` lemmas (auto-generated case distinctions) must stay
  `verified`; a `partial deconstructions` warning at load time means proof
  results below it are unreliable — fix the sources lemma first.
