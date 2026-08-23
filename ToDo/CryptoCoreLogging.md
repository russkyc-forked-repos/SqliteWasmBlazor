# Level-gated logging for the crypto-core bundle

Every other TypeScript surface writes through `logger` from
`@sqlitewasmblazor/worker-common`, so a consumer's
`SqliteWasmLogger.SetLogLevel(...)` decides what reaches the console.
`TypeScript-Crypto` is the one that does not: it writes to `console` directly
and ignores the level.

## Where it stands

Four unconditional writes, all in
`src/Base/SqliteWasmBlazor/TypeScript-Crypto/src/crypto-bridge.ts`:

| Line | Call |
| --- | --- |
| 616 | `console.error('importVapidKeyPair failed:', e)` |
| 636 | `console.error('sendPushNotification: VAPID key not loaded')` |
| 652 | `console.error(\`sendPushNotification: HTTP ${status} …\`)` |
| 656 | `console.error('sendPushNotification failed:', e)` |

They are all `error`-level, so today's damage is limited to `LogLevel.None`
not being honoured — but the file is the natural home for the push/VAPID
diagnostics that do not exist yet, and each one added now inherits the gap.

Nothing under `src/crypto-core/**` logs at all. That is worth preserving as a
rule, not an accident: those modules hold key material, and the redaction
pattern is already written down — `keyFingerprint()` in
`worker/vfs-prf/rekey.ts` emits `<redacted:32B>`, never a prefix of the key.
A logger reaching the primitives must carry the same discipline.

## Why it is not a one-line import

`worker-common` cannot simply be added as a dependency. Its `index.ts` pulls
`msgpackr` and `@sqlite.org/sqlite-wasm`, and `crypto-bridge.js` is a
standalone esbuild bundle loaded through `JSHost.ImportAsync` — the plain
`sqlite-wasm-bridge.js` is not in the picture. Four error lines would cost the
bundle both dependencies, and it would put a plane-1 crypto package behind the
worker infrastructure it currently knows nothing about.

There are two distributions of the same source to keep straight:

- **`src/crypto-core/**`** is the package's export map (`main` →
  `./src/crypto-core/index.ts`). The worker imports it, so it lands *inside*
  `sqlite-wasm-worker.js` — where a `worker-common` logger instance already
  exists.
- **`src/index.ts` + `crypto-bridge.ts` + `webauthn.ts` + `prf.ts`** are the
  esbuild entry for `crypto-bridge.js`, a bundle of its own with no logger in
  it at all.

A module-level singleton is per bundle, so "one logger" only means one
*implementation*; the level still has to reach each bundle separately.

## The shape to build

**Recommended: lift `sqlite-logger.ts` into a dependency-free package** — say
`Base/SqliteWasmBlazor/TypeScript-Log` as `@sqlitewasmblazor/log` — and have
`worker-common` re-export it (`export * from '@sqlitewasmblazor/log'`, so no
existing import changes) with `crypto-core` depending on it directly. Inside
any one bundle esbuild then resolves both paths to the same module, so a
single `setLogLevel` covers `worker-common` and `crypto-core` together. Cost
is one more npm workspace (a sixth) and three `package.json` edits; the payoff
is that `SqliteWasmLogLevel` stays defined once.

Rejected alternatives, so they are not re-derived:

- *A second logger implementation inside `crypto-core`* — cheapest edit, but
  the worker bundle then holds two singletons and every `setLogLevel` path has
  to remember both. The next crypto-core log line that lands quietly at the
  wrong level is the failure mode.
- *`worker-common` depending on `crypto-core`* — inverts the plane direction
  and risks pulling `@awasm/noble` into the plain worker bundle on any
  tree-shaking miss.

## The ordering hazard — this is the part that bites

`SqliteWasmLogger.SetLogLevel` is called from the `SqliteWasmConnection`
constructor (`Ado/SqliteWasmConnection.cs:60`) during startup. `CryptoInterop`
and `PrfService` import `crypto-bridge.js` **lazily, on first crypto call**
(`Interop/CryptoInterop.cs:74`, `Services/PrfService.cs:78`). The level
therefore arrives *before* the bundle that needs it exists, and a plain
`[JSImport("setLogLevel", ModuleName)]` would either throw or be skipped.

So the C# entry point has to become a fan-out with replay, not a second
import:

1. Base keeps the current single JSImport surface and remembers the last level
   it was given.
2. Each bundle registers its own setter on load — `globalThis` registry, the
   same slot `__sqliteWasmLogger` already occupies, extended to a list.
3. Registration immediately replays the remembered level, so a late-loading
   bundle catches up instead of sitting at the `Warning` default.

Verify with `LogLevel.None` and a deliberate push failure: nothing may reach
the console.

## Checklist

- [ ] Extract `TypeScript-Common/src/sqlite-logger.ts` into
      `@sqlitewasmblazor/log`; re-export from `worker-common`.
- [ ] Add the dependency to `TypeScript-Crypto`; convert the four
      `console.error` calls in `crypto-bridge.ts` to `logger.error` with a
      module tag (`Crypto Bridge`).
- [ ] Turn `globalThis.__sqliteWasmLogger` into a registry, and register from
      `crypto-bridge.js`'s entry (`src/index.ts`).
- [ ] Replay the last level on registration; `SqliteWasmLogger` holds it.
- [ ] `npm run typecheck && npm run lint` in each touched workspace
      (`TypeScript-Log`, `TypeScript-Common`, `TypeScript-Crypto`, `TypeScript`,
      `Crypto/TypeScript`), `npm test` in `TypeScript-Crypto` and the crypto
      worker, then the Playwright suite — it exercises both bundles.
