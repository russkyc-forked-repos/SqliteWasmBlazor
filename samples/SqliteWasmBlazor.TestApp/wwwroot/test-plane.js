// test-plane.js
// Which worker bundle this run boots, read from the page URL.
//
// The TestApp is the only place both bundles are reachable — it references
// SqliteWasmBlazor.Crypto, so the host serves _content/SqliteWasmBlazor/ and
// _content/SqliteWasmBlazor.Crypto/ side by side. Registering the Crypto
// services points the bridge at the second one, which means base's own worker
// cases would otherwise never execute anywhere. `?plane=plain` skips that
// registration so the same tests run against the plain bundle.
//
// A real .js module rather than an inline script: this app is CSP-strict.

export function plane() {
    return new URLSearchParams(globalThis.location.search).get('plane') ?? 'crypto';
}
