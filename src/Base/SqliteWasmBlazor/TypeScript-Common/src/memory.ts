// Memory hygiene for worker code that handles user data.
//
// Deliberately not imported from the crypto package: zeroing a buffer is
// not a cryptographic operation, and plane-1 shared code should not take a
// dependency on the crypto plane to call fill(0). crypto-core keeps its own
// copy for crypto call sites, which also keeps that package dependency-free.

/** Overwrite a buffer with zeroes. */
export function clearBytes(bytes: Uint8Array): void {
    bytes.fill(0);
}
