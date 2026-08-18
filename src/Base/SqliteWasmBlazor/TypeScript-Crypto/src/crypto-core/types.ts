// @sqlitewasmblazor/crypto-core — shared types
// All types use Uint8Array for binary data (no Base64 strings)

// ============================================================
// CONSTANTS
// ============================================================

export const NONCE_LENGTH_AES = 12;
export const KEY_LENGTH = 32;

// ============================================================
// ERROR HANDLING
// ============================================================

/**
 * Error codes matching C# PrfErrorCode enum.
 *
 * Member values are the JS -> C# wire tokens and MUST stay byte-identical to the
 * member names of C# `PrfErrorCode` (SqliteWasmBlazor.Crypto/Models/PrfErrorCode.cs).
 * `PrfJsonContext` deserializes them with `UseStringEnumConverter`, which matches
 * enum member names verbatim (case-insensitively, but underscores are significant) --
 * a value that does not match throws JsonException instead of yielding a
 * structured PrfResult failure.
 */
export enum PrfErrorCode {
    Unknown = 'UNKNOWN',
    NotSupported = 'NOT_SUPPORTED',
    PrfNotSupported = 'PRF_NOT_SUPPORTED',
    CredentialNotFound = 'CREDENTIAL_NOT_FOUND',
    AuthenticationTagMismatch = 'AUTHENTICATION_TAG_MISMATCH',
    InvalidData = 'INVALID_DATA',
    KeyDerivationFailed = 'KEY_DERIVATION_FAILED',
    EncryptionFailed = 'ENCRYPTION_FAILED',
    DecryptionFailed = 'DECRYPTION_FAILED',
    RegistrationFailed = 'REGISTRATION_FAILED',
    InvalidPublicKey = 'INVALID_PUBLIC_KEY',
    InvalidPrivateKey = 'INVALID_PRIVATE_KEY',
    SigningFailed = 'SIGNING_FAILED',
    VerificationFailed = 'VERIFICATION_FAILED',
    IncompatibleFormat = 'INCOMPATIBLE_FORMAT',
}

/**
 * Result wrapper matching C# PrfResult<T>.
 */
export interface PrfResult<T> {
    success: boolean;
    value?: T;
    errorCode?: PrfErrorCode;
    cancelled?: boolean;
}

/**
 * Factory functions for PrfResult — mirrors C# static methods.
 */
export const PrfResultUtil = {
    ok: <T>(value: T): PrfResult<T> => ({ success: true, value }),
    fail: <T>(errorCode: PrfErrorCode): PrfResult<T> => ({ success: false, errorCode }),
    cancelled: <T>(): PrfResult<T> => ({ success: false, cancelled: true }),
} as const;

// ============================================================
// KEY TYPES
// ============================================================

/**
 * A key pair (private + public). Matches C# KeyPair record.
 */
export interface KeyPair {
    privateKey: Uint8Array;
    publicKey: Uint8Array;
}

/**
 * Public keys only (X25519 + Ed25519). Matches C# DualKeyPair record.
 */
export interface DualKeyPair {
    x25519PublicKey: Uint8Array;
    ed25519PublicKey: Uint8Array;
}

/**
 * Full dual key pair (both private + public). Matches C# DualKeyPairFull record.
 */
export interface DualKeyPairFull {
    x25519PrivateKey: Uint8Array;
    x25519PublicKey: Uint8Array;
    ed25519PrivateKey: Uint8Array;
    ed25519PublicKey: Uint8Array;
}

// ============================================================
// ENCRYPTED DATA TYPES
// ============================================================

/**
 * AES-256-GCM encrypted data (nonce + ciphertext).
 * Matches C# SymmetricEncryptedData record.
 */
export interface SymmetricEncryptedData {
    ciphertext: Uint8Array;
    nonce: Uint8Array;
}

/**
 * ECIES encrypted data (X25519 + AES-GCM).
 * Matches C# AsymmetricEncryptedData record.
 */
export interface AsymmetricEncryptedData {
    ephemeralPublicKey: Uint8Array;
    ciphertext: Uint8Array;
    nonce: Uint8Array;
}

// ============================================================
// GROUP ENCRYPTION TYPES
// ============================================================

/**
 * A content encryption key (CEK) wrapped for a specific group member.
 * Matches C# WrappedKey record.
 */
export interface WrappedKey {
    memberPublicKey: Uint8Array;
    wrappedContentKey: SymmetricEncryptedData;
}

/**
 * Complete key bundle for a group — contains wrapped CEKs for all members.
 * Matches C# GroupKeyBundle record.
 */
export interface GroupKeyBundle {
    groupContext: string;
    keyVersion: number;
    adminPublicKey: Uint8Array;
    memberKeys: WrappedKey[];
}

/**
 * An encrypted payload within a group, with tamper detection metadata.
 * Matches C# GroupEncryptedData record.
 */
export interface GroupEncryptedData {
    groupContext: string;
    keyVersion: number;
    encrypted: SymmetricEncryptedData;
    senderPublicKey: Uint8Array;
    envelopeSignature: Uint8Array;
}
