//
// Copyright 2026 Tabsdata Inc.
//

//! Descriptor encryption, keyed by `$TD_BACKENDS_CRYPTO_KEY`.
//!
//! Port of `tabsdatax.itest.tdksystems2.crypto`. Descriptors are PEM-armored
//! AES-256-GCM ciphertext whose key is PBKDF2-HMAC-SHA256; a fresh 128-bit salt
//! and 96-bit nonce are stored ahead of the ciphertext:
//!
//! ```text
//! -----BEGIN TDX3 DESCRIPTOR-----
//! <base64(salt || nonce || ciphertext+tag)>
//! -----END TDX3 DESCRIPTOR-----
//! ```
//!
//! A salt is not a secret — it exists to make each derivation unique, so it
//! travels with the ciphertext.
//!
//! `TDX2` is the previous format: same cipher, but no salt in the payload
//! because every descriptor derived its key from one hardcoded application
//! salt. This crate never reads one — it fails with [`LEGACY_REMEDY`], naming
//! the `tdx-system upgrade` / `tdx-domain upgrade` commands that convert a
//! store. Tests only ever read descriptors, so there is no encrypt side here.

use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::{LazyLock, Mutex};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use ring::aead::{AES_256_GCM, Aad, LessSafeKey, Nonce, UnboundKey};
use ring::pbkdf2;

pub const ENV_CRYPTO_KEY: &str = "TD_BACKENDS_CRYPTO_KEY";
pub const MIN_SECRET_LEN: usize = 12;

/// What anything tripping over a TDX2 descriptor should say. Kept in step with
/// the Python `crypto.LEGACY_REMEDY` so both toolchains give one instruction.
pub const LEGACY_REMEDY: &str = "descriptor is in the legacy TDX2 format (one shared salt) — run \
     'tdx-system upgrade' and 'tdx-domain upgrade' to convert the store";

const PEM_LABEL: &str = "TDX3 DESCRIPTOR";
const LEGACY_PEM_LABEL: &str = "TDX2 DESCRIPTOR";
const ITERATIONS: u32 = 200_000;
const SALT_LEN: usize = 16;
const NONCE_LEN: usize = 12;
const KEY_LEN: usize = 32;

/// Derived keys, by the `(secret, salt)` that produced them.
type KeyCache = Mutex<HashMap<(String, Vec<u8>), [u8; KEY_LEN]>>;

/// 200k PBKDF2 rounds cost ~350ms on an M-series Mac. Per-descriptor salts mean
/// one derivation per
/// distinct descriptor rather than one per process, so caching matters more now,
/// not less — a test that reads the same backend twice must not pay twice.
static KEY_CACHE: LazyLock<KeyCache> = LazyLock::new(|| Mutex::new(HashMap::new()));

/// The personal secret from `$TD_BACKENDS_CRYPTO_KEY`. The gate for every
/// descriptor read.
pub fn require_secret() -> Result<String, String> {
    let secret = std::env::var(ENV_CRYPTO_KEY).unwrap_or_default();
    if secret.len() < MIN_SECRET_LEN {
        return Err(format!(
            "{ENV_CRYPTO_KEY} must be set and at least {MIN_SECRET_LEN} characters long \
             to read tdx descriptors (it is what encrypts them)"
        ));
    }
    Ok(secret)
}

fn key(secret: &str, salt: &[u8]) -> [u8; KEY_LEN] {
    let cache_key = (secret.to_string(), salt.to_vec());
    if let Some(cached) = KEY_CACHE.lock().unwrap().get(&cache_key) {
        return *cached;
    }
    let mut derived = [0u8; KEY_LEN];
    pbkdf2::derive(
        pbkdf2::PBKDF2_HMAC_SHA256,
        NonZeroU32::new(ITERATIONS).unwrap(),
        salt,
        secret.as_bytes(),
        &mut derived,
    );
    KEY_CACHE.lock().unwrap().insert(cache_key, derived);
    derived
}

/// The PEM descriptor label in `text`, matched with its `END` armor, or `None`
/// if `text` carries no descriptor armor.
pub fn descriptor_label(text: &str) -> Option<String> {
    let begin = text.find("-----BEGIN ")? + "-----BEGIN ".len();
    let rest = &text[begin..];
    let end = rest.find("-----")?;
    let label = &rest[..end];
    if !label.ends_with(" DESCRIPTOR")
        || !label
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_' || c == ' ')
    {
        return None;
    }
    text.contains(&format!("-----END {label}-----"))
        .then(|| label.to_string())
}

pub fn is_pem(text: &str) -> bool {
    descriptor_label(text).is_some()
}

/// Whether `text` is a descriptor in the superseded `TDX2` format.
///
/// True means [`decrypt`] rejects it; `tdx-system upgrade` / `tdx-domain
/// upgrade` convert it.
pub fn is_legacy(text: &str) -> bool {
    descriptor_label(text).as_deref() == Some(LEGACY_PEM_LABEL)
}

/// Decrypt a PEM descriptor. Errors when `text` is not a descriptor at all, when
/// it is a legacy `TDX2` one (with [`LEGACY_REMEDY`]), and when it is current but
/// unreadable (unknown label, wrong secret, or corrupt).
pub fn decrypt(text: &str) -> Result<Vec<u8>, String> {
    let label = descriptor_label(text)
        .ok_or_else(|| "not an encrypted descriptor (no PEM armor)".to_string())?;
    if label == LEGACY_PEM_LABEL {
        return Err(LEGACY_REMEDY.to_string());
    }
    if label != PEM_LABEL {
        return Err(format!(
            "descriptor PEM label '{label}' is not the expected '{PEM_LABEL}' — \
             recreate it with tdx-system / tdx-domain create"
        ));
    }
    let body: String = text
        .lines()
        .filter(|line| !line.is_empty() && !line.starts_with("-----"))
        .collect();
    let secret = require_secret()?;
    decrypt_body(&body, &secret)
}

fn decrypt_body(body: &str, secret: &str) -> Result<Vec<u8>, String> {
    let undecryptable = || {
        format!("could not decrypt descriptor — is {ENV_CRYPTO_KEY} the one it was created with?")
    };
    let blob = BASE64.decode(body).map_err(|_| undecryptable())?;
    if blob.len() <= SALT_LEN + NONCE_LEN {
        return Err(undecryptable());
    }
    let (salt, rest) = blob.split_at(SALT_LEN);
    let (nonce, ciphertext) = rest.split_at(NONCE_LEN);
    let nonce = Nonce::try_assume_unique_for_key(nonce).map_err(|_| undecryptable())?;
    let unbound = UnboundKey::new(&AES_256_GCM, &key(secret, salt)).map_err(|_| undecryptable())?;
    let mut in_out = ciphertext.to_vec();
    let plaintext = LessSafeKey::new(unbound)
        .open_in_place(nonce, Aad::empty(), &mut in_out)
        .map_err(|_| undecryptable())?;
    Ok(plaintext.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ring::aead::{BoundKey, NonceSequence, SealingKey};
    use ring::error::Unspecified;

    const SECRET: &str = "a-test-secret-long-enough";
    const LEGACY_SALT: &[u8] = b"tabsdata-tdx2/descriptor/v1";

    struct FixedNonce([u8; NONCE_LEN]);

    impl NonceSequence for FixedNonce {
        fn advance(&mut self) -> Result<Nonce, Unspecified> {
            Nonce::try_assume_unique_for_key(&self.0)
        }
    }

    fn with_secret<T>(secret: &str, f: impl FnOnce() -> T) -> T {
        temp_env::with_var(ENV_CRYPTO_KEY, Some(secret), f)
    }

    /// Seal a descriptor the way the Python writer does. This crate only reads,
    /// so the round-trip tests have to produce their own input.
    fn seal(plaintext: &[u8], secret: &str, salt: &[u8], label: &str) -> String {
        let nonce = [7u8; NONCE_LEN];
        let unbound = UnboundKey::new(&AES_256_GCM, &key(secret, salt)).unwrap();
        let mut sealing = SealingKey::new(unbound, FixedNonce(nonce));
        let mut in_out = plaintext.to_vec();
        sealing
            .seal_in_place_append_tag(Aad::empty(), &mut in_out)
            .unwrap();
        // TDX3 carries its salt; TDX2 derived from one hardcoded application salt.
        let mut blob = if label == LEGACY_PEM_LABEL {
            Vec::new()
        } else {
            salt.to_vec()
        };
        blob.extend_from_slice(&nonce);
        blob.extend_from_slice(&in_out);
        let body = BASE64.encode(blob);
        format!("-----BEGIN {label}-----\n{body}\n-----END {label}-----\n")
    }

    fn seal_current(plaintext: &[u8], secret: &str) -> String {
        seal(plaintext, secret, &[9u8; SALT_LEN], PEM_LABEL)
    }

    fn seal_legacy(plaintext: &[u8], secret: &str) -> String {
        seal(plaintext, secret, LEGACY_SALT, LEGACY_PEM_LABEL)
    }

    #[test]
    fn test_descriptor_label() {
        let pem = seal_current(b"x", SECRET);
        assert_eq!(descriptor_label(&pem).as_deref(), Some(PEM_LABEL));
        assert!(is_pem(&pem));
        assert!(!is_legacy(&pem));
        assert!(is_legacy(&seal_legacy(b"x", SECRET)));
        assert_eq!(descriptor_label("no armor here"), None);
        assert!(!is_pem("no armor here"));
        // BEGIN without a matching END is not a descriptor.
        assert_eq!(
            descriptor_label("-----BEGIN TDX3 DESCRIPTOR-----\nx\n"),
            None
        );
    }

    #[test]
    fn test_decrypt_round_trip() {
        let pem = seal_current(b"{\"role\":\"domain\"}", SECRET);
        with_secret(SECRET, || {
            assert_eq!(decrypt(&pem).unwrap(), b"{\"role\":\"domain\"}");
        });
    }

    /// The salt rides in the payload, so two descriptors sealed under different
    /// salts must both open with the one secret.
    #[test]
    fn test_salt_comes_from_the_payload() {
        let a = seal(b"same", SECRET, &[1u8; SALT_LEN], PEM_LABEL);
        let b = seal(b"same", SECRET, &[2u8; SALT_LEN], PEM_LABEL);
        assert_ne!(a, b);
        with_secret(SECRET, || {
            assert_eq!(decrypt(&a).unwrap(), b"same");
            assert_eq!(decrypt(&b).unwrap(), b"same");
        });
    }

    /// A legacy descriptor is intact and the key is fine — it just has to be
    /// converted, and the error has to say by what.
    #[test]
    fn test_rejects_legacy_with_the_upgrade_remedy() {
        let pem = seal_legacy(b"legacy payload", SECRET);
        with_secret(SECRET, || {
            let err = decrypt(&pem).unwrap_err();
            assert_eq!(err, LEGACY_REMEDY);
            assert!(err.contains("tdx-system upgrade"));
            assert!(err.contains("tdx-domain upgrade"));
        });
    }

    #[test]
    fn test_decrypt_wrong_secret() {
        let pem = seal_current(b"payload", SECRET);
        with_secret("another-secret-entirely", || {
            assert!(decrypt(&pem).unwrap_err().contains("could not decrypt"));
        });
    }

    #[test]
    fn test_decrypt_rejects_v1_label() {
        let pem = seal_legacy(b"payload", SECRET).replace("TDX2 ", "TDX ");
        with_secret(SECRET, || {
            assert!(decrypt(&pem).unwrap_err().contains("is not the expected"));
        });
    }

    #[test]
    fn test_decrypt_not_a_descriptor() {
        with_secret(SECRET, || {
            assert!(decrypt("plain text").unwrap_err().contains("no PEM armor"));
        });
    }

    #[test]
    fn test_require_secret() {
        temp_env::with_var_unset(ENV_CRYPTO_KEY, || {
            assert!(require_secret().unwrap_err().contains(ENV_CRYPTO_KEY));
        });
        with_secret("short", || assert!(require_secret().is_err()));
        with_secret(SECRET, || assert_eq!(require_secret().unwrap(), SECRET));
    }

    /// Locks the KDF parameters against the Python writer: changing any of them
    /// silently makes every existing descriptor unreadable. Both digests come
    /// from Python's `hashlib.pbkdf2_hmac`, not from this code.
    #[test]
    fn test_key_derivation_is_stable() {
        assert_eq!(
            hex::encode(key("a-test-secret-long-enough", LEGACY_SALT)),
            "0e7b78b1af08714665c757fe4fc04383a0e626322f5ee88e70b6c0ee17c72a83"
        );
        assert_eq!(
            hex::encode(key("a-test-secret-long-enough", b"0123456789abcdef")),
            "1d73473964d0a08645ab8e76e68bfaf22fbd6827f32c835f29adf130b8e17e9f"
        );
    }
}
