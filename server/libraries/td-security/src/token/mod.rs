//
// Copyright 2026 Tabsdata Inc.
//

//! Service token material: a prefixed CSPRNG secret and the digest
//! stored in its place. An unkeyed SHA-256, not argon2 or an HMAC:
//! [`ENTROPY_BYTES`] random bytes leave no dictionary to run, and a pepper
//! would tie every token to one more secret's lifetime.

use rand::RngExt;
use sha2::{Digest, Sha256};

pub mod config;

/// Tells a service token apart from a session token at the header.
pub const PREFIX: &str = "tdst_";

/// A new scheme (digest, length, alphabet) gets a new segment; old tokens
/// stay recognisable by shape.
pub const SCHEME: &str = "v1_";

pub const ENTROPY_BYTES: usize = 32;

/// Enough to match a found token to its slot without revoking it.
pub const HINT_LENGTH: usize = 8;

const BODY_LENGTH: usize = ENTROPY_BYTES * 2;

pub fn generate() -> String {
    let bytes: [u8; ENTROPY_BYTES] = rand::rng().random();
    format!("{PREFIX}{SCHEME}{}", hex::encode(bytes))
}

fn body(token: &str) -> Option<&str> {
    token.strip_prefix(PREFIX)?.strip_prefix(SCHEME)
}

/// Shape-checked before hashing or touching the database.
pub fn parse(presented: &str) -> Option<&str> {
    let body = body(presented)?;
    let well_formed = body.len() == BODY_LENGTH
        && body
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
    well_formed.then_some(presented)
}

pub fn hint(token: &str) -> Option<&str> {
    body(token)?.get(..HINT_LENGTH)
}

pub fn digest(token: &str) -> String {
    hex::encode(Sha256::digest(token.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::{BODY_LENGTH, HINT_LENGTH, PREFIX, SCHEME, digest, generate, hint, parse};

    #[test]
    fn test_generated_token_has_the_documented_shape() {
        let token = generate();
        let body = token
            .strip_prefix(PREFIX)
            .and_then(|rest| rest.strip_prefix(SCHEME))
            .expect("prefixed and versioned");
        assert_eq!(body.len(), BODY_LENGTH);
        assert!(
            body.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_uppercase())
        );
    }

    #[test]
    fn test_generated_tokens_differ() {
        assert_ne!(generate(), generate());
    }

    #[test]
    fn test_parse_accepts_generated_tokens() {
        let token = generate();
        assert_eq!(parse(&token), Some(token.as_str()));
    }

    #[test]
    fn test_parse_rejects_malformed_tokens() {
        let body = "a".repeat(BODY_LENGTH);
        for presented in [
            String::new(),
            PREFIX.to_string(),
            format!("{PREFIX}{SCHEME}"),
            body.clone(),
            format!("{PREFIX}{body}"),
            format!("{PREFIX}v2_{body}"),
            format!("tdps_{SCHEME}{body}"),
            format!("{PREFIX}{SCHEME}{}", "a".repeat(BODY_LENGTH - 1)),
            format!("{PREFIX}{SCHEME}{body}a"),
            format!("{PREFIX}{SCHEME}{}", "A".repeat(BODY_LENGTH)),
            format!("{PREFIX}{SCHEME}{}", "g".repeat(BODY_LENGTH)),
            format!("{PREFIX}{SCHEME}{}", " ".repeat(BODY_LENGTH)),
        ] {
            assert_eq!(parse(&presented), None, "{presented} must not parse");
        }
    }

    #[test]
    fn test_hint_is_the_head_of_the_body() {
        let token = generate();
        let head = hint(&token).expect("a well-formed token has a hint");
        assert_eq!(head.len(), HINT_LENGTH);
        assert!(token.ends_with(&token[PREFIX.len() + SCHEME.len()..]));
        assert!(token[PREFIX.len() + SCHEME.len()..].starts_with(head));
        assert_eq!(hint("nope"), None);
        assert_eq!(hint(&format!("{PREFIX}{}", "a".repeat(BODY_LENGTH))), None);
    }

    #[test]
    fn test_digest_is_hex_sha256_wide() {
        let digest = digest(&generate());
        assert_eq!(digest.len(), 64);
        assert!(digest.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_digest_is_deterministic_per_token() {
        let token = generate();
        assert_eq!(digest(&token), digest(&token));
        assert_ne!(digest(&token), digest(&generate()));
    }
}
