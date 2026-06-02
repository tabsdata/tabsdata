//
// Copyright 2026 Tabs Data Inc.
//

//! Random-value helpers shared across crates that must agree byte-for-byte
//! on what counts as a generated value (e.g. embedded postgres superuser
//! password, kubernetes generator placeholders). Centralising here keeps
//! the algorithms identical wherever they're invoked.

use rand::RngExt;
use rand::distr::Alphanumeric;

/// Length, in characters, of the alphanumeric password produced by
/// [`password`]. The embedded postgres superuser uses exactly this length;
/// kubernetes `${secret:postgres_password}` placeholders inherit the same shape.
pub const PASSWORD_LENGTH: usize = 32;

/// Generates a random alphanumeric password of [`PASSWORD_LENGTH`]
/// characters using the thread-local RNG.
pub fn password() -> String {
    (&mut rand::rng())
        .sample_iter(Alphanumeric)
        .take(PASSWORD_LENGTH)
        .map(char::from)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn password_length_matches_constant() {
        assert_eq!(password().len(), PASSWORD_LENGTH);
    }

    #[test]
    fn password_is_alphanumeric() {
        let value = password();
        assert!(
            value
                .chars()
                .all(|character| character.is_ascii_alphanumeric())
        );
    }

    #[test]
    fn password_varies_across_calls() {
        let first = password();
        let second = password();
        assert_ne!(first, second);
    }
}
