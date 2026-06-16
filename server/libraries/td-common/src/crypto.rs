//
// Copyright 2026 Tabs Data Inc.
//

//! Process-global rustls cryptographic provider installation.

use rustls::crypto::{CryptoProvider, aws_lc_rs, ring};
use tracing::debug;

pub const SSL_KEY_PEM_FILE: &str = "key.pem";
pub const SSL_CERT_PEM_FILE: &str = "cert.pem";

/// Ensures a process-global rustls [`CryptoProvider`] is installed, returning whether one is
/// available afterwards.
///
/// The default crypto provider is a process-global singleton. [`CryptoProvider::install_default`]
/// returns an error when a provider is already installed (e.g. by another component, or by another
/// test running in the same process), which is not a failure here: we only need *a* provider to be
/// available, not to be the one that installed it. This makes the call idempotent and safe to
/// invoke from multiple entry points, including concurrently.
pub fn install_crypto_provider() -> bool {
    if CryptoProvider::get_default().is_none() {
        if aws_lc_rs::default_provider().install_default().is_ok() {
            debug!("Successfully installed the aws-lc-rs tls cryptographic provider.");
        } else if ring::default_provider().install_default().is_ok() {
            debug!("Successfully installed the ring tls cryptographic provider.");
        }
    }
    CryptoProvider::get_default().is_some()
}
