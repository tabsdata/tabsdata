//
// Copyright 2026 Tabs Data Inc.
//

use kube::Client;
use kube::config::Config;
use rustls::crypto::{aws_lc_rs, ring};
use td_common::status::ExitStatus::GeneralError;
use tracing::debug;

use crate::cerror;
use crate::system::exit::{Outcome, abort};

pub fn install_crypto_provider() {
    if let Err(error) = aws_lc_rs::default_provider().install_default() {
        debug!(
            "Failed to install the aws-lc-rs tls cryptographic provider: {error:?}. Falling back to the ring tls cryptographic provider."
        );
        if let Err(error) = ring::default_provider().install_default() {
            debug!("Failed to install the ring tls cryptographic provider: {error:?}.");
        } else {
            debug!("Successfully installed the ring tls cryptographic provider.");
        }
    } else {
        debug!("Successfully installed the aws-lc-rs tls cryptographic provider.");
    }
}

pub fn build_kube_client(config: &Config) -> Outcome<Client> {
    match Client::try_from(config.clone()) {
        Ok(client) => Ok(client),
        Err(error) => {
            cerror!("Failed to build kubernetes client: {error}");
            abort(GeneralError)
        }
    }
}
