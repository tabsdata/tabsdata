//
// Copyright 2026 Tabs Data Inc.
//

//! `${hashicorp:path;name}` resolution — reads KV v2 secrets from a
//! Hashicorp Vault server.
//!
//! Syntax: `${hashicorp:<path>;<name>}` where `<path>` is the KV v2 secret
//! path (without the `secret/` mount prefix) and `<name>` is the key within
//! that secret. Both components are required and non-empty.
//!
//! Multiple placeholders per leaf are substituted left-to-right. The Vault
//! client is built lazily on first use and reused for the duration of a
//! single leaf resolution.

use std::collections::HashMap;

use once_cell::sync::Lazy;
use regex::Regex;
use td_error::TdError;
use vaultrs::client::{VaultClient, VaultClientSettingsBuilder};
use vaultrs::kv2;

use crate::error::gateway::ResolverError;

static PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"\$\{hashicorp:([^;]+;[^}]+)}").expect("invalid hashicorp regex"));

const KV2_MOUNT: &str = "secret";

#[derive(Debug, Clone, Default)]
pub struct HashicorpConfig {
    pub url: Option<String>,
    pub token: Option<String>,
    pub namespace: Option<String>,
}

pub(super) async fn resolve(leaf: &str, config: &HashicorpConfig) -> Result<String, TdError> {
    let mut current = leaf.to_string();
    let mut client_cache: Option<VaultClient> = None;
    while let Some(captures) = PATTERN.captures(&current) {
        let placeholder = captures.get(0).unwrap().as_str().to_string();
        let spec = captures.get(1).unwrap().as_str().to_string();
        let (path, name) = spec
            .split_once(';')
            .ok_or_else(|| ResolverError::InvalidHashicorpSpec { spec: spec.clone() })?;
        if path.is_empty() || name.is_empty() {
            return Err(ResolverError::InvalidHashicorpSpec { spec }.into());
        }
        let client = match client_cache.as_ref() {
            Some(client) => client,
            None => {
                client_cache = Some(build_client(config)?);
                client_cache.as_ref().unwrap()
            }
        };
        let secret: HashMap<String, String> =
            kv2::read(client, KV2_MOUNT, path).await.map_err(|error| {
                ResolverError::HashicorpReadFailed {
                    path: path.to_string(),
                    name: name.to_string(),
                    cause: error.to_string(),
                }
            })?;
        let value = secret
            .get(name)
            .ok_or_else(|| ResolverError::HashicorpKeyNotFound {
                path: path.to_string(),
                name: name.to_string(),
            })?;
        current = current.replace(&placeholder, value);
    }
    Ok(current)
}

fn build_client(config: &HashicorpConfig) -> Result<VaultClient, TdError> {
    let url = config
        .url
        .as_ref()
        .ok_or(ResolverError::MissingHashicorpCredentials)?;
    let token = config
        .token
        .as_ref()
        .ok_or(ResolverError::MissingHashicorpCredentials)?;
    let mut builder = VaultClientSettingsBuilder::default();
    builder.address(url).token(token);
    if let Some(namespace) = &config.namespace {
        builder.namespace(Some(namespace.clone()));
    }
    let settings = builder
        .build()
        .map_err(|error| ResolverError::HashicorpReadFailed {
            path: String::new(),
            name: String::new(),
            cause: error.to_string(),
        })?;
    VaultClient::new(settings).map_err(|error| {
        ResolverError::HashicorpReadFailed {
            path: String::new(),
            name: String::new(),
            cause: error.to_string(),
        }
        .into()
    })
}
