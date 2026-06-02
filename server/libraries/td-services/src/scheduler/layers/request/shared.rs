//
// Copyright 2025 Tabs Data Inc.
//

//! Helpers shared across the request-building layers (callbacks, storage
//! location resolution).

use http::Method;
use std::collections::HashMap;
use td_common::server::{Callback, HttpCallback};
use td_error::{TdError, td_error};
use td_objects::Buildable;
use td_objects::dxo::yaml::location::Location;
use td_objects::rest_urls::BASE_URL;
use td_objects::types::basic::EnvPrefix;
use td_storage::{SPath, Storage};
use url::Url;

#[td_error]
pub enum WorkerRequestError {
    #[error("Error parsing URL: {0}")]
    UrlParse(#[from] url::ParseError) = 5000,
}

/// Build the loopback POST `Callback` that workers hit to report back.
pub(crate) fn loopback_post_callback(
    server_address: &std::net::SocketAddr,
    endpoint: &str,
) -> Result<Callback, WorkerRequestError> {
    let url = format!("http://{server_address}{BASE_URL}{endpoint}");
    Ok(Callback::Http(HttpCallback {
        url: Url::parse(&url).map_err(WorkerRequestError::UrlParse)?,
        method: Method::POST,
        headers: HashMap::default(),
        body: true,
    }))
}

/// Resolve `path` through `storage` and wrap it into the worker-facing
/// `Location`, returning the `EnvPrefix` separately so callers can register
/// it in their `get_states` set when needed.
pub(crate) fn build_external_location(
    storage: &Storage,
    path: &SPath,
) -> Result<(Location, EnvPrefix), TdError> {
    let (external_path, mount_def) = storage.to_external_uri(path)?;
    let env_prefix = EnvPrefix::try_from(&mount_def.id)?;
    let location = Location::builder()
        .uri(external_path)
        .env_prefix(env_prefix.clone())
        .build()?;
    Ok((location, env_prefix))
}
