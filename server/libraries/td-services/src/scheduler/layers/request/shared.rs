//
// Copyright 2025 Tabs Data Inc.
//

//! Helpers shared across the request-building layers (loopback callbacks).

use http::Method;
use std::collections::HashMap;
use td_common::server::{Callback, HttpCallback};
use td_error::td_error;
use td_objects::rest_urls::BASE_URL;
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
