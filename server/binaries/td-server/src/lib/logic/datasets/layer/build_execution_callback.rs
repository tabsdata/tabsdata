//
//   Copyright 2024 Tabs Data Inc.
//

use http::Method;
use std::collections::HashMap;
use std::net::SocketAddr;
use td_common::server::{Callback, HttpCallbackBuilder};
use td_error::td_error;
use td_error::TdError;
use td_objects::datasets::dao::DsReadyToExecute;
use td_objects::rest_urls::{BASE_URL, UPDATE_DATA_VERSION};
use td_tower::extractors::{Input, SrvCtx};
use url::Url;

pub async fn build_execution_callback(
    SrvCtx(server_url): SrvCtx<SocketAddr>,
    Input(ds): Input<DsReadyToExecute>,
) -> Result<Callback, TdError> {
    // This is loopback address, because this endpoint is only available to the server.
    let endpoint = UPDATE_DATA_VERSION.replace("{data_version_id}", ds.data_version());
    let callback_url = format!(
        "http://127.0.0.1:{}{}{}",
        server_url.port(),
        BASE_URL,
        endpoint
    );
    let callback_url = Url::parse(&callback_url).map_err(UrlParseError::ParseError)?;

    let http_callback = HttpCallbackBuilder::default()
        .url(callback_url)
        .method(Method::POST)
        .headers(HashMap::new())
        .body(true)
        .build()
        .unwrap();

    Ok(Callback::Http(http_callback))
}

#[td_error]
pub enum UrlParseError {
    #[error("Cannot create function execution callback URI: {0}")]
    ParseError(#[from] url::ParseError) = 5000,
}
