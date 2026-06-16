//
// Copyright 2025 Tabs Data Inc.
//

//! Helpers shared across the request-building layers: building the internal
//! `HttpRequest`s workers call back on, each carrying a signed internal-endpoint
//! token bound to that request's route + path-param ids.

use http::Method;
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use td_authz::jwt::{JwtConfig, encode_internal_token};
use td_common::worker::{Callback, HttpCallback, HttpRequest};
use td_error::td_error;
use td_objects::rest_urls::BASE_URL;
use url::Url;

#[td_error]
pub enum WorkerRequestError {
    #[error("Error parsing URL: {0}")]
    UrlParse(#[from] url::ParseError) = 5000,
    #[error("Error minting internal token: {0}")]
    Token(#[from] td_authz::jwt::JwtError) = 5001,
}

/// Build an internal `HttpRequest` for a worker to call back on. Substitutes
/// `bindings` (path-param name → value) into `pattern` to form the concrete
/// URL, then mints an internal-endpoint token bound to `(method, scope =
/// BASE_URL+pattern, bindings)` and adds it as an `Authorization: Bearer`
/// header. The internal router validates that token against the actual request.
pub(crate) fn internal_request(
    jwt: &JwtConfig,
    server_address: &SocketAddr,
    method: Method,
    pattern: &str,
    bindings: &[(&str, &str)],
    mut headers: HashMap<String, String>,
    body: bool,
) -> Result<HttpRequest, WorkerRequestError> {
    let mut endpoint = pattern.to_string();
    let mut binding_map = BTreeMap::new();
    for (name, value) in bindings {
        endpoint = endpoint.replace(&format!("{{{name}}}"), value);
        binding_map.insert(name.to_string(), value.to_string());
    }
    let url = Url::parse(&format!("http://{server_address}{BASE_URL}{endpoint}"))
        .map_err(WorkerRequestError::UrlParse)?;
    let scope = format!("{BASE_URL}{pattern}");
    let token = encode_internal_token(jwt, method.as_str(), &scope, binding_map)?;
    headers.insert("authorization".to_string(), format!("Bearer {token}"));
    Ok(HttpRequest {
        url,
        method,
        headers,
        body,
    })
}

/// Tokenized POST callback (the responder a worker reports status on).
pub(crate) fn internal_post_callback(
    jwt: &JwtConfig,
    server_address: &SocketAddr,
    pattern: &str,
    bindings: &[(&str, &str)],
) -> Result<Callback, WorkerRequestError> {
    let request: HttpCallback = internal_request(
        jwt,
        server_address,
        Method::POST,
        pattern,
        bindings,
        HashMap::new(),
        true,
    )?;
    Ok(Callback::Http(request))
}

/// Tokenized GET callback (e.g. the kubernetes resolver a pod pulls from).
pub(crate) fn internal_get_callback(
    jwt: &JwtConfig,
    server_address: &SocketAddr,
    pattern: &str,
    bindings: &[(&str, &str)],
) -> Result<Callback, WorkerRequestError> {
    let request: HttpCallback = internal_request(
        jwt,
        server_address,
        Method::GET,
        pattern,
        bindings,
        HashMap::new(),
        false,
    )?;
    Ok(Callback::Http(request))
}
