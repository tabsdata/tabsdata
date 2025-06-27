//
// Copyright 2025 Tabs Data Inc.
//

use crate::router;
use crate::router::auth::AUTH_TAG;
use crate::router::state::Auth;
use crate::status::error_status::AuthorizeErrorStatus;
use axum::body::Body;
use axum::extract::State;
use axum::response::IntoResponse;
#[allow(unused_imports)]
use serde_json::json;
use td_apiforge::{apiserver_path, apiserver_schema};
use td_objects::rest_urls::CERT_DOWNLOAD;
use td_tower::ctx_service::RawOneshot;
use utoipa::IntoResponses;

router! {
    state => { Auth },
    routes => { cert_download }
}

/// This struct is just used to document PemFile in the OpenAPI schema.
/// The server is just returning a stream of bytes, so we need to specify the content type.
#[allow(dead_code)]
#[apiserver_schema]
#[derive(IntoResponses)]
#[response(
    status = 200,
    description = "OK",
    example = json!([]),
    content_type = "application/x-pem-file"
)]
pub struct PemFile(Vec<u8>);

#[apiserver_path(method = get, path = CERT_DOWNLOAD, tag = AUTH_TAG, override_response = PemFile)]
#[doc = "PEM certificate download"]
pub async fn cert_download(
    State(state): State<Auth>,
) -> Result<impl IntoResponse, AuthorizeErrorStatus> {
    let response = state.cert_download().await.raw_oneshot(()).await?;
    Ok(Body::from_stream(response.into_inner()))
}
