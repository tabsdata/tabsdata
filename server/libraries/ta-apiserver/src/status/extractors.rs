//
//   Copyright 2024 Tabsdata Inc.
//

//! Wrapper for axum::extract::JSON to control error handling.

use crate::status::error_status::ErrorStatus;
use axum::body::Body;
use axum::extract::rejection::JsonRejection;
use axum::extract::{FromRequest, Request};
use axum::response::{IntoResponse, Response};
use std::fmt::Debug;
use td_error::{TdError, td_error};
use tracing::{error, trace};

/// Documents a raw or multipart upload body in the OpenAPI schema; emitted by
/// `#[apiserver_path]` for handlers taking `Request` or `Multipart`.
#[allow(dead_code)]
#[derive(utoipa::ToSchema)]
pub struct FileUpload(Vec<u8>);

#[td_error]
pub enum JsonError {
    #[error("Error deserializing JSON request: {0}")]
    JsonError(#[from] JsonRejection) = 0,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Json<T>(pub T);

impl<S, T> FromRequest<S> for Json<T>
where
    T: Debug,
    axum::Json<T>: FromRequest<S>,
    JsonError: From<<axum::Json<T> as FromRequest<S>>::Rejection>,
    S: Send + Sync,
{
    type Rejection = JsonError;
    async fn from_request(req: Request<Body>, state: &S) -> Result<Self, Self::Rejection> {
        <axum::Json<T> as FromRequest<_, _>>::from_request(req, state)
            .await
            .map(|axum::Json(value)| {
                // Full bodies can carry sensitive payloads (credentials,
                // conversation content, PII) — log them only at TRACE, which is
                // off in normal operation, not at the commonly-enabled DEBUG.
                trace!("JSON request:\n {:#?} \n", value);
                Json(value)
            })
            .map_err(From::from)
    }
}

impl<T> IntoResponse for Json<T>
where
    T: Debug,
    axum::Json<T>: IntoResponse,
{
    fn into_response(self) -> Response {
        let Json(value) = self;
        // See the request path above: bodies are TRACE-only, not DEBUG.
        trace!("JSON response:\n {:#?} \n", value);
        axum::Json(value).into_response()
    }
}

impl IntoResponse for JsonError {
    fn into_response(self) -> Response {
        let error: TdError = self.into();
        error!("Response error: {error:#?}");
        ErrorStatus::from(error).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::routing::post;
    use serde::Deserialize;
    use tower::ServiceExt;

    #[allow(dead_code)]
    #[derive(Debug, Deserialize)]
    struct TestPayload {
        field: String,
    }

    async fn test_handler(Json(_payload): Json<TestPayload>) -> &'static str {
        "success"
    }

    #[tokio::test]
    async fn test_valid_json() {
        let app = Router::new().route("/", post(test_handler));

        let request = Request::builder()
            .method("POST")
            .uri("/")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"field": "value"}"#))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_invalid_json() {
        let app = Router::new().route("/", post(test_handler));

        let request = Request::builder()
            .method("POST")
            .uri("/")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"field": value}"#)) // invalid JSON
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_missing_field() {
        let app = Router::new().route("/", post(test_handler));

        let request = Request::builder()
            .method("POST")
            .uri("/")
            .header("content-type", "application/json")
            .body(Body::from(r#"{}"#)) // missing required field
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}
