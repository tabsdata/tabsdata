//
//   Copyright 2024 Tabs Data Inc.
//

//! Wrapper for axum::extract::Json to control error handling.

use crate::status::error_status::ErrorStatus;
use axum::extract::FromRequest;
use axum::extract::rejection::JsonRejection;
use axum::response::{IntoResponse, Response};
use td_error::{TdError, td_error};
use tracing::error;

#[td_error]
pub enum JsonError {
    #[error("Error deserializing JSON request: {0}")]
    JsonError(#[from] JsonRejection) = 0,
}

#[derive(FromRequest)]
#[from_request(via(axum::Json), rejection(JsonError))]
pub struct Json<T>(pub T);

impl<T> IntoResponse for Json<T>
where
    axum::Json<T>: IntoResponse,
{
    fn into_response(self) -> Response {
        axum::Json(self.0).into_response()
    }
}

impl IntoResponse for JsonError {
    fn into_response(self) -> Response {
        let error: TdError = self.into();
        error!("{}", error);
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
    #[derive(Deserialize)]
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
