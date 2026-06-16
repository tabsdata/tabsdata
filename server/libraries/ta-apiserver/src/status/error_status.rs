//
//  Copyright 2024 Tabs Data Inc.
//

//! The error response type shared by every API endpoint.
//!
//! [`ErrorStatus`] carries the real [`StatusCode`] computed when a `TdError` is converted
//! (see `td_error_status.rs`) and emits it verbatim — any HTTP status can be returned.
//! For OpenAPI it documents a single honest `default` response; per-endpoint specifics
//! are conveyed by the body `code` field (and, optionally, the `errors(...)` argument of
//! `apiserver_path`).

use http::StatusCode;
use serde::Serialize;

/// Generic API Server Error response.
#[derive(utoipa::ToSchema, Debug, Default, Serialize)]
pub struct ErrorResponse {
    #[serde(skip_serializing)]
    pub status: StatusCode,
    pub code: String,
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_description: Option<String>,
}

#[derive(utoipa::ToSchema, utoipa::IntoResponses, serde::Serialize)]
#[response(
    status = "default",
    description = "Error response. Any HTTP error status may be returned; the `code` field identifies the specific error."
)]
pub struct ErrorStatus(ErrorResponse);

impl axum::response::IntoResponse for ErrorStatus {
    fn into_response(self) -> axum::response::Response {
        let status = self.0.status;
        (status, axum::Json(serde_json::json!(self.0))).into_response()
    }
}

impl From<ErrorResponse> for ErrorStatus {
    fn from(error: ErrorResponse) -> Self {
        ErrorStatus(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::response::IntoResponse;
    use td_error::{ApiError, TdError, api_error};

    async fn run(td_error: TdError) -> (StatusCode, serde_json::Value) {
        let response = ErrorStatus::from(td_error).into_response();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = serde_json::from_slice(&bytes).unwrap();
        (status, body)
    }

    #[tokio::test]
    async fn test_http_passthrough_returns_arbitrary_status() {
        // A status with no dedicated named class is emitted verbatim (not collapsed to 500).
        let (status, body) = run(api_error!(ApiError::Any(409), "clash")).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body["error_description"], "clash");

        let (status, _) = run(api_error!(ApiError::Any(429), "slow down")).await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    }

    #[tokio::test]
    async fn test_api_error_macro_accepts_status_code() {
        // Explicit `status =` expression form.
        let (status, body) =
            run(api_error!(status = StatusCode::LOCKED, "held by {}", "alice")).await;
        assert_eq!(status, StatusCode::LOCKED);
        assert_eq!(body["error_description"], "held by alice");

        let (status, _) =
            run(api_error!(status = StatusCode::TOO_MANY_REQUESTS, "slow down")).await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    }

    #[tokio::test]
    async fn test_api_error_macro_status_expr_is_resilient() {
        // `status = <expr>` accepts forms the bare sugar can't: full paths, `from_u16`, vars.
        let (status, _) = run(api_error!(status = http::StatusCode::CONFLICT, "clash")).await;
        assert_eq!(status, StatusCode::CONFLICT);

        let (status, _) = run(api_error!(
            status = StatusCode::from_u16(451).unwrap(),
            "blocked"
        ))
        .await;
        assert_eq!(status.as_u16(), 451);

        let chosen = StatusCode::SERVICE_UNAVAILABLE;
        let (status, body) = run(api_error!(status = chosen, "down")).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        // 5xx is masked.
        assert_eq!(body["error_description"], "An internal error occurred");
    }

    #[tokio::test]
    async fn test_inline_errors_have_unique_call_site_codes() {
        // Like typed errors' `EnumName::NNNN`, each `api_error!` call site gets a unique,
        // namespaced `code` (module path + line) so a given inline error is pinpointable.
        let (_, a) = run(api_error!(ApiError::InternalError, "one")).await;
        let (_, b) = run(api_error!(ApiError::InternalError, "two")).await;
        let ca = a["code"].as_str().unwrap();
        let cb = b["code"].as_str().unwrap();
        assert!(ca.contains("::"), "code should be namespaced: {ca}");
        assert!(
            ca.starts_with("ta_apiserver::status::error_status"),
            "code should carry the call-site module: {ca}"
        );
        assert_ne!(ca, cb, "distinct call sites must get distinct codes");
    }

    #[tokio::test]
    async fn test_client_error_status_and_message() {
        let (status, body) = run(api_error!(ApiError::InputError, "bad field")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"], "invalid_request");
        assert_eq!(body["error_description"], "bad field");
    }

    #[tokio::test]
    async fn test_internal_error_is_masked() {
        // 5xx (named or passthrough) must not leak the internal message.
        let (status, body) = run(api_error!(ApiError::InternalError, "secret detail")).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body["error_description"], "An internal error occurred");

        let (status, body) = run(api_error!(ApiError::Any(503), "db down internals")).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["error_description"], "An internal error occurred");
    }
}
