//
//  Copyright 2024 Tabs Data Inc.
//

//! This module generates the default error responses an API Server can generate.
//! We also create types to reuse them across the application, in different contexts.
//!
//! Http statuses are determined by the ErrorResponse Status. But the mapping between the status
//! and the actual response might differ. For example, if the response has a NOT_FOUND, but the
//! status does not allow that, it has to be converted to another status such as BAD_REQUEST.
//!
//! Default errors -> BAD_REQUEST(400), UNAUTHORIZED(401) and INTERNAL_SERVER_ERROR(500)
//! Not found with default -> NOT_FOUND(404) and default errors.

#![allow(non_camel_case_types)]

use derive_builder::Builder;
use getset::Getters;
use http::StatusCode;
use serde::Serialize;
use td_apiforge::api_server_schema;
use td_apiforge::status;

pub type AuthorizeErrorStatus = DefaultErrorStatus;
pub type ServerErrorStatus = DefaultErrorStatus;
pub type ListErrorStatus = DefaultAndNotFoundErrorStatus;
pub type GetErrorStatus = DefaultAndNotFoundErrorStatus;
pub type CreateErrorStatus = DefaultAndNotFoundErrorStatus;
pub type UpdateErrorStatus = DefaultAndNotFoundErrorStatus;
pub type DeleteErrorStatus = DefaultAndNotFoundErrorStatus;

/// Generic API Server Error response.
#[api_server_schema]
#[derive(Debug, Default, Builder, Serialize, Getters)]
#[builder(setter(into))]
#[getset(get = "pub")]
pub struct ErrorResponse {
    #[serde(skip_serializing)]
    status: StatusCode,
    code: String,
    error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error_description: Option<String>,
}

status!(
    DefaultErrorStatus,
    BAD_REQUEST => ErrorResponse,
    UNAUTHORIZED => ErrorResponse,
    INTERNAL_SERVER_ERROR => ErrorResponse,
);

impl From<ErrorResponse> for DefaultErrorStatus {
    fn from(error: ErrorResponse) -> Self {
        match error.status {
            StatusCode::BAD_REQUEST => DefaultErrorStatus::BAD_REQUEST(error),
            StatusCode::UNAUTHORIZED => DefaultErrorStatus::UNAUTHORIZED(error),
            StatusCode::FORBIDDEN => DefaultErrorStatus::UNAUTHORIZED(error),
            _ => DefaultErrorStatus::INTERNAL_SERVER_ERROR(error),
        }
    }
}

status!(
    DefaultAndNotFoundErrorStatus,
    NOT_FOUND => ErrorResponse,
    BAD_REQUEST => ErrorResponse,
    UNAUTHORIZED => ErrorResponse,
    INTERNAL_SERVER_ERROR => ErrorResponse,
);

impl From<ErrorResponse> for DefaultAndNotFoundErrorStatus {
    fn from(error: ErrorResponse) -> Self {
        match error.status {
            StatusCode::BAD_REQUEST => DefaultAndNotFoundErrorStatus::BAD_REQUEST(error),
            StatusCode::UNAUTHORIZED => DefaultAndNotFoundErrorStatus::UNAUTHORIZED(error),
            StatusCode::FORBIDDEN => DefaultAndNotFoundErrorStatus::UNAUTHORIZED(error),
            StatusCode::NOT_FOUND => DefaultAndNotFoundErrorStatus::NOT_FOUND(error),
            _ => DefaultAndNotFoundErrorStatus::INTERNAL_SERVER_ERROR(error),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::upper_case_acronyms, non_camel_case_types)]

    use super::*;
    use axum::response::IntoResponse;
    use http::StatusCode;
    use utoipa::ToSchema;

    #[derive(Serialize, ToSchema)]
    pub struct TestErrorResponse {
        error: String,
    }

    status!(
        TestErrorStatus,
        BAD_REQUEST => TestErrorResponse,
        UNAUTHORIZED => TestErrorResponse,
        INTERNAL_SERVER_ERROR => TestErrorResponse,
        NOT_FOUND => TestErrorResponse,
    );

    #[test]
    fn test_default_error_status() {
        let bad_request_response = DefaultErrorStatus::BAD_REQUEST(
            ErrorResponseBuilder::default()
                .status(StatusCode::BAD_REQUEST)
                .code("TD-X: Bad Request".to_string())
                .error(Some("Bad request".to_string()))
                .error_description(Some("Invalid input".to_string()))
                .build()
                .unwrap(),
        )
        .into_response();
        let expected_bad_request = (
            StatusCode::BAD_REQUEST,
            axum::Json(serde_json::json!(ErrorResponseBuilder::default()
                .status(StatusCode::BAD_REQUEST)
                .code("TD-X: Bad Request".to_string())
                .error(Some("Bad request".to_string()))
                .error_description(Some("Invalid input".to_string()))
                .build()
                .unwrap())),
        )
            .into_response();
        assert_eq!(bad_request_response.status(), expected_bad_request.status());

        let unauthorized_response = DefaultErrorStatus::UNAUTHORIZED(
            ErrorResponseBuilder::default()
                .status(StatusCode::UNAUTHORIZED)
                .code("TD-X: Unauthorized".to_string())
                .error(Some("Unauthorized".to_string()))
                .error_description(Some("Authentication required".to_string()))
                .build()
                .unwrap(),
        )
        .into_response();
        let expected_unauthorized = (
            StatusCode::UNAUTHORIZED,
            axum::Json(serde_json::json!(ErrorResponseBuilder::default()
                .status(StatusCode::UNAUTHORIZED)
                .code("TD-X: Unauthorized".to_string())
                .error(Some("Unauthorized".to_string()))
                .error_description(Some("Authentication required".to_string()))
                .build()
                .unwrap())),
        )
            .into_response();
        assert_eq!(
            unauthorized_response.status(),
            expected_unauthorized.status()
        );

        let internal_server_error_response = DefaultErrorStatus::INTERNAL_SERVER_ERROR(
            ErrorResponseBuilder::default()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .code("TD-X: Internal Error".to_string())
                .error(Some("Internal server error".to_string()))
                .error_description(Some("Unexpected error".to_string()))
                .build()
                .unwrap(),
        )
        .into_response();
        let expected_internal_server_error = (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(serde_json::json!(ErrorResponseBuilder::default()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .code("TD-X: Internal Error".to_string())
                .error(Some("Internal server error".to_string()))
                .error_description(Some("Unexpected error".to_string()))
                .build()
                .unwrap())),
        )
            .into_response();
        assert_eq!(
            internal_server_error_response.status(),
            expected_internal_server_error.status()
        );
    }

    #[test]
    fn test_default_and_not_found_error_status() {
        let not_found_response = DefaultAndNotFoundErrorStatus::NOT_FOUND(
            ErrorResponseBuilder::default()
                .status(StatusCode::NOT_FOUND)
                .code("TD-X: Not found".to_string())
                .error(Some("Not found".to_string()))
                .error_description(Some("Resource not found".to_string()))
                .build()
                .unwrap(),
        )
        .into_response();
        let expected_not_found = (
            StatusCode::NOT_FOUND,
            axum::Json(serde_json::json!(ErrorResponseBuilder::default()
                .status(StatusCode::NOT_FOUND)
                .code("TD-X: Not found".to_string())
                .error(Some("Not found".to_string()))
                .error_description(Some("Resource not found".to_string()))
                .build()
                .unwrap())),
        )
            .into_response();
        assert_eq!(not_found_response.status(), expected_not_found.status());

        let bad_request_response = DefaultAndNotFoundErrorStatus::BAD_REQUEST(
            ErrorResponseBuilder::default()
                .status(StatusCode::BAD_REQUEST)
                .code("TD-X: Bad Request".to_string())
                .error(Some("Bad request".to_string()))
                .error_description(Some("Invalid input".to_string()))
                .build()
                .unwrap(),
        )
        .into_response();
        let expected_bad_request = (
            StatusCode::BAD_REQUEST,
            axum::Json(serde_json::json!(ErrorResponseBuilder::default()
                .status(StatusCode::BAD_REQUEST)
                .code("TD-X: Bad Request".to_string())
                .error(Some("Bad request".to_string()))
                .error_description(Some("Invalid input".to_string()))
                .build()
                .unwrap())),
        )
            .into_response();
        assert_eq!(bad_request_response.status(), expected_bad_request.status());

        let unauthorized_response = DefaultAndNotFoundErrorStatus::UNAUTHORIZED(
            ErrorResponseBuilder::default()
                .status(StatusCode::UNAUTHORIZED)
                .code("TD-X: Unauthorized".to_string())
                .error(Some("Unauthorized".to_string()))
                .error_description(Some("Authentication required".to_string()))
                .build()
                .unwrap(),
        )
        .into_response();
        let expected_unauthorized = (
            StatusCode::UNAUTHORIZED,
            axum::Json(serde_json::json!(ErrorResponseBuilder::default()
                .status(StatusCode::UNAUTHORIZED)
                .code("TD-X: Unauthorized".to_string())
                .error(Some("Unauthorized".to_string()))
                .error_description(Some("Authentication required".to_string()))
                .build()
                .unwrap())),
        )
            .into_response();
        assert_eq!(
            unauthorized_response.status(),
            expected_unauthorized.status()
        );

        let internal_server_error_response = DefaultAndNotFoundErrorStatus::INTERNAL_SERVER_ERROR(
            ErrorResponseBuilder::default()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .code("TD-X: Internal Error".to_string())
                .error(Some("Internal server error".to_string()))
                .error_description(Some("Unexpected error".to_string()))
                .build()
                .unwrap(),
        )
        .into_response();
        let expected_internal_server_error = (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(serde_json::json!(ErrorResponseBuilder::default()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .code("TD-X: Internal Error".to_string())
                .error(Some("Internal server error".to_string()))
                .error_description(Some("Unexpected error".to_string()))
                .build()
                .unwrap())),
        )
            .into_response();
        assert_eq!(
            internal_server_error_response.status(),
            expected_internal_server_error.status()
        );
    }

    #[test]
    fn test_from_error_response_for_default_error_status() {
        let error_response = ErrorResponseBuilder::default()
            .status(StatusCode::BAD_REQUEST)
            .code("TD-X: Bad Request".to_string())
            .error(Some("Bad request".to_string()))
            .error_description(Some("Invalid input".to_string()))
            .build()
            .unwrap();
        let default_error_status: DefaultErrorStatus = error_response.into();
        match default_error_status {
            DefaultErrorStatus::BAD_REQUEST(_) => (),
            _ => panic!("Expected BAD_REQUEST status"),
        }

        let error_response = ErrorResponseBuilder::default()
            .status(StatusCode::UNAUTHORIZED)
            .code("TD-X: Bad Request".to_string())
            .error(Some("Unauthorized".to_string()))
            .error_description(Some("Authentication required".to_string()))
            .build()
            .unwrap();
        let default_error_status: DefaultErrorStatus = error_response.into();
        match default_error_status {
            DefaultErrorStatus::UNAUTHORIZED(_) => (),
            _ => panic!("Expected UNAUTHORIZED status"),
        }

        let error_response = ErrorResponseBuilder::default()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .code("TD-X: Internal Error".to_string())
            .error(Some("Internal server error".to_string()))
            .error_description(Some("Unexpected error".to_string()))
            .build()
            .unwrap();
        let default_error_status: DefaultErrorStatus = error_response.into();
        match default_error_status {
            DefaultErrorStatus::INTERNAL_SERVER_ERROR(_) => (),
            _ => panic!("Expected INTERNAL_SERVER_ERROR status"),
        }
    }
}
