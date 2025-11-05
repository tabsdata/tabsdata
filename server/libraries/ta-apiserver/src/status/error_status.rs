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

#![allow(non_camel_case_types, clippy::upper_case_acronyms)]

use derive_builder::Builder;
use getset::Getters;
use http::StatusCode;
use serde::Serialize;

/// Generic API Server Error response.
#[derive(utoipa::ToSchema, Debug, Default, Builder, Serialize, Getters)]
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

#[derive(utoipa::ToSchema, utoipa::IntoResponses, serde::Serialize)]
pub enum ErrorStatus {
    #[response(status = StatusCode::NOT_FOUND, description = "NOT_FOUND")]
    NOT_FOUND(ErrorResponse),
    #[response(status = StatusCode::BAD_REQUEST, description = "BAD_REQUEST")]
    BAD_REQUEST(ErrorResponse),
    #[response(status = StatusCode::UNAUTHORIZED, description = "UNAUTHORIZED")]
    UNAUTHORIZED(ErrorResponse),
    #[response(status = StatusCode::FORBIDDEN, description = "FORBIDDEN")]
    FORBIDDEN(ErrorResponse),
    #[response(status = StatusCode::INTERNAL_SERVER_ERROR, description = "INTERNAL_SERVER_ERROR")]
    INTERNAL_SERVER_ERROR(ErrorResponse),
}

impl axum::response::IntoResponse for ErrorStatus {
    fn into_response(self) -> axum::response::Response {
        let (status, error) = match self {
            ErrorStatus::NOT_FOUND(e) => (StatusCode::NOT_FOUND, e),
            ErrorStatus::BAD_REQUEST(e) => (StatusCode::BAD_REQUEST, e),
            ErrorStatus::UNAUTHORIZED(e) => (StatusCode::UNAUTHORIZED, e),
            ErrorStatus::FORBIDDEN(e) => (StatusCode::FORBIDDEN, e),
            ErrorStatus::INTERNAL_SERVER_ERROR(e) => (StatusCode::INTERNAL_SERVER_ERROR, e),
        };
        (status, axum::Json(serde_json::json!(error))).into_response()
    }
}

impl From<ErrorResponse> for ErrorStatus {
    fn from(error: ErrorResponse) -> Self {
        match error.status {
            StatusCode::NOT_FOUND => ErrorStatus::NOT_FOUND(error),
            StatusCode::BAD_REQUEST => ErrorStatus::BAD_REQUEST(error),
            StatusCode::UNAUTHORIZED => ErrorStatus::UNAUTHORIZED(error),
            StatusCode::FORBIDDEN => ErrorStatus::FORBIDDEN(error),
            _ => ErrorStatus::INTERNAL_SERVER_ERROR(error),
        }
    }
}
