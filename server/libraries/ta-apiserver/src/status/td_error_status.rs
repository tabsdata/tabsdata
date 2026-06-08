//
//  Copyright 2024 Tabs Data Inc.
//

//! TdError to Error Response conversion.
//!
//! This is the single place that bridges the transport-agnostic [`ApiError`] class to an
//! HTTP status. `td-error` itself never depends on `http`.

use crate::status::error_status::{ErrorResponse, ErrorStatus};
use http::StatusCode;
use td_error::{ApiError, TdError};
use tracing::{debug, error};

impl From<TdError> for ErrorStatus {
    fn from(error: TdError) -> Self {
        // Internal/server-side failures are logged in full and masked from the client;
        // everything else is a client-visible error logged at debug level.
        if error.api_error().is_internal() {
            error!("{error:#?}");
        } else {
            debug!("{}", error);
        }

        ErrorResponse {
            status: status_code(error.api_error()),
            code: error.code().to_string(),
            error: Some(reason_phrase(error.api_error())),
            error_description: Some(error.user_message()),
        }
        .into()
    }
}

/// Maps an [`ApiError`] class to its HTTP status. [`ApiError::Any`] passes through any
/// status code verbatim (falling back to 500 if the value is not a valid status).
fn status_code(api_error: ApiError) -> StatusCode {
    match api_error {
        ApiError::InputError => StatusCode::BAD_REQUEST,
        ApiError::NotFound => StatusCode::NOT_FOUND,
        // Historically modeled as a bad request rather than 405 Method Not Allowed.
        ApiError::NotAllowed => StatusCode::BAD_REQUEST,
        ApiError::Forbidden => StatusCode::FORBIDDEN,
        ApiError::NotAuthorized => StatusCode::UNAUTHORIZED,
        ApiError::InternalError => StatusCode::INTERNAL_SERVER_ERROR,
        ApiError::NotImplemented => StatusCode::NOT_IMPLEMENTED,
        ApiError::Unexpected => StatusCode::INTERNAL_SERVER_ERROR,
        ApiError::Any(code) => {
            StatusCode::from_u16(code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// Short machine-readable error tag for the `error` field of the response body.
fn reason_phrase(api_error: ApiError) -> String {
    let phrase = match api_error {
        ApiError::InputError => "invalid_request",
        ApiError::NotFound => "not_found",
        ApiError::NotAllowed => "not_allowed",
        ApiError::Forbidden => "forbidden",
        ApiError::NotAuthorized => "unauthorized",
        ApiError::InternalError => "internal_error",
        ApiError::NotImplemented => "not_implemented",
        ApiError::Unexpected => "unexpected",
        // Derive a tag from the canonical reason of the passed-through status.
        ApiError::Any(code) => {
            return StatusCode::from_u16(code)
                .ok()
                .and_then(|status| status.canonical_reason())
                .map(|reason| reason.to_ascii_lowercase().replace(' ', "_"))
                .unwrap_or_else(|| "error".to_string());
        }
    };
    phrase.to_string()
}
