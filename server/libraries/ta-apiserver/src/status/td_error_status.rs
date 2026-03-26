//
//  Copyright 2024 Tabs Data Inc.
//

//! TdError to Error Response conversion

use crate::status::error_status::{ErrorResponse, ErrorResponseBuilder, ErrorStatus};
use http::StatusCode;
use td_error::{ApiError, TdError};
use tracing::{debug, error};

impl From<TdError> for ErrorStatus {
    fn from(error: TdError) -> Self {
        convert_error(error)
    }
}

fn convert_error<E>(error: TdError) -> E
where
    E: From<ErrorResponse>,
{
    match error.api_error() {
        ApiError::InternalError | ApiError::Unexpected | ApiError::NotImplemented => {
            error!("{}", error);
        }
        _ => {
            debug!("{}", error);
        }
    }
    match error.api_error() {
        ApiError::InputError => ErrorResponseBuilder::default()
            .status(StatusCode::BAD_REQUEST)
            .code(error.code())
            .error(Some(String::from("invalid_request")))
            .error_description(Some(error.user_message()))
            .build()
            .unwrap(),
        ApiError::NotFound => ErrorResponseBuilder::default()
            .status(StatusCode::NOT_FOUND)
            .code(error.code())
            .error(Some(String::from("not_found")))
            .error_description(Some(error.user_message()))
            .build()
            .unwrap(),
        ApiError::NotAllowed => ErrorResponseBuilder::default()
            .status(StatusCode::BAD_REQUEST)
            .code(error.code())
            .error(Some(String::from("not_allowed")))
            .error_description(Some(error.user_message()))
            .build()
            .unwrap(),
        ApiError::NotAuthorized => ErrorResponseBuilder::default()
            .status(StatusCode::UNAUTHORIZED)
            .code(error.code())
            .error(Some(String::from("unauthorized")))
            .error_description(Some(error.user_message()))
            .build()
            .unwrap(),
        ApiError::Forbidden => ErrorResponseBuilder::default()
            .status(StatusCode::FORBIDDEN)
            .code(error.code())
            .error(Some(String::from("forbidden")))
            .error_description(Some(error.user_message()))
            .build()
            .unwrap(),
        ApiError::InternalError => ErrorResponseBuilder::default()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .code(error.code())
            .error(Some(String::from("internal_error")))
            .error_description(Some(error.user_message()))
            .build()
            .unwrap(),
        ApiError::NotImplemented => ErrorResponseBuilder::default()
            .status(StatusCode::NOT_IMPLEMENTED)
            .code(error.code())
            .error(Some(String::from("not_implemented")))
            .error_description(Some(error.user_message()))
            .build()
            .unwrap(),
        ApiError::Unexpected => ErrorResponseBuilder::default()
            .status(StatusCode::IM_A_TEAPOT)
            .code(error.code())
            .error(Some(String::from("unexpected")))
            .error_description(Some(error.user_message()))
            .build()
            .unwrap(),
    }
    .into()
}
