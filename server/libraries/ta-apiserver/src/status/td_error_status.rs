//
//  Copyright 2024 Tabs Data Inc.
//

//! TdError to Error Response conversion

use crate::status::error_status::{ErrorResponse, ErrorResponseBuilder, ErrorStatus};
use http::StatusCode;
use td_error::{ApiError, TdError};

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
        ApiError::InputError => ErrorResponseBuilder::default()
            .status(StatusCode::BAD_REQUEST)
            .code(error.code())
            .error(Some(String::from("invalid_request")))
            .error_description(Some(error.to_string()))
            .build()
            .unwrap(),
        ApiError::NotFound => ErrorResponseBuilder::default()
            .status(StatusCode::NOT_FOUND)
            .code(error.code())
            .error(Some(String::from("not_found")))
            .error_description(Some(error.to_string()))
            .build()
            .unwrap(),
        ApiError::NotAllowed => ErrorResponseBuilder::default()
            .status(StatusCode::BAD_REQUEST)
            .code(error.code())
            .error(Some(String::from("not_allowed")))
            .error_description(Some(error.to_string()))
            .build()
            .unwrap(),
        ApiError::NotAuthorized => ErrorResponseBuilder::default()
            .status(StatusCode::UNAUTHORIZED)
            .code(error.code())
            .error(Some(String::from("unauthorized")))
            .error_description(Some(error.to_string()))
            .build()
            .unwrap(),
        ApiError::Forbidden => ErrorResponseBuilder::default()
            .status(StatusCode::FORBIDDEN)
            .code(error.code())
            .error(Some(String::from("forbidden")))
            .error_description(Some(error.to_string()))
            .build()
            .unwrap(),
        ApiError::InternalError => ErrorResponseBuilder::default()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .code(error.code())
            .error(Some(String::from("internal_error")))
            .error_description(Some(error.to_string()))
            .build()
            .unwrap(),
        ApiError::NotImplemented => ErrorResponseBuilder::default()
            .status(StatusCode::NOT_IMPLEMENTED)
            .code(error.code())
            .error(Some(String::from("not_implemented")))
            .error_description(Some(error.to_string()))
            .build()
            .unwrap(),
        ApiError::Unexpected => ErrorResponseBuilder::default()
            .status(StatusCode::IM_A_TEAPOT)
            .code(error.code())
            .error(Some(String::from("unexpected")))
            .error_description(Some(error.to_string()))
            .build()
            .unwrap(),
    }
    .into()
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use td_objects::entity_finder::EntityFinderError;
//
//     #[test]
//     fn test_from_crudl_error_for_default_and_not_found_error_status() {
//         let status: DefaultAndNotFoundErrorStatus =
//             TdError::CannotGetConnection(sqlx::Error::RowNotFound).into();
//         match status {
//             DefaultAndNotFoundErrorStatus::INTERNAL_SERVER_ERROR(_) => (),
//             _ => panic!("Expected INTERNAL_SERVER_ERROR status"),
//         }
//
//         let status: DefaultAndNotFoundErrorStatus =
//             TdError::CannotBeginTransaction(sqlx::Error::RowNotFound).into();
//         match status {
//             DefaultAndNotFoundErrorStatus::INTERNAL_SERVER_ERROR(_) => (),
//             _ => panic!("Expected INTERNAL_SERVER_ERROR status"),
//         }
//
//         let status: DefaultAndNotFoundErrorStatus =
//             TdError::CannotCommitCreateTransaction(sqlx::Error::RowNotFound).into();
//         match status {
//             DefaultAndNotFoundErrorStatus::INTERNAL_SERVER_ERROR(_) => (),
//             _ => panic!("Expected INTERNAL_SERVER_ERROR status"),
//         }
//
//         let status: DefaultAndNotFoundErrorStatus =
//             TdError::CannotCommitUpdateTransaction(sqlx::Error::RowNotFound).into();
//         match status {
//             DefaultAndNotFoundErrorStatus::INTERNAL_SERVER_ERROR(_) => (),
//             _ => panic!("Expected INTERNAL_SERVER_ERROR status"),
//         }
//
//         let status: DefaultAndNotFoundErrorStatus =
//             TdError::CannotCommitDeleteTransaction(sqlx::Error::RowNotFound).into();
//         match status {
//             DefaultAndNotFoundErrorStatus::INTERNAL_SERVER_ERROR(_) => (),
//             _ => panic!("Expected INTERNAL_SERVER_ERROR status"),
//         }
//
//         let status: DefaultAndNotFoundErrorStatus = TdError::CannotCreateUniqueValueExists(
//             "Cannot create, unique value exists".to_string(),
//         )
//         .into();
//         match status {
//             DefaultAndNotFoundErrorStatus::BAD_REQUEST(_) => (),
//             _ => panic!("Expected BAD_REQUEST status"),
//         }
//
//         let status: DefaultAndNotFoundErrorStatus = TdError::CannotUpdateUniqueValueExists(
//             "Cannot update, unique value exists".to_string(),
//         )
//         .into();
//         match status {
//             DefaultAndNotFoundErrorStatus::BAD_REQUEST(_) => (),
//             _ => panic!("Expected BAD_REQUEST status"),
//         }
//
//         let status: DefaultAndNotFoundErrorStatus =
//             TdError::CannotDelete("Cannot delete".to_string()).into();
//         match status {
//             DefaultAndNotFoundErrorStatus::BAD_REQUEST(_) => (),
//             _ => panic!("Expected BAD_REQUEST status"),
//         }
//
//         let status: DefaultAndNotFoundErrorStatus = TdError::NotFound.into();
//         match status {
//             DefaultAndNotFoundErrorStatus::NOT_FOUND(_) => (),
//             _ => panic!("Expected NOT_FOUND status"),
//         }
//
//         let status: DefaultAndNotFoundErrorStatus =
//             TdError::BadRequest("Bad request".to_string()).into();
//         match status {
//             DefaultAndNotFoundErrorStatus::BAD_REQUEST(_) => (),
//             _ => panic!("Expected BAD_REQUEST status"),
//         }
//
//         let status: DefaultAndNotFoundErrorStatus =
//             TdError::Forbidden("Forbidden".to_string()).into();
//         match status {
//             DefaultAndNotFoundErrorStatus::UNAUTHORIZED(_) => (),
//             _ => panic!("Expected UNAUTHORIZED status"),
//         }
//
//         let status: DefaultAndNotFoundErrorStatus =
//             TdError::NotAllowed("Not allowed".to_string()).into();
//         match status {
//             DefaultAndNotFoundErrorStatus::UNAUTHORIZED(_) => (),
//             _ => panic!("Expected UNAUTHORIZED status"),
//         }
//
//         let status: DefaultAndNotFoundErrorStatus =
//             TdError::InternalError("Internal error".to_string()).into();
//         match status {
//             DefaultAndNotFoundErrorStatus::INTERNAL_SERVER_ERROR(_) => (),
//             _ => panic!("Expected INTERNAL_SERVER_ERROR status"),
//         }
//
//         let status: DefaultAndNotFoundErrorStatus = TdError::Unauthorized.into();
//         match status {
//             DefaultAndNotFoundErrorStatus::UNAUTHORIZED(_) => (),
//             _ => panic!("Expected UNAUTHORIZED status"),
//         }
//
//         let status: DefaultAndNotFoundErrorStatus =
//             TdError::InvalidListParams("Invalid params".to_string(), "Description".to_string())
//                 .into();
//         match status {
//             DefaultAndNotFoundErrorStatus::BAD_REQUEST(_) => (),
//             _ => panic!("Expected BAD_REQUEST status"),
//         }
//
//         let status: DefaultAndNotFoundErrorStatus =
//             TdError::DependencyNotFound(EntityFinderError::NameNotFound("name".to_string()))
//                 .into();
//         match status {
//             DefaultAndNotFoundErrorStatus::NOT_FOUND(_) => (),
//             _ => panic!("Expected NOT_FOUND status"),
//         }
//     }
//
//     #[test]
//     fn test_from_crudl_error_for_default_error_status() {
//         let status: DefaultAndNotFoundErrorStatus =
//             TdError::CannotGetConnection(sqlx::Error::RowNotFound).into();
//         match status {
//             DefaultAndNotFoundErrorStatus::INTERNAL_SERVER_ERROR(_) => (),
//             _ => panic!("Expected INTERNAL_SERVER_ERROR status"),
//         }
//
//         let status: DefaultAndNotFoundErrorStatus =
//             TdError::CannotBeginTransaction(sqlx::Error::RowNotFound).into();
//         match status {
//             DefaultAndNotFoundErrorStatus::INTERNAL_SERVER_ERROR(_) => (),
//             _ => panic!("Expected INTERNAL_SERVER_ERROR status"),
//         }
//
//         let status: DefaultAndNotFoundErrorStatus =
//             TdError::CannotCommitCreateTransaction(sqlx::Error::RowNotFound).into();
//         match status {
//             DefaultAndNotFoundErrorStatus::INTERNAL_SERVER_ERROR(_) => (),
//             _ => panic!("Expected INTERNAL_SERVER_ERROR status"),
//         }
//
//         let status: DefaultAndNotFoundErrorStatus =
//             TdError::CannotCommitUpdateTransaction(sqlx::Error::RowNotFound).into();
//         match status {
//             DefaultAndNotFoundErrorStatus::INTERNAL_SERVER_ERROR(_) => (),
//             _ => panic!("Expected INTERNAL_SERVER_ERROR status"),
//         }
//
//         let status: DefaultAndNotFoundErrorStatus =
//             TdError::CannotCommitDeleteTransaction(sqlx::Error::RowNotFound).into();
//         match status {
//             DefaultAndNotFoundErrorStatus::INTERNAL_SERVER_ERROR(_) => (),
//             _ => panic!("Expected INTERNAL_SERVER_ERROR status"),
//         }
//
//         let status: DefaultErrorStatus = TdError::CannotCreateUniqueValueExists(
//             "Cannot create, unique value exists".to_string(),
//         )
//         .into();
//         match status {
//             DefaultErrorStatus::BAD_REQUEST(_) => (),
//             _ => panic!("Expected BAD_REQUEST status"),
//         }
//
//         let status: DefaultErrorStatus = TdError::CannotUpdateUniqueValueExists(
//             "Cannot update, unique value exists".to_string(),
//         )
//         .into();
//         match status {
//             DefaultErrorStatus::BAD_REQUEST(_) => (),
//             _ => panic!("Expected BAD_REQUEST status"),
//         }
//
//         let status: DefaultErrorStatus =
//             TdError::CannotDelete("Cannot delete".to_string()).into();
//         match status {
//             DefaultErrorStatus::BAD_REQUEST(_) => (),
//             _ => panic!("Expected BAD_REQUEST status"),
//         }
//
//         let status: DefaultErrorStatus = TdError::NotFound.into();
//         match status {
//             DefaultErrorStatus::INTERNAL_SERVER_ERROR(_) => (),
//             _ => panic!("Expected INTERNAL_SERVER_ERROR status"),
//         }
//
//         let status: DefaultErrorStatus = TdError::BadRequest("Bad request".to_string()).into();
//         match status {
//             DefaultErrorStatus::BAD_REQUEST(_) => (),
//             _ => panic!("Expected BAD_REQUEST status"),
//         }
//
//         let status: DefaultErrorStatus = TdError::Forbidden("Forbidden".to_string()).into();
//         match status {
//             DefaultErrorStatus::UNAUTHORIZED(_) => (),
//             _ => panic!("Expected UNAUTHORIZED status"),
//         }
//
//         let status: DefaultErrorStatus = TdError::NotAllowed("Not allowed".to_string()).into();
//         match status {
//             DefaultErrorStatus::UNAUTHORIZED(_) => (),
//             _ => panic!("Expected UNAUTHORIZED status"),
//         }
//
//         let status: DefaultErrorStatus =
//             TdError::InternalError("Internal error".to_string()).into();
//         match status {
//             DefaultErrorStatus::INTERNAL_SERVER_ERROR(_) => (),
//             _ => panic!("Expected INTERNAL_SERVER_ERROR status"),
//         }
//
//         let status: DefaultErrorStatus = TdError::Unauthorized.into();
//         match status {
//             DefaultErrorStatus::UNAUTHORIZED(_) => (),
//             _ => panic!("Expected UNAUTHORIZED status"),
//         }
//
//         let status: DefaultErrorStatus =
//             TdError::InvalidListParams("Invalid params".to_string(), "Description".to_string())
//                 .into();
//         match status {
//             DefaultErrorStatus::BAD_REQUEST(_) => (),
//             _ => panic!("Expected BAD_REQUEST status"),
//         }
//
//         let status: DefaultAndNotFoundErrorStatus =
//             TdError::DependencyNotFound(EntityFinderError::NameNotFound("name".to_string()))
//                 .into();
//         match status {
//             DefaultAndNotFoundErrorStatus::NOT_FOUND(_) => (),
//             _ => panic!("Expected NOT_FOUND status"),
//         }
//     }
// }
