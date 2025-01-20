//
//  Copyright 2024 Tabs Data Inc.
//

//! This module provides a set of macros to simplify the creation of JSON responses and status enums
//! for an API server. These macros help in generating consistent and standardized responses across
//! the application.
//!
//! It does also serve as a linker between Axum responses and Utoipa docs, so they never differ.
//! Since enums cannot be extended, we dynamically generate them.
//!
//! We have 6 predefined responses, for the most common requests:
//! - List -> OK(200)
//! - Get -> OK(200)
//! - Create -> CREATED(201)
//! - Update -> OK(200)
//! - Delete -> OK(200)
//! - Login -> OK(200)
//!
//! Generic responses can be created using the [`crate::status!`] macro, if needed.

#![allow(clippy::upper_case_acronyms)]

/// Creates a JSON response with the given status code and response body.
///
/// # Examples
///
/// ```rust
/// use axum::response::IntoResponse;
/// use http::StatusCode;
/// use tabsdatalib::json_response;
///
/// let response = json_response!(StatusCode::OK, "Test response");
/// ```
#[macro_export]
macro_rules! json_response {
    ($status_code:expr, $response:expr) => {{
        ($status_code, axum::Json(serde_json::json!($response))).into_response()
    }};
    ($status_code:expr) => {{
        ($status_code,).into_response()
    }};
}

/// Defines a status enum with utoipa docs and axum response conversion with the given
/// variants and their corresponding response types.
///
/// # Examples
///
/// ```rust
/// use axum::response::IntoResponse;
/// use serde::Serialize;
/// use tabsdatalib::status;
/// use tabsdatalib::logic::apisrv::status::error_status::ErrorResponse;
/// use utoipa::ToSchema;
///
/// #[derive(Serialize, ToSchema)]
/// struct ApiResponse {
///     message: String,
/// }
///
/// status!(
///     ApiStatus,
///     (OK => ApiResponse),
///     (BAD_REQUEST => ErrorResponse),
/// );
/// ```
#[macro_export]
macro_rules! status {
    // Note that response is tt and not ty to avoid using generics in the schema.
    // Utoipa or type aliases should be used for this matter.
    ($enum_name:ident, $(($variant:ident $(=> $response:tt)?)),* $(,)?) => {
        paste::paste! {
            #[derive(utoipa::IntoResponses, serde::Serialize)]
            #[allow(dead_code)] // TODO(TD-272) remove this when all variants are used
            pub enum [ < $enum_name Status > ] {
                $(
                    #[allow(non_camel_case_types)]
                    #[response(status = StatusCode::$variant, description = stringify!($variant))]
                    $variant$(($response))?,
                )*
            }

            impl axum::response::IntoResponse for [ < $enum_name Status > ] {

                #[allow(non_snake_case)]
                fn into_response(self) -> axum::response::Response {
                    match self {
                        $(Self::$variant $( ($response) )? => $crate::json_response!(http::StatusCode::$variant $( , $response )?),)*
                    }
                }
            }
        }
    };
}

/// Defines default Login status enum.
/// It creates an OK(200) with the response, and the default errors.
///
/// # Examples
///
/// ```rust
/// use serde::Serialize;
/// use tabsdatalib::auth_status;
/// use utoipa::ToSchema;
///
/// #[derive(Serialize, ToSchema)]
/// struct ApiResponse {
///     message: String,
/// }
///
/// auth_status!(ApiResponse);
///
/// let status = AuthStatus::OK(ApiResponse { message: "".to_string() });
/// ```
#[macro_export]
macro_rules! auth_status {
    ($response:ident) => {
        $crate::status!(
            Auth,
            (OK => $response)
        );
    };
}

/// Defines default List status enum.
///
/// # Examples
///
/// ```rust
/// use serde::Serialize;
/// use tabsdatalib::list_status;
/// use utoipa::ToSchema;
///
/// #[derive(Serialize, ToSchema)]
/// struct ApiResponse {
///     message: String,
/// }
///
/// list_status!(ApiResponse);
///
/// let status = ListStatus::OK(ApiResponse { message: "".to_string() });
/// ```
#[macro_export]
macro_rules! list_status {
    ($response:ident) => {
        $crate::status!(
            List,
            (OK => $response)
        );
    };
}

/// Defines default Get status enum.
///
/// # Examples
///
/// ```rust
/// use serde::Serialize;
/// use tabsdatalib::get_status;
/// use utoipa::ToSchema;
///
/// #[derive(Serialize, ToSchema)]
/// struct ApiResponse {
///     message: String,
/// }
///
/// get_status!(ApiResponse);
///
/// let status = GetStatus::OK(ApiResponse { message: "".to_string() });
/// ```
#[macro_export]
macro_rules! get_status {
    ($response:ident) => {
        $crate::status!(
            Get,
            (OK => $response),
        );
    };
}

/// Defines default Create status enum.
///
/// # Examples
///
/// ```rust
/// use serde::Serialize;
/// use tabsdatalib::create_status;
/// use utoipa::ToSchema;
///
/// #[derive(Serialize, ToSchema)]
/// struct ApiResponse {
///     message: String,
/// }
///
/// create_status!(ApiResponse);
///
/// let status = CreateStatus::CREATED(ApiResponse { message: "".to_string() });
/// ```
#[macro_export]
macro_rules! create_status {
    ($response:ident) => {
        $crate::status!(
            Create,
            (CREATED => $response),
        );
    };
}

#[macro_export]
macro_rules! empty_create_status {
    () => {
        $crate::status!(EmptyCreate, (NO_CONTENT));
    };
}

empty_create_status!();

/// Defines default Update status enum.
///
/// # Examples
///
/// ```rust
/// use serde::Serialize;
/// use tabsdatalib::update_status;
/// use utoipa::ToSchema;
///
/// #[derive(Serialize, ToSchema)]
/// struct ApiResponse {
///     message: String,
/// }
///
/// update_status!(ApiResponse);
///
/// let status = UpdateStatus::OK(ApiResponse { message: "".to_string() });
/// ```
#[macro_export]
macro_rules! update_status {
    ($response:ident) => {
        $crate::status!(
            Update,
            (OK => $response),
        );
    };
}

#[macro_export]
macro_rules! empty_update_status {
    () => {
        $crate::status!(EmptyUpdate, (NO_CONTENT));
    };
}

empty_update_status!();

/// Defines default Delete status enum.
///
/// # Examples
///
/// ```rust
/// use serde::Serialize;
/// use tabsdatalib::delete_status;
///
/// delete_status!();
///
/// let status = DeleteStatus::NO_CONTENT;
/// ```
#[macro_export]
macro_rules! delete_status {
    () => {
        $crate::status!(Delete, (NO_CONTENT));
    };
}

delete_status!();

#[cfg(test)]
mod tests {
    use axum::response::IntoResponse;
    use http::StatusCode;
    use serde::Serialize;
    use utoipa::ToSchema;

    #[derive(Serialize, ToSchema)]
    pub struct TestResponse {
        message: String,
    }

    #[derive(Serialize, ToSchema)]
    pub struct ErrorResponse {
        error: String,
    }

    status!(
        Test,
        (OK => TestResponse),
        (BAD_REQUEST => ErrorResponse),
    );

    #[test]
    fn test_json_response() {
        let response = json_response!(StatusCode::OK, "Test response");
        let expected = (
            StatusCode::OK,
            axum::Json(serde_json::json!("Test response")),
        )
            .into_response();
        assert_eq!(response.status(), expected.status());
    }

    #[test]
    fn test_status_macro() {
        let ok_response = TestStatus::OK(TestResponse {
            message: "Success".to_string(),
        })
        .into_response();
        let expected_ok = json_response!(
            StatusCode::OK,
            TestResponse {
                message: "Success".to_string()
            }
        );
        assert_eq!(ok_response.status(), expected_ok.status());

        let error_response = TestStatus::BAD_REQUEST(ErrorResponse {
            error: "Bad request".to_string(),
        })
        .into_response();
        let expected_error = json_response!(
            StatusCode::BAD_REQUEST,
            ErrorResponse {
                error: "Bad request".to_string()
            }
        );
        assert_eq!(error_response.status(), expected_error.status());
    }

    #[test]
    fn test_list_status_macro() {
        list_status!(TestResponse);
        let list_response = ListStatus::OK(TestResponse {
            message: "List success".to_string(),
        })
        .into_response();
        let expected_list = json_response!(
            StatusCode::OK,
            TestResponse {
                message: "List success".to_string()
            }
        );
        assert_eq!(list_response.status(), expected_list.status());
    }

    #[test]
    fn test_get_status_macro() {
        get_status!(TestResponse);
        let get_response = GetStatus::OK(TestResponse {
            message: "Get success".to_string(),
        })
        .into_response();
        let expected_get = json_response!(
            StatusCode::OK,
            TestResponse {
                message: "Get success".to_string()
            }
        );
        assert_eq!(get_response.status(), expected_get.status());
    }

    #[test]
    fn test_create_status_macro() {
        create_status!(TestResponse);
        let create_response = CreateStatus::CREATED(TestResponse {
            message: "Create success".to_string(),
        })
        .into_response();
        let expected_create = json_response!(
            StatusCode::CREATED,
            TestResponse {
                message: "Create success".to_string()
            }
        );
        assert_eq!(create_response.status(), expected_create.status());
    }

    #[test]
    fn test_update_status_macro() {
        update_status!(TestResponse);
        let update_response = UpdateStatus::OK(TestResponse {
            message: "Update success".to_string(),
        })
        .into_response();
        let expected_update = json_response!(
            StatusCode::OK,
            TestResponse {
                message: "Update success".to_string()
            }
        );
        assert_eq!(update_response.status(), expected_update.status());
    }

    #[test]
    fn test_delete_status_macro() {
        delete_status!();
        let delete_response = DeleteStatus::NO_CONTENT.into_response();
        let expected_delete = json_response!(StatusCode::NO_CONTENT, ());
        assert_eq!(delete_response.status(), expected_delete.status());
    }
}
