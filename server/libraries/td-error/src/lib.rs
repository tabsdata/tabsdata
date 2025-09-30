//
// Copyright 2025 Tabs Data Inc.
//

pub mod display_vec;

pub use tm_error::td_error;

use derive_builder::UninitializedFieldError;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use strum::AsRefStr;

/// Error class enum for conversion to API errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, AsRefStr)]
pub enum ApiError {
    /// Discriminants from 0 to 999 are reserved for input errors
    InputError = 0,
    /// Discriminants from 1000 to 1999 are reserved for not found errors
    NotFound = 1000,
    /// Discriminants from 2000 to 2999 are reserved for not allowed errors
    NotAllowed = 2000,
    /// Discriminants from 3000 to 3999 are reserved for not forbidden errors
    Forbidden = 3000,
    /// Discriminants from 4000 to 4999 are reserved for authorization errors
    NotAuthorized = 4000,
    /// Discriminants from 5000 to 5999 are reserved for internal errors
    InternalError = 5000,
    /// Discriminants from 6000 to 6999 are reserved for not implemented errors
    NotImplemented = 6000,
    /// Discriminants from 7000 to u16::MAX are unexpected
    Unexpected = u16::MAX as isize,
}

impl From<u16> for ApiError {
    fn from(discriminant: u16) -> Self {
        match discriminant {
            i if i < Self::InputError as u16 + 1000 => Self::InputError,
            i if i < Self::NotFound as u16 + 1000 => Self::NotFound,
            i if i < Self::NotAllowed as u16 + 1000 => Self::NotAllowed,
            i if i < Self::Forbidden as u16 + 1000 => Self::Forbidden,
            i if i < Self::NotAuthorized as u16 + 1000 => Self::NotAuthorized,
            i if i < Self::InternalError as u16 + 1000 => Self::InternalError,
            i if i < Self::NotImplemented as u16 + 1000 => Self::NotImplemented,
            _i => Self::Unexpected,
        }
    }
}

impl Display for ApiError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

/// Trait implemented by tabsdata errors. This trait is automatically implemented by
/// error enums annotated with the [`#td_error`] macro.
pub trait TdDomainError: Error + Send + Sync {
    /// Returns the domain of the error. The name of the enum type is the domain error.
    fn domain(&self) -> &str;

    /// Returns the error code, the [`Self::domain()`] concatenated with the variant discriminant.
    fn code(&self) -> String;

    /// Returns the API error type of the error.
    fn api_error(&self) -> ApiError;
}

/// Generic tabsdata error type to be returned when there is no need to use a specific error type,
/// and the error should be propagated up the call stack.
///
/// [`TdDomainError`] errors are automatically converted to [`TdError`] using the '?' operator.
#[derive(Debug)]
pub struct TdError {
    domain: String,
    code: String,
    api_error: ApiError,
    td_error: anyhow::Error,
}

impl TdError {
    /// Creates a new [`TdError`] from an error implementing [`TdDomainError`].
    ///
    /// This constructor should not be used directly, instead use the '?' operator to convert
    /// or a `.map_err(TdError::from)` to convert a [`Result<T, impl TdDomainError>`].
    pub fn new<E>(error: E) -> Self
    where
        E: TdDomainError + 'static,
    {
        Self {
            domain: error.domain().to_string(),
            code: error.code(),
            api_error: error.api_error(),
            td_error: anyhow::Error::new(error),
        }
    }

    /// Returns the domain of the error.
    pub fn domain(&self) -> &str {
        &self.domain
    }

    /// Returns the error code of the error.
    pub fn code(&self) -> &str {
        &self.code
    }

    /// Returns the API error class of the error.
    pub fn api_error(&self) -> ApiError {
        self.api_error
    }

    /// Downcasts to the source [`TdDomainError`].
    pub fn domain_err<E: TdDomainError + 'static>(&self) -> &E {
        self.source().unwrap().downcast_ref::<E>().unwrap()
    }
}

impl Display for TdError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "td::error {}[{}] - {}",
            self.api_error(),
            self.code(),
            self.td_error
        )
    }
}

impl Error for TdError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.td_error.as_ref())
    }
}

// useful for types using derive_builder
impl From<UninitializedFieldError> for TdError {
    fn from(ufe: UninitializedFieldError) -> TdError {
        TdError {
            domain: "UninitializedFieldError".to_string(),
            code: "UninitializedFieldError::0000".to_string(),
            api_error: ApiError::InternalError,
            td_error: anyhow::Error::new(ufe),
        }
    }
}

/// Macro to create an inline error with a specific API error code. This macro is used to create
/// errors without the need to define a specific error type.
#[macro_export]
macro_rules! api_error {
    ($api_error:expr, $($arg:tt)*) => {{
        $crate::TdError::new($crate::InlineError::new(
            format!($($arg)*),
            format!(
                "{}:{}[{}]",
                module_path!(),
                file!(),
                line!(),
            ),
            format!("Error::{:04}", $api_error as u16),
            $api_error,
        ))
    }};
}

pub struct InlineError {
    msg: String,
    domain: String,
    code: String,
    api_error: ApiError,
}

impl InlineError {
    /// Creates a new inline error with the given message, domain, code, and API error.
    pub fn new(msg: String, domain: String, code: String, api_error: ApiError) -> Self {
        Self {
            msg,
            domain,
            code,
            api_error,
        }
    }
}

impl Display for InlineError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.msg)
    }
}

impl Debug for InlineError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "InlineError({})", self.msg)
    }
}

impl Error for InlineError {}

impl TdDomainError for InlineError {
    fn domain(&self) -> &str {
        &self.domain
    }

    fn code(&self) -> String {
        self.code.clone()
    }

    fn api_error(&self) -> ApiError {
        self.api_error
    }
}

/// Asserts a service invocation error.
///
/// The invocation of the given `service` with the given `request` is expected to fail.
///
/// The `assert_error` function is called with service invocation [`TdDomainError`] error.
/// The function should assert the error variant.
pub async fn assert_service_error<Req, Res, Err: TdDomainError + 'static>(
    service: tower::util::BoxService<Req, Res, TdError>,
    request: Req,
    assert_error: impl Fn(&Err),
) {
    match tower::ServiceExt::oneshot(service, request).await {
        Ok(_) => panic!("Service is expected to error"),
        Err(err) => match err.source().unwrap().downcast_ref::<Err>() {
            Some(err) => assert_error(err),
            _ => panic!(
                "source expected '{}', but got '{:?}'",
                std::any::type_name::<Err>(),
                err.source()
            ),
        },
    }
}

#[cfg(test)]
mod tests {
    use crate as td_error;

    use super::*;
    use std::error::Error;
    use td_error::td_error;

    #[test]
    fn test_api_error_ranges() {
        assert_eq!(ApiError::InputError as u16, 0);
        assert_eq!(ApiError::NotFound as u16, 1000);
        assert_eq!(ApiError::NotAllowed as u16, 2000);
        assert_eq!(ApiError::Forbidden as u16, 3000);
        assert_eq!(ApiError::NotAuthorized as u16, 4000);
        assert_eq!(ApiError::InternalError as u16, 5000);
        assert_eq!(ApiError::NotImplemented as u16, 6000);
        assert_eq!(ApiError::Unexpected as u16, u16::MAX);

        assert_eq!(ApiError::from(0), ApiError::InputError);
        assert_eq!(ApiError::from(999), ApiError::InputError);
        assert_eq!(ApiError::from(1000), ApiError::NotFound);
        assert_eq!(ApiError::from(1999), ApiError::NotFound);
        assert_eq!(ApiError::from(2000), ApiError::NotAllowed);
        assert_eq!(ApiError::from(2999), ApiError::NotAllowed);
        assert_eq!(ApiError::from(3000), ApiError::Forbidden);
        assert_eq!(ApiError::from(3999), ApiError::Forbidden);
        assert_eq!(ApiError::from(4000), ApiError::NotAuthorized);
        assert_eq!(ApiError::from(4999), ApiError::NotAuthorized);
        assert_eq!(ApiError::from(5000), ApiError::InternalError);
        assert_eq!(ApiError::from(5999), ApiError::InternalError);
        assert_eq!(ApiError::from(6000), ApiError::NotImplemented);
        assert_eq!(ApiError::from(6999), ApiError::NotImplemented);
        assert_eq!(ApiError::from(7000), ApiError::Unexpected);
        assert_eq!(ApiError::from(u16::MAX), ApiError::Unexpected);
    }

    #[td_error]
    #[derive(Clone, PartialEq, Eq)]
    pub enum MyErrorA {
        #[error("A0")]
        A0 = 0,
        #[error("A1({0})")]
        A1(String) = 1000,
    }

    #[td_error]
    #[derive(Clone, PartialEq, Eq)]
    pub enum MyErrorB {
        #[error("B0")]
        B0 = 0,

        #[error("B1({0})")]
        // using an arbitrary discriminant to verify it is correctly assigned to the shadow enum
        B1(#[from] MyErrorA) = 1,
    }

    #[allow(unused)]
    fn f_returning_typed_error() -> Result<(), MyErrorB> {
        Err(MyErrorB::B0)
    }

    #[allow(unused)]
    fn f_early_exit_with_error_conversion() -> Result<(), TdError> {
        f_returning_typed_error()?;
        Ok(())
    }

    #[allow(unused)]
    fn f_with_explicit_typed_error_conversion() -> Result<(), TdError> {
        f_returning_typed_error().map_err(TdError::from)
    }

    #[test]
    fn test_td_error() {
        let error_a = MyErrorA::A1("foo".to_string());
        let error_b = MyErrorB::B1(error_a.clone());
        let td_error = TdError::new(error_b.clone());

        assert_eq!(td_error.domain(), "MyErrorB");
        assert_eq!(td_error.code(), "MyErrorB::0001");
        assert!(matches!(td_error.api_error(), ApiError::InputError));

        td_error
            .source()
            .unwrap()
            .downcast_ref::<MyErrorB>()
            .unwrap();
    }
}
