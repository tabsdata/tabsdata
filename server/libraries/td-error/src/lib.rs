//
// Copyright 2025 Tabsdata Inc.
//

mod display_vec;

pub use {display_vec::DisplayVec, display_vec_macro as display_vec, tm_error::td_error};

use derive_builder::UninitializedFieldError;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use strum::AsRefStr;

/// Error class enum for conversion to API errors.
///
/// This is the transport-agnostic seam between domain errors and the outside world: it
/// carries the *semantic* class of an error, never an HTTP type. Transports map it to
/// their own representation (the API server maps it to an HTTP status; the Kubernetes
/// layer maps it to a process exit code). `td-error` therefore does NOT depend on `http`.
///
/// The named variants cover the common classes; [`ApiError::Any`] is a passthrough that
/// lets a domain error select an explicit status code that has no dedicated named class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, AsRefStr)]
pub enum ApiError {
    /// Invalid input (range fallback: discriminants 0 to 999).
    InputError,
    /// Resource not found (range fallback: 1000 to 1999).
    NotFound,
    /// Operation not allowed (range fallback: 2000 to 2999).
    NotAllowed,
    /// Forbidden (range fallback: 3000 to 3999).
    Forbidden,
    /// Authorization error (range fallback: 4000 to 4999).
    NotAuthorized,
    /// Internal error (range fallback: 5000 to 5999).
    InternalError,
    /// Not implemented (range fallback: 6000 to 6999).
    NotImplemented,
    /// Unexpected / invariant violation (range fallback: 7000 to 7999, and 9000 up).
    Unexpected,
    /// Temporarily unavailable, worth retrying: the database was busy or gave
    /// up waiting (range fallback: 8000 to 8999).
    Unavailable,
    /// Passthrough for an explicit code with no dedicated named class.
    ///
    /// `td-error` treats the value as an opaque integer and does not ascribe any transport
    /// meaning to it; each transport interprets it (e.g. the API server treats it as an HTTP
    /// status). This lets a caller select any code without it having to be enumerated as a
    /// named class here.
    Any(u16),
}

impl ApiError {
    /// Coarse predicate: does this class represent an internal/server-side failure?
    ///
    /// Used to mask user-facing messages, pick the log level, and classify process exit
    /// codes — so consumers do not have to enumerate every variant.
    pub fn is_internal(&self) -> bool {
        match self {
            ApiError::InternalError | ApiError::NotImplemented | ApiError::Unexpected => true,
            // `Any` holds a raw code; treat >= 500 as server-side (the usual convention)
            // for masking and log-level purposes.
            ApiError::Any(code) => *code >= 500,
            _ => false,
        }
    }
}

impl From<u16> for ApiError {
    /// Maps a `#[td_error]` variant's discriminant to its error class by range. Never returns
    /// [`ApiError::Any`].
    fn from(discriminant: u16) -> Self {
        match discriminant {
            i if i < 1000 => Self::InputError,
            i if i < 2000 => Self::NotFound,
            i if i < 3000 => Self::NotAllowed,
            i if i < 4000 => Self::Forbidden,
            i if i < 5000 => Self::NotAuthorized,
            i if i < 6000 => Self::InternalError,
            i if i < 7000 => Self::NotImplemented,
            i if i < 8000 => Self::Unexpected,
            i if i < 9000 => Self::Unavailable,
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

    /// Returns a user-safe error message suitable for API responses.
    /// - For InternalError / Unexpected / NotImplemented: returns a generic message.
    /// - For all other error classes: returns the domain error's Display message.
    pub fn user_message(&self) -> String {
        if self.api_error.is_internal() {
            "An internal error occurred".to_string()
        } else {
            self.td_error.to_string()
        }
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
///
/// It accepts any of:
/// - an [`ApiError`] class: `api_error!(ApiError::NotFound, "...")`
/// - any status expression: `api_error!(status = StatusCode::LOCKED, "...")`,
///   `api_error!(status = http::StatusCode::from_u16(x)?, "...")`
///
/// The status form lowers to the [`ApiError::Any`] passthrough. Every `StatusCode`/`http`
/// reference is resolved in the *caller's* scope (which already has `http` to name it), so
/// `td-error` itself never depends on `http`.
///
/// Each invocation gets a unique `code` derived from its call site (`module_path!()::line!()`),
/// mirroring how `#[td_error]` enums give every variant a unique `EnumName::NNNN` identifier.
/// The public arms forward to a single `@build` arm; the location macros there resolve to the
/// original call site (inherited through the one-level forward).
#[macro_export]
macro_rules! api_error {
    // Explicit status expression — resilient to full paths, `from_u16`, variables, etc.
    (status = $status:expr, $($arg:tt)*) => {
        $crate::api_error!(@build $crate::ApiError::Any(($status).as_u16()), $($arg)*)
    };
    // An `ApiError` class (InputError, NotFound, ...).
    ($api_error:expr, $($arg:tt)*) => {
        $crate::api_error!(@build $api_error, $($arg)*)
    };
    // Internal builder shared by the arms above.
    (@build $api_error:expr, $($arg:tt)*) => {{
        $crate::TdError::new($crate::InlineError {
            msg: format!($($arg)*),
            domain: format!("{}:{}[{}]", module_path!(), file!(), line!()),
            code: format!("{}::{}", module_path!(), line!()),
            api_error: $api_error,
        })
    }};
}

pub struct InlineError {
    pub msg: String,
    pub domain: String,
    pub code: String,
    pub api_error: ApiError,
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

/// Asserts that a service invocation failed with domain error `Err`: the
/// caller invokes the service and passes the result; `assert_error` asserts
/// the variant.
pub fn assert_service_error<T, Err: TdDomainError + 'static>(
    result: Result<T, TdError>,
    assert_error: impl Fn(&Err),
) {
    match result {
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

    #[test]
    fn test_api_error_http_passthrough() {
        // `From<u16>` is the legacy range fallback and never yields `Any`.
        assert!(!matches!(ApiError::from(429), ApiError::Any(_)));
        assert!(matches!(ApiError::from(429), ApiError::InputError));
    }

    #[test]
    fn test_api_error_is_internal() {
        assert!(ApiError::InternalError.is_internal());
        assert!(ApiError::NotImplemented.is_internal());
        assert!(ApiError::Unexpected.is_internal());
        assert!(!ApiError::InputError.is_internal());
        assert!(!ApiError::NotFound.is_internal());
        assert!(!ApiError::NotAuthorized.is_internal());
        // Any passthrough is internal iff it is a 5xx status.
        assert!(ApiError::Any(503).is_internal());
        assert!(ApiError::Any(500).is_internal());
        assert!(!ApiError::Any(409).is_internal());
        assert!(!ApiError::Any(429).is_internal());
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
