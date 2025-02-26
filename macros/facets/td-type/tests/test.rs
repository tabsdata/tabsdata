//
//  Copyright 2024 Tabs Data Inc.
//

/// Mock td_common to test macro generation.
#[allow(dead_code)]
pub mod td_common {
    pub mod error {
        pub trait TdDomainError {
            fn domain(&self) -> &'static str;
            fn code(&self) -> String;
            fn api_error(&self) -> ApiError;
        }

        pub struct ApiError {}

        impl From<u16> for ApiError {
            fn from(_: u16) -> Self {
                Self {}
            }
        }

        #[derive(Debug)]
        pub struct TdError {}

        impl TdError {
            pub fn new(_: impl Into<TdError>) -> Self {
                Self {}
            }
        }
    }
}
