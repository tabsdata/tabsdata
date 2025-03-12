//
//  Copyright 2024 Tabs Data Inc.
//

/// Mock td_common to test macro generation.
#[allow(dead_code)]
pub mod td_common {

    pub mod id {
        use serde::{Deserialize, Serialize};
        use std::fmt::Display;

        #[derive(
            Debug,
            Copy,
            Clone,
            Serialize,
            Deserialize,
            PartialEq,
            PartialOrd,
            Eq,
            Ord,
            Hash,
            Default,
        )]
        pub struct Id(i64);

        impl From<String> for Id {
            fn from(_: String) -> Self {
                Self(123)
            }
        }

        impl From<Id> for String {
            fn from(id: Id) -> String {
                id.to_string()
            }
        }

        impl Display for Id {
            fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                Ok(())
            }
        }

        pub fn id() -> Id {
            Id(123)
        }
    }

    pub mod error {
        use std::error::Error;
        use std::fmt::Display;

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

        impl Display for TdError {
            fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                Ok(())
            }
        }

        impl TdError {
            pub fn new(_: impl Into<TdError>) -> Self {
                Self {}
            }
        }

        impl Error for TdError {
            fn source(&self) -> Option<&(dyn Error + 'static)> {
                None
            }
        }
    }

    pub mod time {
        use chrono::{DateTime, Utc};

        pub struct UniqueUtc {}

        impl UniqueUtc {
            pub async fn now_millis() -> DateTime<Utc> {
                Utc::now()
            }
        }
    }
}
