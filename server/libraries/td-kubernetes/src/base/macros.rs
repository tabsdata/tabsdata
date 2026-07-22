//
// Copyright 2026 Tabsdata Inc.
//

#[macro_export]
macro_rules! cerror {
    ($($arg:tt)*) => {
        ::tracing::error!("\nError: {}", ::std::format_args!($($arg)*))
    };
}

#[macro_export]
macro_rules! cwarn {
    ($($arg:tt)*) => {
        ::tracing::warn!("\nWarning: {}\n", ::std::format_args!($($arg)*))
    };
}

#[macro_export]
macro_rules! command_error_from {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl From<$ty> for $crate::core::system::exit::CommandError {
                fn from(error: $ty) -> Self {
                    ::td_error::TdError::new(error).into()
                }
            }
        )+
    };
}
