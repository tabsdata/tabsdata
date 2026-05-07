//
// Copyright 2026 Tabs Data Inc.
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
