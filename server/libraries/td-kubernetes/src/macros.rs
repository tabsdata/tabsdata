//
// Copyright 2026 Tabs Data Inc.
//

#[macro_export]
macro_rules! cerror {
    ($($arg:tt)*) => {{
        ::tracing::error!("");
        ::tracing::error!("Error: {}", ::std::format!($($arg)*));
    }};
}

#[macro_export]
macro_rules! cwarn {
    ($($arg:tt)*) => {{
        ::tracing::warn!("");
        ::tracing::warn!("Warning: {}", ::std::format!($($arg)*));
        ::tracing::warn!("");
    }};
}
