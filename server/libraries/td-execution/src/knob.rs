//
// Copyright 2026 Tabsdata Inc.
//

//! Config-knob newtypes. Serde derives with `#[serde(from/into = "i64")]`
//! keep the bare-number wire format; the clamp lives in `From<i64>`.

/// A whole-seconds duration knob: serdes as `i64` seconds, `<= 0` falls
/// back to the default.
macro_rules! seconds_knob {
    ($(#[$doc:meta])* $name:ident, $default_secs:expr) => {
        $(#[$doc])*
        /// Serdes as whole seconds; `<= 0` falls back to the default.
        #[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
        #[serde(from = "i64", into = "i64")]
        pub struct $name(pub std::time::Duration);

        impl Default for $name {
            fn default() -> Self {
                Self(Self::DEFAULT)
            }
        }

        impl $name {
            pub const DEFAULT: std::time::Duration =
                std::time::Duration::from_secs($default_secs);

            pub fn from_secs(secs: i64) -> Self {
                u64::try_from(secs)
                    .ok()
                    .filter(|s| *s > 0)
                    .map(std::time::Duration::from_secs)
                    .map(Self)
                    .unwrap_or_default()
            }
        }

        impl From<i64> for $name {
            fn from(secs: i64) -> Self {
                Self::from_secs(secs)
            }
        }

        impl From<$name> for i64 {
            fn from(value: $name) -> i64 {
                value.0.as_secs() as i64
            }
        }
    };
}

/// A whole-count knob: serdes as `i64`, `<= 0` falls back to the default.
macro_rules! count_knob {
    ($(#[$doc:meta])* $name:ident, $default:expr) => {
        $(#[$doc])*
        /// Serdes as a whole count; `<= 0` falls back to the default.
        #[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
        #[serde(from = "i64", into = "i64")]
        pub struct $name(pub u64);

        impl Default for $name {
            fn default() -> Self {
                Self(Self::DEFAULT)
            }
        }

        impl $name {
            pub const DEFAULT: u64 = $default;

            pub fn from_count(count: i64) -> Self {
                u64::try_from(count)
                    .ok()
                    .filter(|c| *c > 0)
                    .map(Self)
                    .unwrap_or_default()
            }
        }

        impl From<i64> for $name {
            fn from(count: i64) -> Self {
                Self::from_count(count)
            }
        }

        impl From<$name> for i64 {
            fn from(value: $name) -> i64 {
                value.0 as i64
            }
        }
    };
}

pub(crate) use {count_knob, seconds_knob};
