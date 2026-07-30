//
// Copyright 2026 Tabsdata Inc.
//

/// Parses a URL filter string into a typed value.
pub trait ListFilterStr: Sized {
    fn parse_filter_str(s: &str) -> Result<Self, td_error::TdError>;
    fn as_string(&self) -> String;
}

// Option<T> parses the inner T and wraps it in Some. The empty string is the
// NULL marker, mirroring `as_string` — so a NULL keyset cursor round-trips,
// and a nullable column admitting "" must not be a list `order_by` column.
impl<T: ListFilterStr> ListFilterStr for Option<T> {
    fn parse_filter_str(s: &str) -> Result<Self, td_error::TdError> {
        if s.is_empty() {
            Ok(None)
        } else {
            T::parse_filter_str(s).map(Some)
        }
    }

    fn as_string(&self) -> String {
        if let Some(v) = self {
            v.as_string()
        } else {
            "".into()
        }
    }
}
