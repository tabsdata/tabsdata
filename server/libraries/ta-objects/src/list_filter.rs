//
// Copyright 2026 Tabsdata Inc.
//

/// Parses a URL filter string into a typed value.
pub trait ListFilterStr: Sized {
    fn parse_filter_str(s: &str) -> Result<Self, td_error::TdError>;
    fn as_string(&self) -> String;
}

// Option<T> parses the inner T and wraps it in Some.
impl<T: ListFilterStr> ListFilterStr for Option<T> {
    fn parse_filter_str(s: &str) -> Result<Self, td_error::TdError> {
        T::parse_filter_str(s).map(Some)
    }

    fn as_string(&self) -> String {
        if let Some(v) = self {
            v.as_string()
        } else {
            "".into()
        }
    }
}
