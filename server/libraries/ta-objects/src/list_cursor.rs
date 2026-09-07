//
// Copyright 2026 Tabsdata Inc.
//

/// NULL marker for keyset cursor values. `~` is URL-unreserved, so it crosses
/// query-string transport verbatim and is never percent-encoded; it is
/// non-empty, so query deserializers cannot drop it as an absent parameter.
pub const NULL_CURSOR: &str = "~";

/// Parses a URL filter string into a typed value.
pub trait ListFilterStr: Sized {
    fn parse_filter_str(s: &str) -> Result<Self, td_error::TdError>;
    fn as_string(&self) -> String;
}

// Option<T> parses the inner T and wraps it in Some. A NULL serializes as the
// `~` marker; a real value starting with `~` is escaped with one extra `~` so
// no column value can be mistaken for the marker (an unescaped `~`-leading
// string still parses verbatim — only the `~~` escape form is stripped). The
// empty string also parses as NULL, so the filter layer can reject it with a
// pointer to `:is:null`.
impl<T: ListFilterStr> ListFilterStr for Option<T> {
    fn parse_filter_str(s: &str) -> Result<Self, td_error::TdError> {
        if s.is_empty() || s == NULL_CURSOR {
            Ok(None)
        } else {
            let s = match s.strip_prefix(NULL_CURSOR) {
                Some(escaped) if escaped.starts_with(NULL_CURSOR) => escaped,
                _ => s,
            };
            T::parse_filter_str(s).map(Some)
        }
    }

    fn as_string(&self) -> String {
        match self {
            Some(v) => {
                let s = v.as_string();
                if s.starts_with(NULL_CURSOR) {
                    format!("{NULL_CURSOR}{s}")
                } else {
                    s
                }
            }
            None => NULL_CURSOR.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl ListFilterStr for String {
        fn parse_filter_str(s: &str) -> Result<Self, td_error::TdError> {
            Ok(s.to_string())
        }

        fn as_string(&self) -> String {
            self.clone()
        }
    }

    #[test]
    fn test_nullable_round_trip() {
        for value in [
            None,
            Some("value".to_string()),
            Some("~".to_string()),
            Some("~~".to_string()),
            Some("~value".to_string()),
        ] {
            let cursor = value.as_string();
            assert!(!cursor.is_empty());
            assert_eq!(Option::<String>::parse_filter_str(&cursor).unwrap(), value);
        }
    }

    #[test]
    fn test_unescaped_value_parses_verbatim() {
        assert_eq!(
            Option::<String>::parse_filter_str("~value").unwrap(),
            Some("~value".to_string())
        );
    }
}
