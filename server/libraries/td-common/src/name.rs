//
// Copyright 2024 Tabs Data Inc.
//

use lazy_static::lazy_static;
use regex::Regex;

/// Maximum length for a name, 100 characters.
pub fn name_max_len() -> usize {
    const MAX_NAME_LEN: usize = 100;
    MAX_NAME_LEN
}

/// Regular expression pattern for a name.
///
/// A name is an alphanumeric word starting with a letter or an underscore, followed by letters, digits, hyphens, and underscores.
///
/// It must be at most [`name_max_len`] characters long, 100 characters.
pub fn name_regex_pattern() -> &'static str {
    lazy_static! {
        static ref NAME_REGEX: String =
            format!("[a-zA-Z_][a-zA-Z0-9_-]{{0,{}}}", name_max_len() - 1);
    }
    &NAME_REGEX
}

pub fn name_with_dot_regex_pattern() -> &'static str {
    lazy_static! {
        static ref NAME_REGEX: String =
            format!("[.a-zA-Z_][.a-zA-Z0-9_-]{{0,{}}}", name_max_len() - 1);
    }
    &NAME_REGEX
}

pub fn name_regex() -> &'static Regex {
    lazy_static! {
        static ref NAME_REGEX: Regex = Regex::new(name_regex_pattern()).unwrap();
    }
    &NAME_REGEX
}

pub fn is_valid_name(name: &str) -> bool {
    lazy_static! {
        static ref VALID_NAME_REGEX: Regex =
            Regex::new(&format!("^{}$", name_regex_pattern())).unwrap();
    }
    VALID_NAME_REGEX.is_match(name)
}

pub fn is_valid_name_with_dot(name: &str) -> bool {
    lazy_static! {
        static ref VALID_NAME_REGEX: Regex =
            Regex::new(&format!("^{}$", name_with_dot_regex_pattern())).unwrap();
    }
    VALID_NAME_REGEX.is_match(name)
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_is_valid_name() {
        assert!(super::is_valid_name("a"));
        assert!(!super::is_valid_name(" a"));
        assert!(!super::is_valid_name("a "));
    }

    #[test]
    fn test_is_valid_name_with_dot() {
        assert!(super::is_valid_name_with_dot("a"));
        assert!(!super::is_valid_name_with_dot(" a"));
        assert!(!super::is_valid_name_with_dot("a "));
        assert!(super::is_valid_name_with_dot(".a"));
        assert!(super::is_valid_name_with_dot("a."));
        assert!(super::is_valid_name_with_dot("a.a"));
        assert!(!super::is_valid_name_with_dot(" a."));
        assert!(!super::is_valid_name_with_dot(".a "));
    }
}
