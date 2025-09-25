//
// Copyright 2025 Tabs Data Inc.
//

use arrow::legacy::error::PolarsResult;
use grok::{Grok, Pattern, patterns};
use polars::datatypes::{DataType, Field};
use pyo3::PyResult;
use pyo3::exceptions::PyValueError;
use std::collections::BTreeMap;
use uuid::Uuid;

#[inline]
pub fn grok_schema(_: &[Field]) -> PolarsResult<Field> {
    let transient_column = format!("__tmp_{}", Uuid::new_v4().simple());
    Ok(Field::new(
        transient_column.into(),
        DataType::List(Box::new(DataType::String)),
    ))
}

#[inline]
pub fn grok_patterns() -> PyResult<BTreeMap<&'static str, &'static str>> {
    Ok(patterns().iter().copied().collect())
}

#[inline]
pub fn grok_fields(matcher: &Pattern) -> PyResult<Vec<String>> {
    Ok(matcher
        .capture_names()
        .map(|field| field.to_string())
        .collect())
}

#[inline]
pub fn grok_compile(grok: &Grok, pattern: &str) -> PyResult<Pattern> {
    let matcher = grok.compile(pattern, false).map_err(|error| {
        PyValueError::new_err(format!(
            "Failed to compile the Grok pattern '{}': {}",
            pattern, error
        ))
    })?;
    Ok(matcher)
}

#[inline]
pub fn grok_values(matcher: &Pattern, input: &str, fields: &[String]) -> Vec<Option<String>> {
    if let Some(capture) = matcher.match_against(input) {
        fields
            .iter()
            .map(|field| capture.get(field).map(|value| value.to_string()))
            .collect()
    } else {
        vec![None; fields.len()]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_grok_patterns() {
        let patterns = grok_patterns().unwrap();
        for (key, value) in &patterns {
            println!("{} => {}", key, value);
        }
        println!("Total patterns: {}", patterns.len());
    }

    #[test]
    fn test_grok_compile_valid_pattern() {
        let grok = Grok::default();
        let pattern = "%{WORD:word}";
        let capture = grok_compile(&grok, pattern);
        assert!(capture.is_ok());
        let matcher = capture.unwrap();
        let names: Vec<_> = matcher.capture_names().collect();
        assert!(names.contains(&"word"));
    }

    #[test]
    fn test_name_and_alias_basic() {
        let grok = Grok::default();
        let matcher = grok_compile(&grok, r"%{WORD} %{WORD:user} %{NUMBER:n1}").unwrap();
        let fields = matcher.capture_names().collect::<Vec<_>>();
        assert_eq!(fields, vec!["BASE10NUM", "WORD", "n1", "user"]);
        let line = "root alice 42";
        let capture = matcher.match_against(line).expect("Unmatched!");
        assert_eq!(capture.get("WORD"), Some("root"));
        assert_eq!(capture.get("user"), Some("alice"));
        assert_eq!(capture.get("n1"), Some("42"));
    }

    #[test]
    fn test_extract_sub_capture_only() {
        let grok = Grok::default();
        let matcher = grok_compile(&grok, r"%{SYSLOGPROG::pid}").unwrap();
        let fields = matcher.capture_names().collect::<Vec<_>>();
        assert_eq!(fields, vec!["SYSLOGPROG", "pid", "program"]);
        let line = "sshd[1234]";
        let capture = matcher.match_against(line).expect("Unmatched!");
        assert_eq!(capture.get("pid"), Some("1234"));
    }

    #[test]
    fn test_extract_with_alias() {
        let grok = Grok::default();
        let matcher = grok_compile(&grok, r"%{SYSLOGPROG:process_id:pid}").unwrap();
        let fields = matcher.capture_names().collect::<Vec<_>>();
        assert_eq!(fields, vec!["pid", "process_id", "program"]);
        let line = "sshd[9876]";
        let capture = matcher.match_against(line).expect("Unmatched!");
        assert_eq!(capture.get("process_id"), Some("sshd[9876]"));
        assert_eq!(capture.get("pid"), Some("9876"));
    }

    #[test]
    fn tets_inline_definition_definition_slot() {
        let grok = Grok::default();
        let matcher = grok_compile(&grok, r"^%{PORT:port=\b\d\d(?:\d(?:\d(?:\d)?)?)?\b}$").unwrap();
        let fields = matcher.capture_names().collect::<Vec<_>>();
        assert_eq!(fields, vec!["port"]);
        let capture = matcher.match_against("8080").expect("Unmatched!");
        assert_eq!(capture.get("port"), Some("8080"));
        assert!(matcher.match_against("123456").is_none());
    }

    #[test]
    fn test_multiple_columns_common_patterns() {
        let grok = Grok::default();
        let matcher = grok_compile(
            &grok,
            r"%{TIMESTAMP_ISO8601:ts} %{IPV4:ip} %{EMAILADDRESS:email} %{NUMBER:num}",
        )
        .unwrap();
        let fields = matcher.capture_names().collect::<Vec<_>>();
        assert_eq!(
            fields,
            vec![
                "BASE10NUM",
                "EMAILLOCALPART",
                "HOSTNAME",
                "HOUR",
                "HOUR[1]",
                "ISO8601_TIMEZONE",
                "MINUTE",
                "MINUTE[1]",
                "MONTHDAY",
                "MONTHNUM",
                "SECOND",
                "YEAR",
                "email",
                "ip",
                "num",
                "ts"
            ]
        );
        let line = "2016-09-19T18:19:00 8.8.8.8 user@example.com 3.14";
        let capture = matcher.match_against(line).expect("should match");
        assert_eq!(capture.get("ts"), Some("2016-09-19T18:19:00"));
        assert_eq!(capture.get("ip"), Some("8.8.8.8"));
        assert_eq!(capture.get("email"), Some("user@example.com"));
        assert_eq!(capture.get("num"), Some("3.14"));
    }

    #[test]
    fn test_alias_without_name_collision() {
        let grok = Grok::default();
        let matcher = grok_compile(&grok, r"%{WORD:first} %{WORD:second}").unwrap();
        let fields = matcher.capture_names().collect::<Vec<_>>();
        assert_eq!(fields, vec!["first", "second"]);
        let capture = matcher.match_against("alpha beta").expect("should match");
        assert_eq!(capture.get("first"), Some("alpha"));
        assert_eq!(capture.get("second"), Some("beta"));
        assert_eq!(capture.get("WORD"), None);
    }

    #[test]
    fn two_consecutive_syslogprog_programs() {
        let grok = Grok::default();
        let matcher = grok_compile(&grok, r"^%{SYSLOGPROG:p1}\s+%{SYSLOGPROG:p2}$").unwrap();
        let fields = matcher.capture_names().collect::<Vec<_>>();
        assert_eq!(
            fields,
            vec!["p1", "p2", "pid", "pid[1]", "program", "program[1]"]
        );

        let line_good = "sshd[1234] nginx[42]";
        let capture_good = matcher.match_against(line_good).expect("should match");
        assert_eq!(capture_good.get("p1"), Some("sshd[1234]"));
        assert_eq!(capture_good.get("p2"), Some("nginx[42]"));

        let line_bad = "sshd[1234]";
        assert!(matcher.match_against(line_bad).is_none());

        let line_half = "cron syslogd";
        let capture_half = matcher.match_against(line_half).expect("should match");
        assert_eq!(capture_half.get("p1"), Some("cron"));
        assert_eq!(capture_half.get("p2"), Some("syslogd"));
    }

    #[test]
    fn test_grok_parse_function() {
        let grok = Grok::default();
        let matcher = grok_compile(&grok, r"%{WORD:word} %{NUMBER:num}").unwrap();
        let capture_names = matcher.capture_names().collect::<Vec<_>>();
        let capture_names_string: Vec<String> =
            capture_names.iter().map(|s| s.to_string()).collect();

        let capture = grok_values(&matcher, "hello 42", &capture_names_string);
        assert!(capture.contains(&Some("hello".to_string())));
        assert!(capture.contains(&Some("42".to_string())));

        let capture = grok_values(&matcher, "no match", &capture_names_string);
        assert!(capture.iter().all(|item| item.is_none()));
    }

    #[test]
    fn test_grok_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        fn assert_static<T: 'static>() {}
        assert_send_sync::<Pattern>();
        assert_static::<Pattern>();
    }
}
