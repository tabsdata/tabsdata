//
// Copyright 2026 Tabsdata Inc.
//

//! One `filter` value of a listing: `<field>:<op>:<value>` tests a column,
//! `<entity>.<key>:<op>:<value>` a tag. Column fields never contain a dot.

use crate::list::params::ListError;
use crate::types::basic::LikeFilter;
use regex::Regex;
use std::sync::LazyLock;
use ta_objects::{TypedEnum, TypedOpenApi, TypedSerde, TypedSqlx};
use td_error::TdError;

const OPERATORS: &str = ":eq:|:ne:|:gt:|:ge:|:lt:|:le:|:lk:|:btw:|:is:";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterOp {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
    Like,
    Between,
    Is,
}

pub enum Field<'a> {
    Column(&'a str),
    Tag { entity: &'a str, key: &'a str },
}

pub struct Filter<'a> {
    pub field: Field<'a>,
    pub op: FilterOp,
    pub value: &'a str,
}

impl<'a> Filter<'a> {
    pub fn parse(s: &'a str) -> Result<Self, ListError> {
        static FILTER_REGEX: LazyLock<Regex> = LazyLock::new(|| {
            Regex::new(&format!("^(?<field>[^:]+)(?<op>{OPERATORS})(?<value>.*)$")).unwrap()
        });
        let caps = FILTER_REGEX
            .captures(s)
            .ok_or_else(|| ListError::InvalidCondition(OPERATORS.to_string(), s.to_string()))?;
        let field = caps.name("field").unwrap().as_str();
        let op = match caps.name("op").unwrap().as_str() {
            ":eq:" => FilterOp::Eq,
            ":ne:" => FilterOp::Ne,
            ":gt:" => FilterOp::Gt,
            ":ge:" => FilterOp::Ge,
            ":lt:" => FilterOp::Lt,
            ":le:" => FilterOp::Le,
            ":lk:" => FilterOp::Like,
            ":btw:" => FilterOp::Between,
            _ => FilterOp::Is,
        };
        Ok(Self {
            field: match field.split_once('.') {
                Some((entity, key)) => Field::Tag { entity, key },
                None => Field::Column(field),
            },
            op,
            value: caps.name("value").unwrap().as_str(),
        })
    }
}

#[derive(TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi)]
pub enum IsFilter {
    #[typed_enum(rename = "null")]
    Null,
    #[typed_enum(rename = "not_null")]
    NotNull,
}

/// The SQL LIKE pattern of a `*`/`.` wildcard pattern, `\` escaping either.
pub fn convert_to_like_pattern(value: &str) -> Result<LikeFilter, TdError> {
    let mut out = String::with_capacity(value.len() * 2);
    let mut escaped = false;
    for c in value.chars() {
        let mut buf = String::with_capacity(4);
        let (fragment, next_escaped) = match (c, escaped) {
            ('\\', false) => ("", true),
            ('\\', true) => ("\\\\", false),
            ('%', _) => ("\\%", false),
            ('_', _) => ("\\_", false),
            ('*', true) => ("*", false),
            ('.', true) => (".", false),
            ('*', false) => ("%", false),
            ('.', false) => ("_", false),
            (other, _) => {
                buf.push(other);
                (buf.as_str(), false)
            }
        };
        out += fragment;
        escaped = next_escaped;
    }
    out.try_into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn field(s: &str) -> String {
        match Filter::parse(s).unwrap().field {
            Field::Column(name) => name.to_string(),
            Field::Tag { entity, key } => format!("{entity}|{key}"),
        }
    }

    #[test]
    fn test_filter_parse() {
        let filter = Filter::parse("name:btw:a::b").unwrap();
        assert_eq!(filter.op, FilterOp::Between);
        assert_eq!(filter.value, "a::b");
        assert_eq!(Filter::parse("at:ge:10:00").unwrap().value, "10:00");
        for bad in ["", "a", ":eq:", "a:ff:b", "a:eq", "table.team"] {
            assert!(Filter::parse(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn test_only_dotted_fields_are_tags() {
        assert_eq!(field("function.team:eq:a"), "function|team");
        assert_eq!(field("table.eu.west:eq:1"), "table|eu.west");
        assert_eq!(field("table._parent:is:null"), "table|_parent");
        assert_eq!(field("name:eq:a.b"), "name");
        assert_eq!(field("name:lk:*.csv"), "name");
    }

    #[test]
    fn test_convert_to_like_pattern() {
        let test_cases = vec![
            ("abc", "abc"),
            ("\\abc", "abc"),
            ("a\\bc", "abc"),
            ("abc\\\\", "abc\\\\"),
            ("*abc", "%abc"),
            ("a*bc", "a%bc"),
            ("abc*", "abc%"),
            (".abc", "_abc"),
            ("a.bc", "a_bc"),
            ("abc.", "abc_"),
            ("\\*abc", "*abc"),
            ("a\\*bc", "a*bc"),
            ("abc\\*", "abc*"),
            ("\\.abc", ".abc"),
            ("a\\.bc", "a.bc"),
            ("abc\\.", "abc."),
            ("%abc", "\\%abc"),
            ("a%bc", "a\\%bc"),
            ("abc%", "abc\\%"),
            ("_abc", "\\_abc"),
            ("a_bc", "a\\_bc"),
            ("abc_", "abc\\_"),
            ("\\\\", "\\\\"),
            ("\\*", "*"),
            ("\\%", "\\%"),
            ("\\.", "."),
            ("\\_", "\\_"),
            ("_a%b*\\%c\\*d.e\\.\\_", "\\_a\\%b%\\%c*d_e.\\_"),
        ];

        for (input, expected) in test_cases {
            let result = convert_to_like_pattern(input).unwrap();
            assert_eq!(*result, expected);
        }
    }
}
