//
// Copyright 2026 Tabsdata Inc.
//

//! The `tag.<key>` filters of a listing: same key → OR, different keys → AND,
//! as for columns, with the column operators that apply to a map value.

use crate::list::params::{IsFilter, convert_to_like_pattern};
use crate::types::basic::{LikeFilter, TagKey, TagValue};
use regex::Regex;
use std::str::FromStr;
use std::sync::LazyLock;
use ta_objects::Typed;
use td_error::{TdError, td_error};

pub(crate) const TAG_FILTER_PREFIX: &str = "tag.";

#[td_error]
enum TagFilterError {
    #[error(
        "Invalid tag filter '{0}', it must be tag.<KEY>:eq:<VALUE>, tag.<KEY>:ne:<VALUE>, tag.<KEY>:lk:<PATTERN> or tag.<KEY>:is:[not_]null"
    )]
    InvalidTagFilter(String) = 0,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TagTest {
    /// The tag is set to the value.
    Equals(TagValue),
    /// The tag is set, to another value.
    NotEquals(TagValue),
    /// The tag is set, to a value matching the `*`/`.` pattern.
    Like(LikeFilter),
    Present,
    Absent,
}

/// Every key's tests hold, any one of them per key.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TagSelector(pub Vec<(TagKey, Vec<TagTest>)>);

impl TagSelector {
    pub fn is_tag_filter(filter: &str) -> bool {
        filter.starts_with(TAG_FILTER_PREFIX)
    }

    pub fn parse<S: AsRef<str>>(filters: &[S]) -> Result<Self, TdError> {
        let mut selector = Self::default();
        for filter in filters {
            let (key, test) = parse_tag_filter(filter.as_ref())?;
            match selector.0.iter_mut().find(|(k, _)| *k == key) {
                Some((_, tests)) => tests.push(test),
                None => selector.0.push((key, vec![test])),
            }
        }
        Ok(selector)
    }
}

fn parse_tag_filter(s: &str) -> Result<(TagKey, TagTest), TdError> {
    static TAG_FILTER_REGEX: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new("^tag\\.(?<key>[^:]+)(?<operator>:eq:|:ne:|:lk:|:is:)(?<value>.*)$").unwrap()
    });
    let invalid = || TdError::from(TagFilterError::InvalidTagFilter(s.to_string()));
    let caps = TAG_FILTER_REGEX.captures(s).ok_or_else(invalid)?;
    let key = TagKey::try_new(caps.name("key").unwrap().as_str())?;
    let value = caps.name("value").unwrap().as_str();
    let test = match caps.name("operator").unwrap().as_str() {
        ":eq:" => TagTest::Equals(TagValue::try_new(value)?),
        ":ne:" => TagTest::NotEquals(TagValue::try_new(value)?),
        ":lk:" => TagTest::Like(convert_to_like_pattern(value)?),
        _ => match IsFilter::from_str(value).map_err(|_| invalid())? {
            IsFilter::NotNull => TagTest::Present,
            IsFilter::Null => TagTest::Absent,
        },
    };
    Ok((key, test))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(k: &str) -> TagKey {
        TagKey::try_new(k).unwrap()
    }

    fn value(v: &str) -> TagValue {
        TagValue::try_new(v).unwrap()
    }

    #[test]
    fn test_tag_filters_parse() {
        let selector = TagSelector::parse(&[
            "tag.td.parent:eq:06abc",
            "tag.team:ne:data",
            "tag.flag:is:not_null",
            "tag.gone:is:null",
            "tag.empty:eq:",
            "tag.cost-center:lk:eu/*",
        ])
        .unwrap();
        assert_eq!(
            selector.0,
            [
                (key("td.parent"), vec![TagTest::Equals(value("06abc"))]),
                (key("team"), vec![TagTest::NotEquals(value("data"))]),
                (key("flag"), vec![TagTest::Present]),
                (key("gone"), vec![TagTest::Absent]),
                (key("empty"), vec![TagTest::Equals(value(""))]),
                (
                    key("cost-center"),
                    vec![TagTest::Like(LikeFilter::try_new("eu/%").unwrap())]
                ),
            ]
        );
    }

    #[test]
    fn test_one_key_groups_its_tests() {
        let selector =
            TagSelector::parse(&["tag.team:eq:a", "tag.env:eq:prod", "tag.team:eq:b"]).unwrap();
        assert_eq!(
            selector.0,
            [
                (
                    key("team"),
                    vec![TagTest::Equals(value("a")), TagTest::Equals(value("b"))]
                ),
                (key("env"), vec![TagTest::Equals(value("prod"))]),
            ]
        );
    }

    #[test]
    fn test_malformed_tag_filters_are_rejected() {
        for bad in [
            "tag.:eq:a",
            "tag.team:gt:a",
            "tag.team:is:empty",
            "tag.team",
            "tag.team:eq",
        ] {
            assert!(TagSelector::parse(&[bad]).is_err(), "{bad}");
        }
    }

    #[test]
    fn test_only_tag_prefixed_filters_are_tag_filters() {
        assert!(TagSelector::is_tag_filter("tag.team:eq:a"));
        assert!(!TagSelector::is_tag_filter("tags:eq:a"));
        assert!(!TagSelector::is_tag_filter("name:eq:tag.x"));
    }
}
