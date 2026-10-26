//
// Copyright 2026 Tabsdata Inc.
//

//! The tag filters of a listing, `<entity>.<key>:<op>:<value>`: the entity
//! names a taggable id the list's DTO marks `tags`, so on the functions list
//! `function.team` tests each function's own tags and on the runs list it
//! tests the tags of each run's function. Same entity and key → OR, otherwise
//! AND, as for columns.

use crate::list::params::{IsFilter, convert_to_like_pattern};
use crate::types::basic::{LikeFilter, TagKey, TagValue};
use regex::Regex;
use sea_query::Expr;
use std::str::FromStr;
use std::sync::LazyLock;
use ta_objects::{Dao, ListDto, Typed};
use td_error::{TdError, td_error};

#[td_error]
enum TagFilterError {
    #[error(
        "Invalid tag filter '{0}', it must be <ENTITY>.<KEY>:eq:<VALUE>, <ENTITY>.<KEY>:ne:<VALUE>, <ENTITY>.<KEY>:lk:<PATTERN> or <ENTITY>.<KEY>:is:[not_]null"
    )]
    InvalidTagFilter(String) = 0,
    #[error("Tag filter '{0}' names no entity of this list; it can filter by the tags of: {1}")]
    UnknownEntity(String, String) = 1,
    #[error("Tag filter '{0}': this list takes no tag filters")]
    NotTagged(String) = 2,
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

/// One entity's key and its tests, any one of which holds.
#[derive(Debug, Clone)]
pub struct TagTerm<I> {
    pub column: I,
    pub entity_type: Expr,
    pub entity: String,
    pub key: TagKey,
    pub tests: Vec<TagTest>,
}

/// Every term holds.
#[derive(Debug, Clone)]
pub struct TagSelector<I>(pub Vec<TagTerm<I>>);

impl<I> Default for TagSelector<I> {
    fn default() -> Self {
        Self(Vec::new())
    }
}

impl<I> TagSelector<I> {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// A filter whose field is dotted, `<entity>.<key>`, tests tags: column
/// fields never contain a dot.
pub fn is_tag_filter(filter: &str) -> bool {
    filter
        .split(':')
        .next()
        .is_some_and(|field| field.contains('.'))
}

impl<I: Clone> TagSelector<I> {
    pub fn parse<T, S>(filters: &[S]) -> Result<Self, TdError>
    where
        T: ListDto,
        T::Dao: Dao<Iden = I>,
        S: AsRef<str>,
    {
        let mut selector = Self::default();
        for filter in filters {
            let filter = filter.as_ref();
            let (entity, key, test) = parse_tag_filter(filter)?;
            let Some((column, entity_type)) = T::tag_col(&entity) else {
                Err(match T::tag_entities() {
                    [] => TagFilterError::NotTagged(filter.to_string()),
                    entities => {
                        TagFilterError::UnknownEntity(filter.to_string(), entities.join(", "))
                    }
                })?
            };
            match selector
                .0
                .iter_mut()
                .find(|term| term.entity == entity && term.key == key)
            {
                Some(term) => term.tests.push(test),
                None => selector.0.push(TagTerm {
                    column,
                    entity_type,
                    entity,
                    key,
                    tests: vec![test],
                }),
            }
        }
        Ok(selector)
    }
}

fn parse_tag_filter(s: &str) -> Result<(String, TagKey, TagTest), TdError> {
    static TAG_FILTER_REGEX: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(
            "^(?<entity>[a-z]+)\\.(?<key>[^:]+)(?<operator>:eq:|:ne:|:lk:|:is:)(?<value>.*)$",
        )
        .unwrap()
    });
    let invalid = || TdError::from(TagFilterError::InvalidTagFilter(s.to_string()));
    let caps = TAG_FILTER_REGEX.captures(s).ok_or_else(invalid)?;
    let entity = caps.name("entity").unwrap().as_str().to_string();
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
    Ok((entity, key, test))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dxo::service_token::TokenRead;
    use crate::dxo::table::Table;

    fn parse(
        filters: &[&str],
    ) -> Result<TagSelector<<<Table as ta_objects::FromDao>::Dao as Dao>::Iden>, TdError> {
        TagSelector::parse::<Table, _>(filters)
    }

    fn terms(filters: &[&str]) -> Vec<(String, String, Vec<TagTest>)> {
        parse(filters)
            .unwrap()
            .0
            .into_iter()
            .map(|t| (t.entity, t.key.to_string(), t.tests))
            .collect()
    }

    fn value(v: &str) -> TagValue {
        TagValue::try_new(v).unwrap()
    }

    fn term(entity: &str, key: &str, tests: Vec<TagTest>) -> (String, String, Vec<TagTest>) {
        (entity.to_string(), key.to_string(), tests)
    }

    #[test]
    fn test_tag_filters_parse() {
        assert_eq!(
            terms(&[
                "table._parent:eq:06abc",
                "function.team:ne:data",
                "table.flag:is:not_null",
                "collection.gone:is:null",
                "table.empty:eq:",
                "table.cost-center:lk:eu/*",
                "table.eu.west:eq:1",
            ]),
            [
                term("table", "_parent", vec![TagTest::Equals(value("06abc"))]),
                term("function", "team", vec![TagTest::NotEquals(value("data"))]),
                term("table", "flag", vec![TagTest::Present]),
                term("collection", "gone", vec![TagTest::Absent]),
                term("table", "empty", vec![TagTest::Equals(value(""))]),
                term(
                    "table",
                    "cost-center",
                    vec![TagTest::Like(LikeFilter::try_new("eu/%").unwrap())]
                ),
                term("table", "eu.west", vec![TagTest::Equals(value("1"))]),
            ]
        );
    }

    #[test]
    fn test_one_entity_key_groups_its_tests() {
        assert_eq!(
            terms(&["table.team:eq:a", "function.team:eq:c", "table.team:eq:b"]),
            [
                term(
                    "table",
                    "team",
                    vec![TagTest::Equals(value("a")), TagTest::Equals(value("b"))]
                ),
                term("function", "team", vec![TagTest::Equals(value("c"))]),
            ]
        );
    }

    #[test]
    fn test_malformed_tag_filters_are_rejected() {
        for bad in [
            "table.:eq:a",
            "table.team:gt:a",
            "table.team:is:empty",
            "table.team",
            "table.team:eq",
        ] {
            assert!(parse(&[bad]).is_err(), "{bad}");
        }
    }

    #[test]
    fn test_an_entity_the_list_does_not_mark_is_rejected() {
        let err = parse(&["group.team:eq:a"]).unwrap_err().to_string();
        assert!(err.contains("can filter by the tags of"), "{err}");
        for entity in ["collection", "table", "function"] {
            assert!(err.contains(entity), "{err}");
        }
        assert!(TagSelector::parse::<TokenRead, _>(&["function.team:eq:a"]).is_err());
    }

    #[test]
    fn test_only_dotted_fields_are_tag_filters() {
        assert!(is_tag_filter("function.team:eq:a"));
        assert!(is_tag_filter("table._parent:is:null"));
        assert!(!is_tag_filter("name:eq:a.b"));
        assert!(!is_tag_filter("name:lk:*.csv"));
    }
}
