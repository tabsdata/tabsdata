//
// Copyright 2026 Tabsdata Inc.
//

use super::*;

#[test]
fn a_plain_name_is_left_alone() {
    assert_eq!(label_value("my_collection"), "my_collection");
}

#[test]
fn a_connection_suffix_is_left_alone() {
    assert_eq!(
        label_value("PostgresCdcStreamSrcConn"),
        "PostgresCdcStreamSrcConn"
    );
}

#[test]
fn illegal_characters_become_dashes() {
    assert_eq!(
        label_value("tabsdatak.conn.mysql:MySQLSrcConn"),
        "tabsdatak.conn.mysql-MySQLSrcConn"
    );
}

#[test]
fn a_value_over_the_limit_is_truncated() {
    let value = "a".repeat(LABEL_VALUE_MAX_LENGTH + 20);
    assert_eq!(label_value(&value).len(), LABEL_VALUE_MAX_LENGTH);
}

#[test]
fn truncation_never_leaves_a_trailing_separator() {
    let mut value = "a".repeat(LABEL_VALUE_MAX_LENGTH - 1);
    value.push_str("-tail");
    let sanitised = label_value(&value);
    assert!(sanitised.len() <= LABEL_VALUE_MAX_LENGTH);
    assert!(sanitised.ends_with('a'), "{sanitised}");
}

#[test]
fn leading_and_trailing_separators_are_dropped() {
    assert_eq!(label_value("-_.name._-"), "name");
}

#[test]
fn an_empty_value_stays_empty() {
    assert_eq!(label_value(""), "");
}

#[test]
fn a_value_of_only_separators_becomes_empty() {
    assert_eq!(label_value(":::"), "");
}
