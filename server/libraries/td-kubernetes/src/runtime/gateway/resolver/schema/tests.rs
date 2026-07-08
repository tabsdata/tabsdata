//
// Copyright 2026 Tabs Data Inc.
//

use super::*;

const STORAGE_SCHEMA: &str = "\
directives:
  - at: storage.mounts[*].options.*
    treat: secret
";

fn path(segments: &[&str]) -> Vec<String> {
    segments.iter().map(|s| s.to_string()).collect()
}

const STORAGE_CONFIG: &str = "\
storage:
  mounts:
  - id: ROOT
    path: /
    uri: file:///data/
  - id: S3
    path: /s3
    uri: s3://bucket/
    options:
      aws_access_key_id: AKIA123
      aws_secret_access_key: topsecret
";

#[test]
fn rewrite_bakes_secret_token_into_matched_option_leaves() {
    let schema = Schema::parse("apiserver.storage.yamls", STORAGE_SCHEMA).expect("parse");
    let mut value: serde_yaml::Value = serde_yaml::from_str(STORAGE_CONFIG).expect("yaml");
    schema.rewrite_secrets(&mut value);

    let mounts = value["storage"]["mounts"].as_sequence().expect("mounts");
    let s3 = mounts
        .iter()
        .find(|m| m["id"].as_str() == Some("S3"))
        .expect("S3 mount");
    let options = &s3["options"];
    assert_eq!(options["aws_access_key_id"].as_str(), Some("${{secret}}"));
    assert_eq!(
        options["aws_secret_access_key"].as_str(),
        Some("${{secret}}")
    );

    let root = mounts
        .iter()
        .find(|m| m["id"].as_str() == Some("ROOT"))
        .expect("ROOT mount");
    assert_eq!(root["uri"].as_str(), Some("file:///data/"));
    assert_eq!(s3["uri"].as_str(), Some("s3://bucket/"));
    assert_eq!(s3["id"].as_str(), Some("S3"));
}

#[test]
fn rewrite_is_noop_for_empty_schema() {
    let schema = Schema::parse("x.yamls", "other: 1\n").expect("parse");
    let mut value: serde_yaml::Value = serde_yaml::from_str(STORAGE_CONFIG).expect("yaml");
    schema.rewrite_secrets(&mut value);
    let rendered = serde_yaml::to_string(&value).expect("render");
    assert!(!rendered.contains("${{secret}}"));
    assert!(rendered.contains("topsecret"));
}

#[test]
fn parses_storage_options_rule() {
    let schema = Schema::parse("apiserver.storage.yamls", STORAGE_SCHEMA).expect("parse");
    assert!(!schema.is_empty());
}

#[test]
fn matches_option_leaf_under_any_mount_and_key() {
    let schema = Schema::parse("apiserver.storage.yamls", STORAGE_SCHEMA).expect("parse");
    assert!(schema.secret_at(&path(&[
        "storage",
        "mounts",
        "ROOT",
        "options",
        "secret_key"
    ])));
    assert!(schema.secret_at(&path(&["storage", "mounts", "ANY", "options", "token"])));
}

#[test]
fn does_not_match_non_option_leaves() {
    let schema = Schema::parse("apiserver.storage.yamls", STORAGE_SCHEMA).expect("parse");
    assert!(!schema.secret_at(&path(&["storage", "mounts", "ROOT", "uri"])));
    assert!(!schema.secret_at(&path(&["storage", "mounts", "ROOT", "options"])));
    assert!(!schema.secret_at(&path(&["storage", "mounts"])));
}

#[test]
fn prefix_match_enables_descent_into_subtree() {
    let schema = Schema::parse("apiserver.storage.yamls", STORAGE_SCHEMA).expect("parse");
    assert!(schema.could_match_prefix(&path(&["storage"])));
    assert!(schema.could_match_prefix(&path(&["storage", "mounts"])));
    assert!(schema.could_match_prefix(&path(&["storage", "mounts", "ROOT"])));
    assert!(schema.could_match_prefix(&path(&["storage", "mounts", "ROOT", "options"])));
    assert!(!schema.could_match_prefix(&path(&["other"])));
    assert!(!schema.could_match_prefix(&path(&["storage", "buckets"])));
}

#[test]
fn absent_directives_section_is_empty_schema() {
    let schema = Schema::parse("x.yamls", "other: 1\n").expect("parse");
    assert!(schema.is_empty());
    assert!(!schema.secret_at(&path(&["storage", "mounts", "ROOT", "options", "k"])));
}

#[test]
fn empty_pattern_is_rejected() {
    let text = "directives:\n  - at: ''\n    treat: secret\n";
    let result = Schema::parse("x.yamls", text);
    assert!(matches!(result, Err(SchemaError::EmptyPattern { .. })));
}

#[test]
fn unsupported_treatment_is_rejected() {
    let text = "directives:\n  - at: a.b\n    treat: config\n";
    let result = Schema::parse("x.yamls", text);
    assert!(matches!(
        result,
        Err(SchemaError::UnsupportedTreatment { .. })
    ));
}

#[test]
fn malformed_yaml_is_rejected() {
    let result = Schema::parse("x.yamls", "directives: : :\n");
    assert!(matches!(result, Err(SchemaError::ParseFailed { .. })));
}

#[test]
fn missing_treat_is_rejected() {
    let text = "directives:\n  - at: a.b\n";
    let result = Schema::parse("x.yamls", text);
    assert!(matches!(
        result,
        Err(SchemaError::UnsupportedTreatment { .. })
    ));
}

#[test]
fn leading_wildcard_matches_any_first_segment() {
    let schema = Schema::parse(
        "x.yamls",
        "directives:\n  - at: '*.token'\n    treat: secret\n",
    )
    .expect("parse");
    assert!(schema.secret_at(&path(&["alpha", "token"])));
    assert!(schema.secret_at(&path(&["beta", "token"])));
    assert!(!schema.secret_at(&path(&["alpha", "value"])));
    assert!(!schema.secret_at(&path(&["token"])));
}

#[test]
fn mid_path_wildcard_matches_single_segment() {
    let schema =
        Schema::parse("x.yamls", "directives:\n  - at: a.*.c\n    treat: secret\n").expect("parse");
    assert!(schema.secret_at(&path(&["a", "anything", "c"])));
    assert!(!schema.secret_at(&path(&["a", "c"])));
    assert!(!schema.secret_at(&path(&["a", "x", "y", "c"])));
}

#[test]
fn pattern_longer_than_path_does_not_full_match() {
    let schema = Schema::parse("x.yamls", STORAGE_SCHEMA).expect("parse");
    assert!(!schema.secret_at(&path(&["storage", "mounts", "ROOT", "options"])));
    assert!(schema.could_match_prefix(&path(&["storage", "mounts", "ROOT", "options"])));
}

#[test]
fn list_wildcard_suffix_expands_to_literal_then_wildcard() {
    let schema = Schema::parse(
        "x.yamls",
        "directives:\n  - at: items[*].pw\n    treat: secret\n",
    )
    .expect("parse");
    assert!(schema.secret_at(&path(&["items", "0", "pw"])));
    assert!(schema.secret_at(&path(&["items", "NAME", "pw"])));
    assert!(!schema.secret_at(&path(&["items", "pw"])));
}

#[test]
fn multiple_directives_all_apply() {
    let text = "directives:\n\
                \x20 - at: a.secret\n    treat: secret\n\
                \x20 - at: b.token\n    treat: secret\n";
    let schema = Schema::parse("x.yamls", text).expect("parse");
    assert!(schema.secret_at(&path(&["a", "secret"])));
    assert!(schema.secret_at(&path(&["b", "token"])));
    assert!(!schema.secret_at(&path(&["a", "token"])));
}
