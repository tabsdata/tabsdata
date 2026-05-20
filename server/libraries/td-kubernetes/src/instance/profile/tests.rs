//
// Copyright 2026 Tabs Data Inc.
//

use super::*;

#[test]
fn encode_path_replaces_slash_with_double_underscore() {
    assert_eq!(encode_path("pods/init/foo.yaml"), "pods__init__foo.yaml");
    assert_eq!(encode_path("config.yaml"), "config.yaml");
    assert_eq!(encode_path(""), "");
}

#[test]
fn encode_path_replaces_backslash_with_double_underscore() {
    assert_eq!(encode_path("pods\\init\\foo.yaml"), "pods__init__foo.yaml");
}

#[test]
fn encode_path_drops_colon_and_question_mark() {
    assert_eq!(encode_path("C:/pods/foo.yaml"), "C__pods__foo.yaml");
    assert_eq!(encode_path("pods/?/foo.yaml"), "pods____foo.yaml");
    assert_eq!(encode_path("a:b?c"), "abc");
}

#[test]
fn is_valid_path_component_accepts_alphanumerics_dash_dot() {
    assert!(is_valid_path_component("foo"));
    assert!(is_valid_path_component("foo-bar"));
    assert!(is_valid_path_component("foo.yaml"));
    assert!(is_valid_path_component("apiserver"));
    assert!(is_valid_path_component(
        "apiserver-mounts-options-extractor"
    ));
    assert!(is_valid_path_component("123"));
    assert!(is_valid_path_component("a.b.c.d"));
}

#[test]
fn is_valid_path_component_rejects_underscore() {
    assert!(!is_valid_path_component("foo_bar"));
    assert!(!is_valid_path_component("manifest.yaml"));
    assert!(!is_valid_path_component("pods__init"));
}

#[test]
fn is_valid_path_component_rejects_empty() {
    assert!(!is_valid_path_component(""));
}

#[test]
fn is_valid_path_component_rejects_special_characters() {
    assert!(!is_valid_path_component("foo bar"));
    assert!(!is_valid_path_component("foo/bar"));
    assert!(!is_valid_path_component("foo:bar"));
    assert!(!is_valid_path_component("foo@bar"));
}

#[test]
fn validate_path_components_accepts_full_valid_path() {
    assert!(validate_path_components("pods/init/foo.yaml").is_ok());
    assert!(validate_path_components("config.yaml").is_ok());
    assert!(validate_path_components("pods/regular/application/application.pod.yaml").is_ok());
}

#[test]
fn validate_path_components_reports_first_bad_component() {
    let bad = validate_path_components("pods/init/foo_bar.yaml").expect_err("expected error");
    assert_eq!(bad, "foo_bar.yaml");
}

#[test]
fn validate_path_components_rejects_underscore_anywhere_in_path() {
    assert!(validate_path_components("pods/in_it/foo.yaml").is_err());
    assert!(validate_path_components("po_ds/init/foo.yaml").is_err());
}
