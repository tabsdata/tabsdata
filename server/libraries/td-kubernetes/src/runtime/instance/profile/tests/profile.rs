//
// Copyright 2026 Tabs Data Inc.
//

use std::collections::BTreeMap;

use chrono::Utc;

use super::*;
use crate::runtime::instance::manifest::Manifest;

#[test]
fn normalize_schema_extension_rewrites_schema_yaml_to_yamls() {
    assert_eq!(
        normalize_schema_extension("apiserver.storage.schema.yaml"),
        "apiserver.storage.yamls"
    );
}

#[test]
fn normalize_schema_extension_preserves_axis_tokens() {
    assert_eq!(
        normalize_schema_extension("apiserver_docker.storage.schema.yaml"),
        "apiserver_docker.storage.yamls"
    );
}

#[test]
fn normalize_schema_extension_leaves_plain_configs_untouched() {
    assert_eq!(
        normalize_schema_extension("apiserver.storage.yaml"),
        "apiserver.storage.yaml"
    );
    assert_eq!(
        normalize_schema_extension("apiserver.yaml"),
        "apiserver.yaml"
    );
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
    assert!(!is_valid_path_component("_manifest.yaml"));
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
fn validate_path_component_returns_offender_for_invalid() {
    let bad = validate_path_component("foo_bar.yaml").expect_err("expected error");
    assert_eq!(bad, "foo_bar.yaml");
}

#[test]
fn validate_path_component_accepts_valid_filename() {
    assert!(validate_path_component("apiserver.yaml").is_ok());
    assert!(validate_path_component("config.yaml").is_ok());
}

fn profile_with_configs(configs: &[(&str, &str)]) -> Profile {
    let mut map = BTreeMap::new();
    for (key, value) in configs {
        map.insert((*key).to_owned(), (*value).to_owned());
    }
    Profile {
        pods: BTreeMap::new(),
        assets: BTreeMap::new(),
        configs: map,
        manifest: Manifest::initial_install("1.0.0", Utc::now(), std::iter::empty()),
    }
}

#[test]
fn configs_for_worker_matches_name_up_to_first_dot() {
    let profile = profile_with_configs(&[
        ("apiserver.yaml", "a"),
        ("apiserver.tracing.yaml", "b"),
        ("worker.yaml", "c"),
    ]);
    let mut found: Vec<&str> = configs_for_worker(&profile, "apiserver")
        .map(|(key, _)| key)
        .collect();
    found.sort();
    assert_eq!(found, vec!["apiserver.tracing.yaml", "apiserver.yaml"]);
}

#[test]
fn configs_for_worker_does_not_match_prefix_substring() {
    let profile = profile_with_configs(&[("apiserver.yaml", "a"), ("apiserver-extra.yaml", "b")]);
    let found: Vec<&str> = configs_for_worker(&profile, "apiserver")
        .map(|(key, _)| key)
        .collect();
    assert_eq!(found, vec!["apiserver.yaml"]);
}

#[test]
fn configs_for_worker_matches_extensionless_only_when_exact() {
    let profile = profile_with_configs(&[("apiserver", "a"), ("apiserver.yaml", "b")]);
    let mut found: Vec<&str> = configs_for_worker(&profile, "apiserver")
        .map(|(key, _)| key)
        .collect();
    found.sort();
    assert_eq!(found, vec!["apiserver", "apiserver.yaml"]);
}
