//
// Copyright 2026 Tabs Data Inc.
//

use crate::topology::{
    PROVIDER_PREFIX_K, PROVIDER_PREFIX_K4S, PROVIDER_PREFIX_K8S, ProviderPrefixes,
};

use super::*;

const KUBERNETES_PREFIXES: ProviderPrefixes = ProviderPrefixes {
    keep: PROVIDER_PREFIX_K8S,
    drop: &[PROVIDER_PREFIX_K4S],
};

const KMINUS_PREFIXES: ProviderPrefixes = ProviderPrefixes {
    keep: PROVIDER_PREFIX_K4S,
    drop: &[PROVIDER_PREFIX_K8S],
};

#[test]
fn filename_for_provider_drops_dropped_prefix_for_kubernetes() {
    assert_eq!(
        filename_for_provider("k4s-runner.yaml", &KUBERNETES_PREFIXES),
        None
    );
}

#[test]
fn filename_for_provider_drops_dropped_prefix_for_kminus() {
    assert_eq!(
        filename_for_provider("k8s-runner.yaml", &KMINUS_PREFIXES),
        None
    );
}

#[test]
fn filename_for_provider_renames_kept_prefix_with_neutral_k() {
    assert_eq!(
        filename_for_provider("k8s-runner.yaml", &KUBERNETES_PREFIXES),
        Some(format!("{PROVIDER_PREFIX_K}runner.yaml"))
    );
    assert_eq!(
        filename_for_provider("k4s-runner.yaml", &KMINUS_PREFIXES),
        Some(format!("{PROVIDER_PREFIX_K}runner.yaml"))
    );
}

#[test]
fn filename_for_provider_passes_through_unprefixed_names() {
    assert_eq!(
        filename_for_provider("runner.yaml", &KUBERNETES_PREFIXES),
        Some("runner.yaml".to_owned())
    );
    assert_eq!(
        filename_for_provider("config.yaml", &KMINUS_PREFIXES),
        Some("config.yaml".to_owned())
    );
}

#[test]
fn filename_for_provider_renames_keep_with_only_prefix() {
    assert_eq!(
        filename_for_provider("k8s-", &KUBERNETES_PREFIXES),
        Some(PROVIDER_PREFIX_K.to_owned())
    );
}

#[test]
fn filename_for_provider_passes_empty_name_through() {
    assert_eq!(
        filename_for_provider("", &KUBERNETES_PREFIXES),
        Some(String::new())
    );
}

#[test]
fn filename_for_provider_is_case_sensitive() {
    // Uppercase prefix does not match the lowercase keep/drop literals.
    assert_eq!(
        filename_for_provider("K8S-runner.yaml", &KUBERNETES_PREFIXES),
        Some("K8S-runner.yaml".to_owned())
    );
}

#[test]
fn filename_for_provider_drop_takes_precedence_when_keep_overlaps() {
    let synthetic = ProviderPrefixes {
        keep: "foo-",
        drop: &["foo-bar-"],
    };
    // "foo-bar-baz" matches both "foo-bar-" (drop) and "foo-" (keep).
    // Drop must win.
    assert_eq!(filename_for_provider("foo-bar-baz", &synthetic), None);
}

#[test]
fn filename_for_provider_handles_multiple_drop_prefixes() {
    let synthetic = ProviderPrefixes {
        keep: "kept-",
        drop: &["dropA-", "dropB-", "dropC-"],
    };
    assert_eq!(filename_for_provider("dropA-x", &synthetic), None);
    assert_eq!(filename_for_provider("dropB-x", &synthetic), None);
    assert_eq!(filename_for_provider("dropC-x", &synthetic), None);
    assert_eq!(
        filename_for_provider("kept-x", &synthetic),
        Some(format!("{PROVIDER_PREFIX_K}x"))
    );
}

#[test]
fn is_valid_configmap_key_accepts_alphanumeric() {
    assert!(is_valid_configmap_key("foo"));
    assert!(is_valid_configmap_key("Bar123"));
    assert!(is_valid_configmap_key("ABC"));
}

#[test]
fn is_valid_configmap_key_accepts_dash_underscore_dot() {
    assert!(is_valid_configmap_key("foo-bar"));
    assert!(is_valid_configmap_key("foo_bar"));
    assert!(is_valid_configmap_key("foo.yaml"));
    assert!(is_valid_configmap_key("a.b-c_d.123"));
}

#[test]
fn is_valid_configmap_key_rejects_empty() {
    assert!(!is_valid_configmap_key(""));
}

#[test]
fn is_valid_configmap_key_rejects_slash() {
    assert!(!is_valid_configmap_key("foo/bar"));
    assert!(!is_valid_configmap_key("/foo"));
    assert!(!is_valid_configmap_key("foo/"));
}

#[test]
fn is_valid_configmap_key_rejects_whitespace() {
    assert!(!is_valid_configmap_key("foo bar"));
    assert!(!is_valid_configmap_key(" foo"));
    assert!(!is_valid_configmap_key("foo\t"));
}

#[test]
fn is_valid_configmap_key_rejects_special_characters() {
    assert!(!is_valid_configmap_key("foo:bar"));
    assert!(!is_valid_configmap_key("foo@bar"));
    assert!(!is_valid_configmap_key("foo+bar"));
    assert!(!is_valid_configmap_key("foo*bar"));
    assert!(!is_valid_configmap_key("foo!bar"));
}

#[test]
fn is_valid_configmap_key_rejects_non_ascii_letters() {
    assert!(!is_valid_configmap_key("café"));
    assert!(!is_valid_configmap_key("instância"));
    assert!(!is_valid_configmap_key("λ"));
}
