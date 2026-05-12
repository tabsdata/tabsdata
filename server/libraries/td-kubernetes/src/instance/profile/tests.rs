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
fn encode_path_replaces_slash_with_double_underscore() {
    assert_eq!(encode_path("pods/init/foo.yaml"), "pods__init__foo.yaml");
    assert_eq!(encode_path("config.yaml"), "config.yaml");
    assert_eq!(encode_path(""), "");
}

#[test]
fn decode_path_is_inverse_of_encode() {
    let cases = [
        "pods/init/foo.yaml",
        "config.yaml",
        "pods/regular/application/application.yaml",
        "",
    ];
    for case in cases {
        let encoded = encode_path(case);
        let decoded = decode_path(&encoded);
        assert_eq!(decoded, case);
    }
}

#[test]
fn is_valid_path_component_accepts_alphanumerics_dash_dot() {
    assert!(is_valid_path_component("foo"));
    assert!(is_valid_path_component("foo-bar"));
    assert!(is_valid_path_component("foo.yaml"));
    assert!(is_valid_path_component("k8s-initializer"));
    assert!(is_valid_path_component("apiserver-mounts-options-extractor"));
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
fn validate_path_components_accepts_full_valid_path() {
    assert!(validate_path_components("pods/init/foo.yaml").is_ok());
    assert!(validate_path_components("config.yaml").is_ok());
    assert!(
        validate_path_components("pods/regular/application/k8s-application.yaml").is_ok()
    );
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
