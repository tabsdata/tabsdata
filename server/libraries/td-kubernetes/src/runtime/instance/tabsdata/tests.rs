//
// Copyright 2026 Tabsdata Inc.
//

use super::*;

#[test]
fn an_empty_document_yields_no_pods_global_envs() {
    let config = TabsdataConfig::parse("").expect("parses");
    assert!(config.pods.global.envs.is_empty());
}

#[test]
fn a_document_without_the_pods_section_yields_no_pods_global_envs() {
    let config = TabsdataConfig::parse("other:\n  key: value\n").expect("parses");
    assert!(config.pods.global.envs.is_empty());
}

#[test]
fn an_empty_envs_map_yields_no_pods_global_envs() {
    let config = TabsdataConfig::parse("pods:\n  global:\n    envs: {}\n").expect("parses");
    assert!(config.pods.global.envs.is_empty());
}

#[test]
fn text_numbers_and_flags_all_render_as_strings() {
    let config = TabsdataConfig::parse(
        "pods:\n  global:\n    envs:\n      TD_TRACE: verbose\n      TD_RETRIES: 3\n      TD_STRICT: true\n",
    )
    .expect("parses");
    let envs = config.pods.global.envs;
    assert_eq!(envs["TD_TRACE"].render(), "verbose");
    assert_eq!(envs["TD_RETRIES"].render(), "3");
    assert_eq!(envs["TD_STRICT"].render(), "true");
}

#[test]
fn a_non_scalar_env_value_fails_to_parse() {
    let outcome =
        TabsdataConfig::parse("pods:\n  global:\n    envs:\n      TD_TRACE:\n        nested: v\n");
    assert!(outcome.is_err());
}

#[test]
fn unknown_sections_are_tolerated_so_the_file_can_grow() {
    let config = TabsdataConfig::parse(
        "pods:\n  global:\n    envs:\n      TD_TRACE: verbose\nfuture:\n  section: value\n",
    )
    .expect("parses");
    assert_eq!(config.pods.global.envs["TD_TRACE"].render(), "verbose");
}

#[test]
fn a_pods_section_without_global_yields_no_envs() {
    let config = TabsdataConfig::parse("pods:\n  other: value\n").expect("parses");
    assert!(config.pods.global.envs.is_empty());
}

#[test]
fn a_global_section_without_envs_yields_no_envs() {
    let config = TabsdataConfig::parse("pods:\n  global:\n    other: value\n").expect("parses");
    assert!(config.pods.global.envs.is_empty());
}

#[test]
fn a_null_envs_entry_yields_no_envs() {
    let config = TabsdataConfig::parse("pods:\n  global:\n    envs:\n").expect("parses");
    assert!(config.pods.global.envs.is_empty());
}

#[test]
fn a_file_holding_only_a_comment_yields_no_envs() {
    let config = TabsdataConfig::parse("#\n# Copyright 2026 Tabsdata Inc.\n#\n").expect("parses");
    assert!(config.pods.global.envs.is_empty());
}

#[test]
fn shipped_configs_parse() {
    for provider in ["kminus", "docker", "eks", "fargate"] {
        let path = format!(
            "{}/../../../variant/resources/kprofile/configs/tabsdata_{provider}.yaml",
            env!("CARGO_MANIFEST_DIR")
        );
        let yaml = std::fs::read_to_string(&path).expect(&path);
        let config = TabsdataConfig::parse(&yaml).expect(&path);

        assert!(
            !config.pods.function.resources.is_empty(),
            "{provider} function has no resources"
        );
        assert!(
            !config.pods.service.defaults.resources.is_empty(),
            "{provider} service has no default resources"
        );
        assert!(
            !config
                .component("apiserver", Some("regular"))
                .resources
                .is_empty(),
            "{provider} apiserver"
        );
        assert!(
            !config
                .component("function-publisher", Some("ephemeral"))
                .resources
                .is_empty(),
            "{provider} function fallback"
        );
        assert!(
            !config
                .component("venvloader", Some("init"))
                .resources
                .is_empty(),
            "{provider} service fallback"
        );
    }
}
