//
// Copyright 2026 Tabsdata Inc.
//

use std::path::PathBuf;

use indoc::indoc;
use strum::IntoEnumIterator;
use tm_workspace::workspace_root;

use super::*;
use crate::runtime::provider::ClusterProvider;

#[test]
fn an_empty_document_yields_no_pods_global_envs() {
    let config = TabsdataConfig::parse("").expect("parses");
    assert!(config.pods.global.envs.is_empty());
}

#[test]
fn a_document_without_the_pods_section_yields_no_pods_global_envs() {
    let config = TabsdataConfig::parse(indoc! {"
        other:
          key: value
    "})
    .expect("parses");
    assert!(config.pods.global.envs.is_empty());
}

#[test]
fn an_empty_envs_map_yields_no_pods_global_envs() {
    let config = TabsdataConfig::parse(indoc! {"
        pods:
          global:
            envs: {}
    "})
    .expect("parses");
    assert!(config.pods.global.envs.is_empty());
}

#[test]
fn text_numbers_and_flags_all_render_as_strings() {
    let config = TabsdataConfig::parse(indoc! {"
            pods:
              global:
                envs:
                  TD_TRACE: verbose
                  TD_RETRIES: 3
                  TD_STRICT: true
        "})
    .expect("parses");
    let envs = config.pods.global.envs;
    assert_eq!(envs["TD_TRACE"].render().as_deref(), Some("verbose"));
    assert_eq!(envs["TD_RETRIES"].render().as_deref(), Some("3"));
    assert_eq!(envs["TD_STRICT"].render().as_deref(), Some("true"));
}

#[test]
fn an_unspecified_env_value_renders_as_nothing() {
    let config = TabsdataConfig::parse(indoc! {r#"
            pods:
              global:
                envs:
                  TD_UNSET:
                  TD_EMPTY: ""
        "#})
    .expect("parses");
    let envs = config.pods.global.envs;
    assert_eq!(envs["TD_UNSET"].render(), None);
    assert_eq!(envs["TD_EMPTY"].render().as_deref(), Some(""));
}

#[test]
fn a_non_scalar_env_value_fails_to_parse() {
    let outcome = TabsdataConfig::parse(indoc! {"
            pods:
              global:
                envs:
                  TD_TRACE:
                    nested: v
        "});
    assert!(outcome.is_err());
}

#[test]
fn unknown_sections_are_tolerated_so_the_file_can_grow() {
    let config = TabsdataConfig::parse(indoc! {"
            pods:
              global:
                envs:
                  TD_TRACE: verbose
            future:
              section: value
        "})
    .expect("parses");
    assert_eq!(
        config.pods.global.envs["TD_TRACE"].render().as_deref(),
        Some("verbose")
    );
}

#[test]
fn a_pods_section_without_global_yields_no_envs() {
    let config = TabsdataConfig::parse(indoc! {"
        pods:
          other: value
    "})
    .expect("parses");
    assert!(config.pods.global.envs.is_empty());
}

#[test]
fn a_global_section_without_envs_yields_no_envs() {
    let config = TabsdataConfig::parse(indoc! {"
        pods:
          global:
            other: value
    "})
    .expect("parses");
    assert!(config.pods.global.envs.is_empty());
}

#[test]
fn a_null_envs_entry_yields_no_envs() {
    let config = TabsdataConfig::parse(indoc! {"
        pods:
          global:
            envs:
    "})
    .expect("parses");
    assert!(config.pods.global.envs.is_empty());
}

#[test]
fn a_file_holding_only_a_comment_yields_no_envs() {
    let config = TabsdataConfig::parse(indoc! {"
        #
        # Copyright 2026 Tabsdata Inc.
        #
    "})
    .expect("parses");
    assert!(config.pods.global.envs.is_empty());
}

#[test]
fn shipped_configs_parse() {
    for provider in ClusterProvider::iter().map(ClusterProvider::as_str) {
        let path = PathBuf::from(workspace_root!()).join(format!(
            "variant/resources/kprofile/configs/tabsdata_{provider}.yaml"
        ));
        let yaml = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("{}: {error}", path.display()));
        let config = TabsdataConfig::parse(&yaml)
            .unwrap_or_else(|error| panic!("{}: {error}", path.display()));

        assert!(
            !config.pods.function.component.resources.is_empty(),
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
        let dataflow = config
            .dataflow("function-publisher", Some("ephemeral"))
            .expect("function dataflow");
        assert!(
            dataflow.temporary_folder.is_some(),
            "{provider} temporary folder"
        );
        assert_eq!(dataflow.spill.enabled, Some(true), "{provider} spill");
        assert!(dataflow.spill.folder.is_some(), "{provider} spill folder");
        assert!(
            dataflow.spill.memory_threshold_percent.is_some(),
            "{provider} memory threshold"
        );
        assert!(
            dataflow.spill.disk_limit_percent.is_some(),
            "{provider} disk limit"
        );
        assert!(
            dataflow.abort.memory_threshold_percent.is_some(),
            "{provider} abort threshold"
        );
    }
}

#[test]
fn the_function_dataflow_sits_beside_its_resources() {
    let config = TabsdataConfig::parse(indoc! {"
        pods:
          function:
            resources:
              cpu: '2'
            dataflow:
              temporary_folder: /tabsdata/dataflow/temporary
              spill:
                enabled: false
                folder: /tabsdata/dataflow/spill
                memory_threshold_percent: 40
                disk_limit_percent: 80
              abort:
                memory_threshold_percent: 95
    "})
    .expect("parses");
    let function = &config.pods.function;
    assert_eq!(function.component.resources.cpu.as_deref(), Some("2"));
    let dataflow = &function.dataflow;
    assert_eq!(
        dataflow.temporary_folder.as_deref(),
        Some("/tabsdata/dataflow/temporary")
    );
    assert_eq!(dataflow.spill.enabled, Some(false));
    assert_eq!(
        dataflow.spill.folder.as_deref(),
        Some("/tabsdata/dataflow/spill")
    );
    assert_eq!(
        dataflow.spill.memory_threshold_percent.map(u8::from),
        Some(40)
    );
    assert_eq!(dataflow.spill.disk_limit_percent.map(u8::from), Some(80));
    assert_eq!(
        dataflow.abort.memory_threshold_percent.map(u8::from),
        Some(95)
    );
}

#[test]
fn a_dataflow_percentage_outside_1_to_100_fails_to_parse() {
    for value in ["0", "101", "-5"] {
        let yaml = format!(
            "pods:\n  function:\n    dataflow:\n      spill:\n        disk_limit_percent: {value}\n"
        );
        assert!(TabsdataConfig::parse(&yaml).is_err(), "{value}");
    }
}

#[test]
fn only_function_pods_have_a_dataflow() {
    let config = TabsdataConfig::parse("").expect("parses");
    assert!(
        config
            .dataflow("function-publisher", Some("ephemeral"))
            .is_some()
    );
    assert!(config.dataflow("apiserver", Some("ephemeral")).is_none());
    assert!(config.dataflow("venvloader", Some("init")).is_none());
    assert!(config.dataflow("", None).is_none());
}
