//
// Copyright 2026 Tabsdata Inc.
//

use super::*;

const INDEX: &str = "http://localhost:8000/simple";

const TEMPLATE: &str = "\
pods:
  global:
    envs: {}

  function:
    resources:
      cpu: \"2\"
";

fn rewrite(yaml: &str, uv_index: &str) -> String {
    let mut config = TabsdataConfig::parse(yaml).expect("valid config");
    config
        .pods
        .global
        .envs
        .insert(ENV_UV_INDEX.to_owned(), EnvValue::Text(uv_index.to_owned()));
    config.pods.global.envs.insert(
        ENV_UV_INDEX_STRATEGY.to_owned(),
        EnvValue::Text(UV_INDEX_STRATEGY_BEST_MATCH.to_owned()),
    );
    serde_yaml::to_string(&config).expect("serialized")
}

fn env_value(yaml: &str, name: &str) -> String {
    TabsdataConfig::parse(yaml)
        .expect("valid config")
        .pods
        .global
        .envs
        .get(name)
        .expect("env")
        .render()
        .expect("specified env")
}

#[test]
fn the_empty_envs_mapping_takes_both_entries() {
    let updated = rewrite(TEMPLATE, INDEX);
    assert_eq!(env_value(&updated, ENV_UV_INDEX), INDEX);
    assert_eq!(
        env_value(&updated, ENV_UV_INDEX_STRATEGY),
        UV_INDEX_STRATEGY_BEST_MATCH
    );
}

#[test]
fn the_rest_of_the_file_survives() {
    let updated = rewrite(TEMPLATE, INDEX);
    let config = TabsdataConfig::parse(&updated).expect("valid config");
    assert_eq!(config.pods.function.resources.cpu.as_deref(), Some("2"));
}

#[test]
fn an_envs_mapping_that_already_has_entries_keeps_them() {
    let template = "pods:\n  global:\n    envs:\n      TD_TRACE: verbose\n";
    let updated = rewrite(template, INDEX);
    assert_eq!(env_value(&updated, ENV_UV_INDEX), INDEX);
    assert_eq!(env_value(&updated, "TD_TRACE"), "verbose");
}

#[test]
fn an_existing_uv_index_is_overwritten() {
    let template = format!(
        "pods:\n  global:\n    envs:\n      {ENV_UV_INDEX}: http://stale/simple\n      \
         {ENV_UV_INDEX_STRATEGY}: first-index\n"
    );
    let updated = rewrite(&template, INDEX);
    assert_eq!(env_value(&updated, ENV_UV_INDEX), INDEX);
    assert_eq!(
        env_value(&updated, ENV_UV_INDEX_STRATEGY),
        UV_INDEX_STRATEGY_BEST_MATCH
    );
}

#[test]
fn a_value_needing_quotes_round_trips() {
    let value = "http://localhost:8000/simple # main";
    let updated = rewrite(TEMPLATE, value);
    assert_eq!(env_value(&updated, ENV_UV_INDEX), value);
}

#[test]
fn a_file_without_the_envs_mapping_gains_it() {
    let updated = rewrite("pods:\n  global: {}\n", INDEX);
    assert_eq!(env_value(&updated, ENV_UV_INDEX), INDEX);
    assert_eq!(
        env_value(&updated, ENV_UV_INDEX_STRATEGY),
        UV_INDEX_STRATEGY_BEST_MATCH
    );
}

#[test]
fn the_rewrite_is_stable_across_repeated_runs() {
    let once = rewrite(TEMPLATE, INDEX);
    let twice = rewrite(&once, INDEX);
    assert_eq!(once, twice);
}
