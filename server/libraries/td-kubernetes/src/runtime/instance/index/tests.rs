//
// Copyright 2026 Tabsdata Inc.
//

use super::*;

const CONFIG: &str = "\
pods:
  global:
    envs: {}
";

fn config() -> TabsdataConfig {
    TabsdataConfig::parse(CONFIG).expect("parses")
}

fn rendered(config: &TabsdataConfig, name: &str) -> Option<String> {
    config.pods.global.envs.get(name).and_then(EnvValue::render)
}

#[test]
fn the_policy_governs_only_kminus_without_an_explicit_mode() {
    assert!(governed(ClusterProvider::Kminus, UvMode::Unset));
    assert!(!governed(ClusterProvider::Kminus, UvMode::Devpi));
    assert!(!governed(ClusterProvider::Kminus, UvMode::Linked));
    assert!(!governed(ClusterProvider::Eks, UvMode::Unset));
}

#[test]
fn the_argument_wins_over_every_environment_variable() {
    let plan = resolve(Some("https://private/simple/"), None);
    assert_eq!(plan.local.as_deref(), Some("https://private/simple/"));
    assert_eq!(plan.origin, IndexOrigin::Argument);
}

#[test]
fn a_blank_argument_is_not_a_choice() {
    let plan = resolve(Some("   "), None);
    assert_ne!(plan.origin, IndexOrigin::Argument);
}

#[test]
fn a_plan_matching_the_remote_value_is_the_same() {
    let plan = resolve(
        Some("https://private/simple/"),
        Some("https://private/simple/".to_owned()),
    );
    assert!(plan.same());
    assert!(!plan.none());
}

#[test]
fn a_remote_value_with_no_local_one_is_cleared() {
    let plan = IndexPlan {
        local: None,
        origin: IndexOrigin::Absent,
        remote: Some("https://private/simple/".to_owned()),
    };
    assert!(plan.none());
    assert!(!plan.same());
    assert_eq!(plan.obtain_local(), "<none>");
    assert_eq!(plan.obtain_remote(), "https://private/simple/");
}

#[test]
fn applying_an_index_sets_the_three_entries() {
    let mut config = config();
    apply(&mut config, Some("https://private/simple/"));
    assert_eq!(
        rendered(&config, ENV_UV_INDEX).as_deref(),
        Some("https://private/simple/")
    );
    assert_eq!(
        rendered(&config, ENV_UV_DEFAULT_INDEX).as_deref(),
        Some(PYPI_INDEX_URL)
    );
    assert_eq!(
        rendered(&config, ENV_UV_INDEX_STRATEGY).as_deref(),
        Some(UV_INDEX_STRATEGY_BEST_MATCH)
    );
}

#[test]
fn clearing_the_index_keeps_the_other_two_entries() {
    let mut config = config();
    apply(&mut config, Some("https://private/simple/"));
    apply(&mut config, None);
    assert_eq!(rendered(&config, ENV_UV_INDEX), None);
    assert_eq!(
        rendered(&config, ENV_UV_DEFAULT_INDEX).as_deref(),
        Some(PYPI_INDEX_URL)
    );
    assert_eq!(
        rendered(&config, ENV_UV_INDEX_STRATEGY).as_deref(),
        Some(UV_INDEX_STRATEGY_BEST_MATCH)
    );
}

#[test]
fn a_cleared_index_reads_back_as_absent() {
    let mut config = config();
    apply(&mut config, None);
    assert_eq!(remote_index(&config), None);
}

#[test]
fn a_set_index_reads_back_from_the_config() {
    let mut config = config();
    apply(&mut config, Some("https://private/simple/"));
    assert_eq!(
        remote_index(&config).as_deref(),
        Some("https://private/simple/")
    );
}
