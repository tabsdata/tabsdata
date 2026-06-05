//
// Copyright 2026 Tabs Data Inc.
//

use std::collections::BTreeMap;

use chrono::Utc;
use k8s_openapi::api::core::v1::Container;

use super::*;
use crate::runtime::instance::manifest::Manifest;

fn build_profile(configs: &[(&str, &str)]) -> Profile {
    let configs: BTreeMap<String, String> = configs
        .iter()
        .map(|(file, yaml)| ((*file).to_owned(), (*yaml).to_owned()))
        .collect();
    Profile {
        pods: BTreeMap::new(),
        configs,
        manifest: Manifest::initial_install("0.0.0", Utc::now(), Vec::new()),
    }
}

fn build_pod_spec_with(container_names: &[&str]) -> PodSpec {
    PodSpec {
        containers: container_names
            .iter()
            .map(|name| Container {
                name: (*name).to_owned(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

fn names(spec: &PodSpec, container: &str) -> Vec<String> {
    spec.containers
        .iter()
        .find(|c| c.name == container)
        .and_then(|c| c.env.as_ref())
        .map(|env| env.iter().map(|e| e.name.clone()).collect())
        .unwrap_or_default()
}

#[test]
fn worker_with_no_matching_configs_yields_no_env() {
    let profile = build_profile(&[]);
    let mut spec = build_pod_spec_with(&["apiserver"]);
    let secrets: BTreeSet<String> = BTreeSet::new();
    inject_secret_envs(&profile, "apiserver.yaml", &mut spec, &secrets).expect("ok");
    assert!(names(&spec, "apiserver").is_empty());
}

#[test]
fn worker_with_matching_config_injects_secret_keys() {
    let profile = build_profile(&[(
        "apiserver.yaml",
        "database:\n  password: ${secret:postgres_password}\njwt:\n  secret: ${secret:jwt_signing_key}\n",
    )]);
    let mut spec = build_pod_spec_with(&["apiserver"]);
    let mut secrets: BTreeSet<String> = BTreeSet::new();
    secrets.insert("td-app-apiserver-secret-config-apiserver.yaml".to_owned());
    inject_secret_envs(&profile, "apiserver.yaml", &mut spec, &secrets).expect("ok");
    let injected = names(&spec, "apiserver");
    assert!(injected.contains(&"DATABASE__PASSWORD".to_owned()));
    assert!(injected.contains(&"JWT__SECRET".to_owned()));
}

#[test]
fn worker_with_matching_config_targets_all_containers() {
    let profile = build_profile(&[(
        "apiserver.yaml",
        "database:\n  password: ${secret:postgres_password}\n",
    )]);
    let mut spec = build_pod_spec_with(&["apiserver", "sidecar"]);
    let mut secrets: BTreeSet<String> = BTreeSet::new();
    secrets.insert("td-app-apiserver-secret-config-apiserver.yaml".to_owned());
    inject_secret_envs(&profile, "apiserver.yaml", &mut spec, &secrets).expect("ok");
    assert_eq!(names(&spec, "apiserver"), vec!["DATABASE__PASSWORD"]);
    assert_eq!(names(&spec, "sidecar"), vec!["DATABASE__PASSWORD"]);
}

#[test]
fn worker_pulls_dotted_sibling_configs() {
    let profile = build_profile(&[
        (
            "apiserver.yaml",
            "database:\n  password: ${secret:postgres_password}\n",
        ),
        ("apiserver.log.yaml", "audit:\n  token: ${secret}\n"),
        ("tabsdata.yaml", "ignored:\n  secret: ${secret}\n"),
    ]);
    let mut spec = build_pod_spec_with(&["apiserver"]);
    let mut secrets: BTreeSet<String> = BTreeSet::new();
    secrets.insert("td-app-apiserver-secret-config-apiserver.yaml".to_owned());
    secrets.insert("td-app-apiserver-secret-config-apiserver.log.yaml".to_owned());
    secrets.insert("td-app-tabsdata-secret-config-tabsdata.yaml".to_owned());
    inject_secret_envs(&profile, "apiserver.yaml", &mut spec, &secrets).expect("ok");
    let injected = names(&spec, "apiserver");
    assert!(injected.contains(&"DATABASE__PASSWORD".to_owned()));
    assert!(injected.contains(&"AUDIT__TOKEN".to_owned()));
    assert!(
        !injected.contains(&"IGNORED__SECRET".to_owned()),
        "tabsdata.yaml leaves should not leak into apiserver pod"
    );
}

#[test]
fn missing_secret_in_cluster_is_a_hard_failure() {
    let profile = build_profile(&[(
        "apiserver.yaml",
        "database:\n  password: ${secret:postgres_password}\n",
    )]);
    let mut spec = build_pod_spec_with(&["apiserver"]);
    let secrets: BTreeSet<String> = BTreeSet::new();
    let error =
        inject_secret_envs(&profile, "apiserver.yaml", &mut spec, &secrets).expect_err("missing");
    assert!(matches!(
        error,
        LauncherError::SecretEnvInjectionMissingSecrets { .. }
    ));
}

#[test]
fn config_without_placeholders_does_not_require_secret() {
    let profile = build_profile(&[("apiserver.yaml", "noseed:\n  value: literal\n")]);
    let mut spec = build_pod_spec_with(&["apiserver"]);
    let secrets: BTreeSet<String> = BTreeSet::new();
    inject_secret_envs(&profile, "apiserver.yaml", &mut spec, &secrets).expect("ok");
    assert!(names(&spec, "apiserver").is_empty());
}

#[test]
fn already_present_env_var_is_not_duplicated() {
    let profile = build_profile(&[(
        "apiserver.yaml",
        "database:\n  password: ${secret:postgres_password}\n",
    )]);
    let mut spec = build_pod_spec_with(&["apiserver"]);
    if let Some(container) = spec.containers.first_mut() {
        container.env = Some(vec![EnvVar {
            name: "DATABASE__PASSWORD".to_owned(),
            value: Some("preset".to_owned()),
            value_from: None,
        }]);
    }
    let mut secrets: BTreeSet<String> = BTreeSet::new();
    secrets.insert("td-app-apiserver-secret-config-apiserver.yaml".to_owned());
    inject_secret_envs(&profile, "apiserver.yaml", &mut spec, &secrets).expect("ok");
    let injected = names(&spec, "apiserver");
    assert_eq!(injected, vec!["DATABASE__PASSWORD"]);
    let env = spec
        .containers
        .first()
        .and_then(|c| c.env.as_ref())
        .expect("env");
    assert_eq!(env[0].value.as_deref(), Some("preset"));
}

#[test]
fn worker_stem_strips_yaml_suffix() {
    assert_eq!(worker_stem("apiserver.yaml"), "apiserver");
    assert_eq!(worker_stem("apiserver"), "apiserver");
}

#[test]
fn config_matches_exact_worker_name() {
    assert!(config_matches_worker("apiserver.yaml", "apiserver"));
}

#[test]
fn config_matches_dotted_extension() {
    assert!(config_matches_worker("apiserver.log.yaml", "apiserver"));
}

#[test]
fn config_does_not_match_unrelated_prefix() {
    assert!(!config_matches_worker("apiserver-extra.yaml", "apiserver"));
    assert!(!config_matches_worker("tabsdata.yaml", "apiserver"));
}
