//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::BTreeMap;

use chrono::Utc;
use k8s_openapi::api::core::v1::{
    ConfigMapProjection, Container, ProjectedVolumeSource, Volume, VolumeProjection,
};

use super::*;
use crate::base::naming::to_kube_resource_name;
use crate::runtime::instance::manifest::Manifest;

fn build_profile(configs: &[(&str, &str)]) -> Profile {
    let configs: BTreeMap<String, String> = configs
        .iter()
        .map(|(file, yaml)| ((*file).to_owned(), (*yaml).to_owned()))
        .collect();
    Profile {
        pods: BTreeMap::new(),
        assets: BTreeMap::new(),
        configs,
        manifest: Manifest::initial_install("0.0.0", Utc::now(), Vec::new()),
    }
}

fn build_pod_spec_with(container_names: &[&str]) -> PodSpec {
    build_pod_spec_mounting(container_names, &[])
}

fn build_pod_spec_mounting(container_names: &[&str], config_files: &[&str]) -> PodSpec {
    let sources = config_files
        .iter()
        .map(|file| {
            let id = crate::base::naming::id_from_file(file);
            let name = to_kube_resource_name(
                KPROFILE_CONFIGS_FAMILY.scope,
                id,
                KPROFILE_CONFIGS_FAMILY.kind,
                file,
            )
            .expect("configmap name");
            VolumeProjection {
                config_map: Some(ConfigMapProjection {
                    name,
                    ..Default::default()
                }),
                ..Default::default()
            }
        })
        .collect();
    PodSpec {
        containers: container_names
            .iter()
            .map(|name| Container {
                name: (*name).to_owned(),
                ..Default::default()
            })
            .collect(),
        volumes: Some(vec![Volume {
            name: "config".to_owned(),
            projected: Some(ProjectedVolumeSource {
                sources: Some(sources),
                ..Default::default()
            }),
            ..Default::default()
        }]),
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
fn mounted_config_injects_secret_keys() {
    let profile = build_profile(&[(
        "apiserver.yaml",
        "database:\n  password: ${{secret:postgres_password}}\njwt:\n  secret: ${{secret:jwt_signing_key}}\n",
    )]);
    let mut spec = build_pod_spec_mounting(&["apiserver"], &["apiserver.yaml"]);
    let mut secrets: BTreeSet<String> = BTreeSet::new();
    secrets.insert("td-app-apiserver-secret-config-apiserver.yaml".to_owned());
    inject_secret_envs(&profile, "apiserver.yaml", &mut spec, &secrets).expect("ok");
    let injected = names(&spec, "apiserver");
    assert!(injected.contains(&"DATABASE__PASSWORD".to_owned()));
    assert!(injected.contains(&"JWT__SECRET".to_owned()));
}

#[test]
fn secret_auto_placeholders_are_injected() {
    let profile = build_profile(&[(
        "dbmanager.database.yaml",
        "database_server:\n  password: ${{secretAuto:postgres_password}}\n",
    )]);
    let mut spec = build_pod_spec_mounting(&["dbcreate"], &["dbmanager.database.yaml"]);
    let mut secrets: BTreeSet<String> = BTreeSet::new();
    secrets.insert("td-app-dbmanager-secret-config-dbmanager.database.yaml".to_owned());
    inject_secret_envs(&profile, "dbmanager.yaml", &mut spec, &secrets).expect("ok");
    let injected = names(&spec, "dbcreate");
    assert!(injected.contains(&"DATABASE_SERVER__PASSWORD".to_owned()));
}

/// The privileged server config is its own file: a pod that does not mount it
/// (the apiserver) never receives its secret env vars.
#[test]
fn server_secrets_stay_out_of_pods_not_mounting_the_file() {
    let profile = build_profile(&[
        (
            "apiserver.database.yaml",
            "database:\n  apiserver:\n    password: ${{secretAuto:postgres_password}}\n",
        ),
        (
            "dbmanager.database.yaml",
            "database_server:\n  password: ${{secretAuto:postgres_password}}\n",
        ),
    ]);
    let mut spec = build_pod_spec_mounting(&["apiserver"], &["apiserver.database.yaml"]);
    let mut secrets: BTreeSet<String> = BTreeSet::new();
    secrets.insert("td-app-apiserver-secret-config-apiserver.database.yaml".to_owned());
    secrets.insert("td-app-dbmanager-secret-config-dbmanager.database.yaml".to_owned());
    inject_secret_envs(&profile, "apiserver.yaml", &mut spec, &secrets).expect("ok");
    let injected = names(&spec, "apiserver");
    assert!(injected.contains(&"DATABASE__APISERVER__PASSWORD".to_owned()));
    assert!(
        !injected.contains(&"DATABASE_SERVER__PASSWORD".to_owned()),
        "the server secret must not reach a pod that does not mount its config"
    );
}

#[test]
fn mounted_config_targets_all_containers() {
    let profile = build_profile(&[(
        "apiserver.yaml",
        "database:\n  password: ${{secret:postgres_password}}\n",
    )]);
    let mut spec = build_pod_spec_mounting(&["apiserver", "sidecar"], &["apiserver.yaml"]);
    let mut secrets: BTreeSet<String> = BTreeSet::new();
    secrets.insert("td-app-apiserver-secret-config-apiserver.yaml".to_owned());
    inject_secret_envs(&profile, "apiserver.yaml", &mut spec, &secrets).expect("ok");
    assert_eq!(names(&spec, "apiserver"), vec!["DATABASE__PASSWORD"]);
    assert_eq!(names(&spec, "sidecar"), vec!["DATABASE__PASSWORD"]);
}

#[test]
fn only_mounted_configs_are_injected() {
    let profile = build_profile(&[
        (
            "apiserver.yaml",
            "database:\n  password: ${{secret:postgres_password}}\n",
        ),
        ("apiserver.log.yaml", "audit:\n  token: ${{secret}}\n"),
        ("tabsdata.yaml", "ignored:\n  secret: ${{secret}}\n"),
    ]);
    let mut spec =
        build_pod_spec_mounting(&["apiserver"], &["apiserver.yaml", "apiserver.log.yaml"]);
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
        "tabsdata.yaml is not mounted, so its leaves should not be injected"
    );
}

#[test]
fn missing_secret_in_cluster_is_a_hard_failure() {
    let profile = build_profile(&[(
        "apiserver.yaml",
        "database:\n  password: ${{secret:postgres_password}}\n",
    )]);
    let mut spec = build_pod_spec_mounting(&["apiserver"], &["apiserver.yaml"]);
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
    let mut spec = build_pod_spec_mounting(&["apiserver"], &["apiserver.yaml"]);
    let secrets: BTreeSet<String> = BTreeSet::new();
    inject_secret_envs(&profile, "apiserver.yaml", &mut spec, &secrets).expect("ok");
    assert!(names(&spec, "apiserver").is_empty());
}

#[test]
fn already_present_env_var_is_not_duplicated() {
    let profile = build_profile(&[(
        "apiserver.yaml",
        "database:\n  password: ${{secret:postgres_password}}\n",
    )]);
    let mut spec = build_pod_spec_mounting(&["apiserver"], &["apiserver.yaml"]);
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
fn global_envs_reach_every_container() {
    let profile = build_profile(&[(
        "tabsdata.yaml",
        "pods:\n  global:\n    envs:\n      TD_TRACE: verbose\n      TD_RETRIES: 3\n",
    )]);
    let mut spec = build_pod_spec_mounting(&["apiserver", "sidecar"], &[]);
    inject_secret_envs(&profile, "apiserver.yaml", &mut spec, &BTreeSet::new()).expect("ok");
    for container in ["apiserver", "sidecar"] {
        let injected = names(&spec, container);
        assert!(injected.contains(&"TD_TRACE".to_owned()), "{container}");
        assert!(injected.contains(&"TD_RETRIES".to_owned()), "{container}");
    }
}

#[test]
fn global_envs_do_not_need_the_config_mounted() {
    let profile = build_profile(&[(
        "tabsdata.yaml",
        "pods:\n  global:\n    envs:\n      TD_TRACE: verbose\n",
    )]);
    let mut spec = build_pod_spec_mounting(&["apiserver"], &[]);
    inject_secret_envs(&profile, "apiserver.yaml", &mut spec, &BTreeSet::new()).expect("ok");
    assert!(names(&spec, "apiserver").contains(&"TD_TRACE".to_owned()));
}

#[test]
fn an_empty_global_envs_map_injects_nothing() {
    let profile = build_profile(&[("tabsdata.yaml", "pods:\n  global:\n    envs: {}\n")]);
    let mut spec = build_pod_spec_mounting(&["apiserver"], &[]);
    inject_secret_envs(&profile, "apiserver.yaml", &mut spec, &BTreeSet::new()).expect("ok");
    assert!(names(&spec, "apiserver").is_empty());
}

#[test]
fn a_container_env_wins_over_a_global_one() {
    let profile = build_profile(&[(
        "tabsdata.yaml",
        "pods:\n  global:\n    envs:\n      TD_TRACE: global\n",
    )]);
    let mut spec = build_pod_spec_mounting(&["apiserver"], &[]);
    spec.containers[0].env = Some(vec![EnvVar {
        name: "TD_TRACE".to_owned(),
        value: Some("container".to_owned()),
        value_from: None,
    }]);
    inject_secret_envs(&profile, "apiserver.yaml", &mut spec, &BTreeSet::new()).expect("ok");
    let value = spec.containers[0]
        .env
        .as_ref()
        .expect("env")
        .iter()
        .find(|env| env.name == "TD_TRACE")
        .and_then(|env| env.value.clone());
    assert_eq!(value, Some("container".to_owned()));
}

#[test]
fn a_non_scalar_global_env_is_a_hard_failure() {
    let profile = build_profile(&[(
        "tabsdata.yaml",
        "pods:\n  global:\n    envs:\n      TD_TRACE:\n        nested: value\n",
    )]);
    let mut spec = build_pod_spec_mounting(&["apiserver"], &[]);
    let outcome = inject_secret_envs(&profile, "apiserver.yaml", &mut spec, &BTreeSet::new());
    assert!(outcome.is_err());
}
