//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::BTreeMap;

use chrono::Utc;
use indoc::indoc;
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
        indoc! {"
            database:
              password: ${{secret:postgres_system_password}}
            jwt:
              secret: ${{secret:jwt_signing_key}}
        "},
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
        indoc! {"
            database_server:
              password: ${{secretAuto:postgres_system_password}}
        "},
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
            indoc! {"
                database:
                  apiserver:
                    password: ${{secretAuto:postgres_system_password}}
            "},
        ),
        (
            "dbmanager.database.yaml",
            indoc! {"
                database_server:
                  password: ${{secretAuto:postgres_system_password}}
            "},
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
        indoc! {"
            database:
              password: ${{secret:postgres_system_password}}
        "},
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
            indoc! {"
                database:
                  password: ${{secret:postgres_system_password}}
            "},
        ),
        (
            "apiserver.log.yaml",
            indoc! {"
            audit:
              token: ${{secret}}
        "},
        ),
        (
            "tabsdata.yaml",
            indoc! {"
            ignored:
              secret: ${{secret}}
        "},
        ),
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
        indoc! {"
            database:
              password: ${{secret:postgres_system_password}}
        "},
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
    let profile = build_profile(&[(
        "apiserver.yaml",
        indoc! {"
        noseed:
          value: literal
    "},
    )]);
    let mut spec = build_pod_spec_mounting(&["apiserver"], &["apiserver.yaml"]);
    let secrets: BTreeSet<String> = BTreeSet::new();
    inject_secret_envs(&profile, "apiserver.yaml", &mut spec, &secrets).expect("ok");
    assert!(names(&spec, "apiserver").is_empty());
}

#[test]
fn already_present_env_var_is_not_duplicated() {
    let profile = build_profile(&[(
        "apiserver.yaml",
        indoc! {"
            database:
              password: ${{secret:postgres_system_password}}
        "},
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
        indoc! {"
            pods:
              global:
                envs:
                  TD_TRACE: verbose
                  TD_RETRIES: 3
        "},
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
        indoc! {"
            pods:
              global:
                envs:
                  TD_TRACE: verbose
        "},
    )]);
    let mut spec = build_pod_spec_mounting(&["apiserver"], &[]);
    inject_secret_envs(&profile, "apiserver.yaml", &mut spec, &BTreeSet::new()).expect("ok");
    assert!(names(&spec, "apiserver").contains(&"TD_TRACE".to_owned()));
}

#[test]
fn global_envs_apply_to_a_function_pod_spec() {
    let mut spec = build_pod_spec_mounting(&["function", "upload"], &[]);
    apply_global_envs(
        indoc! {"
            pods:
              global:
                envs:
                  TYPESAFE_API_KEY: key
        "},
        &mut spec,
    )
    .expect("ok");
    for container in ["function", "upload"] {
        assert!(
            names(&spec, container).contains(&"TYPESAFE_API_KEY".to_owned()),
            "{container}"
        );
    }
}

#[test]
fn an_empty_global_envs_map_injects_nothing() {
    let profile = build_profile(&[(
        "tabsdata.yaml",
        indoc! {"
        pods:
          global:
            envs: {}
    "},
    )]);
    let mut spec = build_pod_spec_mounting(&["apiserver"], &[]);
    inject_secret_envs(&profile, "apiserver.yaml", &mut spec, &BTreeSet::new()).expect("ok");
    assert!(names(&spec, "apiserver").is_empty());
}

#[test]
fn an_unspecified_global_env_leaves_the_container_one_alone() {
    let profile = build_profile(&[(
        "tabsdata.yaml",
        indoc! {r#"
            pods:
              global:
                envs:
                  UV_INDEX:
                  UV_INDEX_STRATEGY: ""
        "#},
    )]);
    let mut spec = build_pod_spec_mounting(&["apiserver"], &[]);
    spec.containers[0].env = Some(vec![
        EnvVar {
            name: "UV_INDEX".to_owned(),
            value: Some("from-pod".to_owned()),
            value_from: None,
        },
        EnvVar {
            name: "UV_INDEX_STRATEGY".to_owned(),
            value: Some("from-pod".to_owned()),
            value_from: None,
        },
    ]);
    inject_secret_envs(&profile, "apiserver.yaml", &mut spec, &BTreeSet::new()).expect("ok");
    let entries = spec.containers[0].env.as_ref().expect("env");
    let value = |name: &str| {
        entries
            .iter()
            .find(|env| env.name == name)
            .and_then(|env| env.value.clone())
    };
    assert_eq!(value("UV_INDEX").as_deref(), Some("from-pod"));
    assert_eq!(value("UV_INDEX_STRATEGY").as_deref(), Some(""));
}

#[test]
fn the_host_receives_only_the_global_envs_with_a_value() {
    let config = indoc! {r#"
        pods:
          global:
            envs:
              UV_INDEX:
              UV_INDEX_STRATEGY: ""
              UV_DEFAULT_INDEX: https://pypi.org/simple/
              UV_HTTP_RETRIES: 16
    "#};
    assert_eq!(
        host_global_envs(config).expect("ok"),
        vec![
            (
                "UV_DEFAULT_INDEX".to_owned(),
                "https://pypi.org/simple/".to_owned()
            ),
            ("UV_HTTP_RETRIES".to_owned(), "16".to_owned()),
        ]
    );
}

#[test]
fn a_global_env_wins_over_a_container_one() {
    let profile = build_profile(&[(
        "tabsdata.yaml",
        indoc! {"
            pods:
              global:
                envs:
                  TD_TRACE: global
        "},
    )]);
    let mut spec = build_pod_spec_mounting(&["apiserver"], &[]);
    spec.containers[0].env = Some(vec![EnvVar {
        name: "TD_TRACE".to_owned(),
        value: Some("container".to_owned()),
        value_from: None,
    }]);
    inject_secret_envs(&profile, "apiserver.yaml", &mut spec, &BTreeSet::new()).expect("ok");
    let entries = spec.containers[0].env.as_ref().expect("env");
    let matches: Vec<_> = entries
        .iter()
        .filter(|env| env.name == "TD_TRACE")
        .collect();
    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0].value, Some("global".to_owned()));
}

#[test]
fn a_global_env_overrides_an_init_container_one() {
    let profile = build_profile(&[(
        "tabsdata.yaml",
        indoc! {"
            pods:
              global:
                envs:
                  UV_INDEX: from-config
        "},
    )]);
    let mut spec = build_pod_spec_mounting(&["apiserver"], &[]);
    spec.init_containers = Some(vec![Container {
        name: "bootstrap".to_owned(),
        env: Some(vec![EnvVar {
            name: "UV_INDEX".to_owned(),
            value: Some("from-pod".to_owned()),
            value_from: None,
        }]),
        ..Default::default()
    }]);
    inject_secret_envs(&profile, "apiserver.yaml", &mut spec, &BTreeSet::new()).expect("ok");
    let value = spec.init_containers.as_ref().expect("init containers")[0]
        .env
        .as_ref()
        .expect("env")
        .iter()
        .find(|env| env.name == "UV_INDEX")
        .and_then(|env| env.value.clone());
    assert_eq!(value, Some("from-config".to_owned()));
}

#[test]
fn a_non_scalar_global_env_is_a_hard_failure() {
    let profile = build_profile(&[(
        "tabsdata.yaml",
        indoc! {"
            pods:
              global:
                envs:
                  TD_TRACE:
                    nested: value
        "},
    )]);
    let mut spec = build_pod_spec_mounting(&["apiserver"], &[]);
    let outcome = inject_secret_envs(&profile, "apiserver.yaml", &mut spec, &BTreeSet::new());
    assert!(outcome.is_err());
}

#[test]
fn options_a_user_added_are_injected_one_env_each() {
    let profile = build_profile(&[(
        "apiserver.storage.yaml",
        indoc! {"
            storage:
              mounts:
              - id: ROOT
                path: /
                uri: file:///data/
                options:
                  aws_access_key_id: ${{secret}}
                  aws_endpoint_url: ${{secret}}
        "},
    )]);
    let mut spec = build_pod_spec_mounting(&["apiserver"], &["apiserver.storage.yaml"]);
    let mut secrets: BTreeSet<String> = BTreeSet::new();
    secrets.insert("td-app-apiserver-secret-config-apiserver.storage.yaml".to_owned());
    inject_secret_envs(&profile, "apiserver.yaml", &mut spec, &secrets).expect("ok");
    let mut injected = names(&spec, "apiserver");
    injected.sort();
    assert_eq!(
        injected,
        [
            "STORAGE__MOUNTS__ROOT__OPTIONS__AWS_ACCESS_KEY_ID",
            "STORAGE__MOUNTS__ROOT__OPTIONS__AWS_ENDPOINT_URL",
        ]
    );
}

const DATAFLOW_CONFIG: &str = indoc! {"
    pods:
      function:
        dataflow:
          temporary_folder: /tabsdata/dataflow/temporary
          spill:
            enabled: true
            folder: /tabsdata/dataflow/spill
            memory_threshold_percent: 40
            disk_limit_percent: 80
          abort:
            memory_threshold_percent: 95
"};

fn function_placement() -> PodPlacement {
    PodPlacement {
        component: Some("function-transformer".to_owned()),
        tier: Some("ephemeral".to_owned()),
    }
}

fn value(spec: &PodSpec, container: &str, name: &str) -> Option<String> {
    spec.containers
        .iter()
        .chain(spec.init_containers.iter().flatten())
        .find(|candidate| candidate.name == container)
        .and_then(|candidate| candidate.env.as_ref())
        .and_then(|env| env.iter().find(|entry| entry.name == name))
        .and_then(|entry| entry.value.clone())
}

#[test]
fn dataflow_envs_reach_every_container_of_a_function_pod() {
    let mut spec = build_pod_spec_with(&["function", "report-completion"]);
    apply_dataflow_envs(DATAFLOW_CONFIG, &function_placement(), &mut spec).expect("ok");
    for container in ["function", "report-completion"] {
        for (name, expected) in [
            (
                ENV_TD_DATAFLOW__TEMPORARY_FOLDER,
                "/tabsdata/dataflow/temporary",
            ),
            (ENV_TD_DATAFLOW__SPILL__ENABLED, "true"),
            (ENV_TD_DATAFLOW__SPILL__FOLDER, "/tabsdata/dataflow/spill"),
            (ENV_TD_DATAFLOW__SPILL__MEMORY_THRESHOLD_PERCENT, "40"),
            (ENV_TD_DATAFLOW__SPILL__DISK_LIMIT_PERCENT, "80"),
            (ENV_TD_DATAFLOW__ABORT__MEMORY_THRESHOLD_PERCENT, "95"),
        ] {
            assert_eq!(
                value(&spec, container, name).as_deref(),
                Some(expected),
                "{container} {name}"
            );
        }
    }
}

#[test]
fn dataflow_envs_stay_out_of_service_pods() {
    let mut spec = build_pod_spec_with(&["apiserver"]);
    let placement = PodPlacement {
        component: Some("apiserver".to_owned()),
        tier: Some("regular".to_owned()),
    };
    apply_dataflow_envs(DATAFLOW_CONFIG, &placement, &mut spec).expect("ok");
    assert!(names(&spec, "apiserver").is_empty());
}

#[test]
fn unset_dataflow_keys_inject_nothing() {
    let mut spec = build_pod_spec_with(&["function"]);
    apply_dataflow_envs(
        indoc! {"
            pods:
              function:
                dataflow:
                  spill:
                    enabled: false
        "},
        &function_placement(),
        &mut spec,
    )
    .expect("ok");
    assert_eq!(names(&spec, "function"), [ENV_TD_DATAFLOW__SPILL__ENABLED]);
}

#[test]
fn a_global_env_overrides_a_dataflow_env() {
    let config = indoc! {"
        pods:
          global:
            envs:
              TD_DATAFLOW__ABORT__MEMORY_THRESHOLD_PERCENT: 90
          function:
            dataflow:
              abort:
                memory_threshold_percent: 95
    "};
    let mut spec = build_pod_spec_with(&["function"]);
    apply_dataflow_envs(config, &function_placement(), &mut spec).expect("ok");
    apply_global_envs(config, &mut spec).expect("ok");
    assert_eq!(
        value(
            &spec,
            "function",
            ENV_TD_DATAFLOW__ABORT__MEMORY_THRESHOLD_PERCENT
        )
        .as_deref(),
        Some("90")
    );
}
