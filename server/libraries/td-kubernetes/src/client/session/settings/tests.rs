//
// Copyright 2026 Tabsdata Inc.
//

use std::path::PathBuf;

use super::*;

fn sample_docker() -> Session {
    Session::Docker {
        kubeconfig: PathBuf::from("/home/user/.kube/config"),
        context: "dev".to_owned(),
        cluster: "dev-cluster".to_owned(),
        namespace: "dev-ns".to_owned(),
        inputs: DockerInputs {
            kubeconfig: Some(PathBuf::from("/home/user/.kube/config")),
            context: Some("dev".to_owned()),
            current: false,
            namespace: Some("dev-ns".to_owned()),
        },
    }
}

fn sample_kminus() -> Session {
    Session::Kminus {
        path: PathBuf::from("/home/user/.tabsdata/instances/tabsdata"),
        kubeconfig: PathBuf::from("/home/user/.tabsdata/instances/tabsdata/cluster/kube/config"),
        context: "kminus".to_owned(),
        cluster: "kminus-cluster".to_owned(),
        namespace: "default".to_owned(),
        inputs: KminusInputs {
            name: Some("tabsdata".to_owned()),
        },
    }
}

#[test]
fn tdksettings_path_ends_with_server_session_file() {
    let path = tdksettings_path();
    assert_eq!(
        path.file_name().and_then(|name| name.to_str()),
        Some(SERVER_SESSION_FILE)
    );
}

#[test]
fn tdksettings_path_lives_under_tabsdata_home_dir() {
    let path = tdksettings_path();
    let parent = path.parent().expect("path has parent");
    assert_eq!(
        parent.file_name().and_then(|name| name.to_str()),
        Some(TABSDATA_HOME_DIR)
    );
}

#[test]
fn docker_serializes_with_lowercase_provider_tag() {
    let context = sample_docker();
    let json = serde_json::to_string(&context).expect("serialize kubernetes");
    assert!(
        json.contains("\"provider\":\"docker\""),
        "missing lowercase provider tag in: {json}"
    );
}

#[test]
fn kminus_serializes_with_lowercase_provider_tag() {
    let context = sample_kminus();
    let json = serde_json::to_string(&context).expect("serialize kminus");
    assert!(
        json.contains("\"provider\":\"kminus\""),
        "missing lowercase provider tag in: {json}"
    );
}

#[test]
fn docker_round_trips_through_json() {
    let original = sample_docker();
    let json = serde_json::to_string(&original).expect("serialize");
    let parsed: Session = serde_json::from_str(&json).expect("deserialize");
    let Session::Docker {
        kubeconfig,
        context,
        cluster,
        namespace,
        inputs,
    } = parsed
    else {
        panic!("expected Docker variant after round trip");
    };
    assert_eq!(kubeconfig, PathBuf::from("/home/user/.kube/config"));
    assert_eq!(context, "dev");
    assert_eq!(cluster, "dev-cluster");
    assert_eq!(namespace, "dev-ns");
    assert_eq!(inputs.context.as_deref(), Some("dev"));
    assert_eq!(inputs.namespace.as_deref(), Some("dev-ns"));
    assert!(!inputs.current);
}

#[test]
fn kminus_round_trips_through_json() {
    let original = sample_kminus();
    let json = serde_json::to_string(&original).expect("serialize");
    let parsed: Session = serde_json::from_str(&json).expect("deserialize");
    let Session::Kminus {
        path,
        kubeconfig,
        context,
        cluster,
        namespace,
        inputs,
    } = parsed
    else {
        panic!("expected Kminus variant after round trip");
    };
    assert_eq!(
        path,
        PathBuf::from("/home/user/.tabsdata/instances/tabsdata")
    );
    assert_eq!(
        kubeconfig,
        PathBuf::from("/home/user/.tabsdata/instances/tabsdata/cluster/kube/config")
    );
    assert_eq!(context, "kminus");
    assert_eq!(cluster, "kminus-cluster");
    assert_eq!(namespace, "default");
    assert_eq!(inputs.name.as_deref(), Some("tabsdata"));
}

#[test]
fn docker_inputs_with_default_values_skips_optional_fields_in_json() {
    let context = Session::Docker {
        kubeconfig: PathBuf::from("/k"),
        context: "c".to_owned(),
        cluster: "cl".to_owned(),
        namespace: "n".to_owned(),
        inputs: DockerInputs::default(),
    };
    let json = serde_json::to_string(&context).expect("serialize");
    // DockerInputs::default has every field at None / false; these are
    // marked skip_serializing_if so the serialized inputs object is empty.
    assert!(!json.contains("\"current\""), "current not skipped: {json}");
    assert!(
        !json.contains("\"context\":null"),
        "context not skipped: {json}"
    );
}

#[test]
fn docker_inputs_default_has_all_optional_fields_unset() {
    let inputs = DockerInputs::default();
    assert!(inputs.kubeconfig.is_none());
    assert!(inputs.context.is_none());
    assert!(!inputs.current);
    assert!(inputs.namespace.is_none());
}

#[test]
fn kminus_inputs_default_has_all_optional_fields_unset() {
    let inputs = KminusInputs::default();
    assert!(inputs.name.is_none());
}

#[test]
fn provider_returns_docker_for_docker_variant() {
    assert!(matches!(
        sample_docker().provider(),
        ClusterProvider::Docker
    ));
}

#[test]
fn provider_returns_kminus_for_kminus_variant() {
    assert!(matches!(
        sample_kminus().provider(),
        ClusterProvider::Kminus
    ));
}

#[test]
fn display_name_for_docker_is_context_slash_namespace() {
    assert_eq!(sample_docker().display_name(), "docker:dev/dev-ns");
}

#[test]
fn display_name_for_kminus_uses_last_path_component() {
    assert_eq!(sample_kminus().display_name(), "kminus:tabsdata");
}

#[test]
fn display_name_for_kminus_falls_back_when_path_has_no_filename() {
    let context = Session::Kminus {
        path: PathBuf::from("/"),
        kubeconfig: PathBuf::from("/kube"),
        context: "c".to_owned(),
        cluster: "cl".to_owned(),
        namespace: "n".to_owned(),
        inputs: KminusInputs::default(),
    };
    assert_eq!(context.display_name(), "kminus:?");
}

#[test]
fn to_args_for_docker_preserves_cluster_arguments() {
    let (cluster, instance) = sample_docker().to_args();
    assert!(matches!(cluster.provider, Some(ClusterProvider::Docker)));
    assert_eq!(cluster.context.as_deref(), Some("dev"));
    assert_eq!(
        cluster.kubeconfig,
        Some(PathBuf::from("/home/user/.kube/config"))
    );
    assert!(!cluster.current);
    assert_eq!(cluster.namespace.as_deref(), Some("dev-ns"));
    assert!(instance.name.is_none());
}

#[test]
fn to_args_for_kminus_passes_name_through_instance_arguments() {
    let (cluster, instance) = sample_kminus().to_args();
    assert!(matches!(cluster.provider, Some(ClusterProvider::Kminus)));
    assert!(cluster.context.is_none());
    assert!(cluster.kubeconfig.is_none());
    assert!(!cluster.current);
    assert!(cluster.namespace.is_none());
    assert_eq!(instance.name.as_deref(), Some("tabsdata"));
}
