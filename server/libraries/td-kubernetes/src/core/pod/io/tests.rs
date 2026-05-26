//
// Copyright 2026 Tabs Data Inc.
//

use super::*;

const SAMPLE_POD: &str = "\
apiVersion: v1
kind: Pod
metadata:
  name: td-server
  namespace: tabsdata
spec:
  containers:
  - name: tdserver
    image: tabsdata:1.4.0
    command: [\"tdserver\"]
    args: [\"start\"]
";

#[test]
fn deserialize_pod_manifest_extracts_metadata_name_and_namespace() {
    let pod = deserialize_pod_manifest(SAMPLE_POD).expect("valid pod");
    assert_eq!(pod.metadata.name.as_deref(), Some("td-server"));
    assert_eq!(pod.metadata.namespace.as_deref(), Some("tabsdata"));
}

#[test]
fn deserialize_pod_manifest_extracts_container_image_and_command() {
    let pod = deserialize_pod_manifest(SAMPLE_POD).expect("valid pod");
    let spec = pod.spec.expect("spec present");
    assert_eq!(spec.containers.len(), 1);
    let container = &spec.containers[0];
    assert_eq!(container.name, "tdserver");
    assert_eq!(container.image.as_deref(), Some("tabsdata:1.4.0"));
    assert_eq!(
        container.command.as_deref(),
        Some(&["tdserver".to_owned()][..])
    );
    assert_eq!(container.args.as_deref(), Some(&["start".to_owned()][..]));
}

#[test]
fn deserialize_pod_manifest_returns_err_for_malformed_yaml() {
    let outcome = deserialize_pod_manifest("not: : : valid: yaml");
    assert!(matches!(outcome, Err(PodYamlError::ParseFailed(_))));
}

#[test]
fn deserialize_pod_manifest_returns_err_for_wrong_type() {
    // A plain scalar is not a Pod struct.
    let outcome = deserialize_pod_manifest("just a string");
    assert!(matches!(outcome, Err(PodYamlError::ParseFailed(_))));
}

#[test]
fn serialize_pod_manifest_emits_yaml_with_metadata_name() {
    let pod = deserialize_pod_manifest(SAMPLE_POD).expect("valid pod");
    let yaml = serialize_pod_manifest(&pod).expect("serialize");
    assert!(yaml.contains("td-server"));
    assert!(yaml.contains("tabsdata:1.4.0"));
}

#[test]
fn parse_then_serialize_preserves_essential_fields() {
    let pod = deserialize_pod_manifest(SAMPLE_POD).expect("valid pod");
    let yaml = serialize_pod_manifest(&pod).expect("serialize");
    let reparsed = deserialize_pod_manifest(&yaml).expect("re-parse");
    assert_eq!(pod.metadata.name, reparsed.metadata.name);
    assert_eq!(pod.metadata.namespace, reparsed.metadata.namespace);
    let original = pod.spec.unwrap();
    let restored = reparsed.spec.unwrap();
    assert_eq!(original.containers.len(), restored.containers.len());
    assert_eq!(original.containers[0].name, restored.containers[0].name);
    assert_eq!(original.containers[0].image, restored.containers[0].image);
}
