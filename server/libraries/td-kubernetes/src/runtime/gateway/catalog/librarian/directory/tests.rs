//
// Copyright 2026 Tabsdata Inc.
//

use super::*;

fn mapping(yaml: &str) -> serde_yaml::Mapping {
    serde_yaml::from_str(yaml).unwrap()
}

const TDINFO_OUTPUT: &str = r#"
version: 2.0.2
sourcetrack:
  tabsdata-os:
    anchor: 2.0.2
    branch: ''
    long_commit_id: d3fe885481963b0ee870be7e120b6eb55529fd12
    short_commit_id: d3fe88548
    tag: 2.0.2
  workflow:
    run: '33055555562'
"#;

#[test]
fn the_version_is_rendered_as_the_server_version_document() {
    let bytes = KubernetesServerCatalog::server_version(
        &EtcContent::ServerVersion_yaml,
        &mapping(TDINFO_OUTPUT),
    )
    .unwrap()
    .unwrap();
    let parsed: serde_yaml::Mapping = serde_yaml::from_slice(&bytes).unwrap();
    assert_eq!(parsed.get("version").unwrap().as_str(), Some("2.0.2"));
}

#[test]
fn the_sourcetrack_is_nested_under_a_sourcetrack_key() {
    let bytes = KubernetesServerCatalog::server_build_manifest(
        &EtcContent::ServerBuildManifest_yaml,
        &mapping(TDINFO_OUTPUT),
    )
    .unwrap()
    .unwrap();
    let parsed: serde_yaml::Mapping = serde_yaml::from_slice(&bytes).unwrap();
    let sourcetrack = parsed.get("sourcetrack").unwrap().as_mapping().unwrap();
    let os = sourcetrack
        .get("tabsdata-os")
        .unwrap()
        .as_mapping()
        .unwrap();
    assert_eq!(os.get("tag").unwrap().as_str(), Some("2.0.2"));
    assert_eq!(
        os.get("short_commit_id").unwrap().as_str(),
        Some("d3fe88548")
    );
}

#[test]
fn a_missing_version_yields_no_document() {
    let info = mapping("sourcetrack:\n  tabsdata-os:\n    tag: 2.0.2\n");
    assert!(
        KubernetesServerCatalog::server_version(&EtcContent::ServerVersion_yaml, &info)
            .unwrap()
            .is_none()
    );
}

#[test]
fn an_absent_or_null_sourcetrack_yields_no_document() {
    for yaml in ["version: 2.0.2\n", "version: 2.0.2\nsourcetrack: null\n"] {
        assert!(
            KubernetesServerCatalog::server_build_manifest(
                &EtcContent::ServerBuildManifest_yaml,
                &mapping(yaml),
            )
            .unwrap()
            .is_none(),
            "expected no document for {yaml:?}"
        );
    }
}

#[tokio::test]
async fn the_python_versions_are_still_served() {
    let catalog = KubernetesServerCatalog::new();
    let bytes = catalog
        .read(&EtcContent::ValidPythonVersions_yaml)
        .await
        .unwrap()
        .unwrap();
    let parsed: serde_yaml::Mapping = serde_yaml::from_slice(&bytes).unwrap();
    let versions = parsed.get("versions").unwrap().as_sequence().unwrap();
    assert!(
        versions
            .iter()
            .any(|version| version.as_str() == Some("3.12.13"))
    );
}
