//
// Copyright 2026 Tabsdata Inc.
//

use super::*;

use crate::base::constants::SECRET_FAMILY_CONFIG;
use crate::runtime::secrets::discovery::Requirement;

fn leaves(keys: &[(&str, &str)]) -> Vec<SecretLeaf> {
    keys.iter()
        .map(|(path, key)| SecretLeaf {
            path: path.split('.').map(str::to_owned).collect(),
            key: (*key).to_owned(),
            generator: None,
            requirement: Requirement::Mandatory,
        })
        .collect()
}

#[test]
fn manifest_carries_name_type_and_labels_but_no_namespace() {
    let leaves = leaves(&[("database.postgres.password", "DATABASE__POSTGRES__PASSWORD")]);
    let secret = build_secret_manifest(
        "td-app-apiserver-secret-config-apiserver.yaml",
        SECRET_FAMILY_CONFIG,
        &leaves,
        None,
    );
    let metadata = &secret.metadata;
    assert_eq!(
        metadata.name.as_deref(),
        Some("td-app-apiserver-secret-config-apiserver.yaml")
    );
    assert!(metadata.namespace.is_none());
    assert_eq!(secret.type_.as_deref(), Some("Opaque"));
    let labels = metadata.labels.as_ref().expect("labels present");
    assert_eq!(
        labels.get("app.kubernetes.io/part-of").map(String::as_str),
        Some("tabsdata")
    );
    assert_eq!(
        labels.get("tabsdata.com/secret-family").map(String::as_str),
        Some("secret-config")
    );
}

#[test]
fn manifest_without_values_fills_empty_strings() {
    let leaves = leaves(&[("a.b", "A__B"), ("c", "C")]);
    let secret = build_secret_manifest("name", SECRET_FAMILY_CONFIG, &leaves, None);
    let string_data = secret.string_data.as_ref().expect("string_data present");
    assert_eq!(string_data.get("A__B").map(String::as_str), Some(""));
    assert_eq!(string_data.get("C").map(String::as_str), Some(""));
    assert_eq!(string_data.len(), 2);
}

#[test]
fn manifest_with_values_uses_them() {
    let leaves = leaves(&[("a.b", "A__B"), ("c", "C")]);
    let mut values = BTreeMap::new();
    values.insert("A__B".to_owned(), "supersecret".to_owned());
    values.insert("C".to_owned(), "another".to_owned());
    let secret = build_secret_manifest("name", SECRET_FAMILY_CONFIG, &leaves, Some(&values));
    let string_data = secret.string_data.as_ref().expect("string_data present");
    assert_eq!(
        string_data.get("A__B").map(String::as_str),
        Some("supersecret")
    );
    assert_eq!(string_data.get("C").map(String::as_str), Some("another"));
}

#[test]
fn manifest_with_partial_values_fills_missing_keys_with_empty() {
    let leaves = leaves(&[("a", "A"), ("b", "B")]);
    let mut values = BTreeMap::new();
    values.insert("A".to_owned(), "only-a".to_owned());
    let secret = build_secret_manifest("name", SECRET_FAMILY_CONFIG, &leaves, Some(&values));
    let string_data = secret.string_data.as_ref().expect("string_data present");
    assert_eq!(string_data.get("A").map(String::as_str), Some("only-a"));
    assert_eq!(string_data.get("B").map(String::as_str), Some(""));
}

#[test]
fn manifest_serializes_to_kubectl_applyable_yaml() {
    let leaves = leaves(&[
        ("database.postgres.password", "DATABASE__POSTGRES__PASSWORD"),
        ("auth.jwt_signing_key", "AUTH__JWT_SIGNING_KEY"),
    ]);
    let mut values = BTreeMap::new();
    values.insert("DATABASE__POSTGRES__PASSWORD".to_owned(), "pgpw".to_owned());
    values.insert("AUTH__JWT_SIGNING_KEY".to_owned(), "jwtkey".to_owned());
    let secret = build_secret_manifest(
        "td-app-apiserver-secret-config-apiserver.yaml",
        SECRET_FAMILY_CONFIG,
        &leaves,
        Some(&values),
    );
    let yaml = serialize_secret_manifest(&secret).expect("serializes");

    assert!(yaml.contains("kind: Secret"));
    assert!(yaml.contains("name: td-app-apiserver-secret-config-apiserver.yaml"));
    assert!(!yaml.contains("namespace:"));
    assert!(yaml.contains("type: Opaque"));
    assert!(yaml.contains("stringData:"));
    assert!(yaml.contains("DATABASE__POSTGRES__PASSWORD: pgpw"));
    assert!(yaml.contains("AUTH__JWT_SIGNING_KEY: jwtkey"));
    assert!(yaml.contains("app.kubernetes.io/part-of: tabsdata"));
    assert!(yaml.contains("tabsdata.com/secret-family: secret-config"));

    let reparsed: Secret = serde_yaml::from_str(&yaml).expect("yaml reparses as Secret");
    assert_eq!(
        reparsed.metadata.name.as_deref(),
        Some("td-app-apiserver-secret-config-apiserver.yaml")
    );
}

#[test]
fn manifest_with_zero_leaves_still_serializes() {
    let secret = build_secret_manifest("name", SECRET_FAMILY_CONFIG, &[], None);
    let yaml = serialize_secret_manifest(&secret).expect("serializes");
    assert!(yaml.contains("kind: Secret"));
    assert!(yaml.contains("stringData: {}") || yaml.contains("stringData:"));
}
