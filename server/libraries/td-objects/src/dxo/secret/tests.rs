//
// Copyright 2026 Tabsdata Inc.
//

use super::*;

fn secret(name: &str, keys: &[&str]) -> SecretUpsert {
    SecretUpsert {
        name: SecretName::try_from(name).unwrap(),
        entries: keys
            .iter()
            .map(|key| SecretEntry {
                key: SecretKey::try_from(*key).unwrap(),
                value: Some("value".to_owned()),
            })
            .collect(),
    }
}

#[test]
fn names_and_keys_follow_the_pattern() {
    for valid in ["warehouse", "Warehouse2", "api_key", "a", "A1_b2_C3"] {
        assert!(SecretName::try_from(valid).is_ok(), "{valid}");
        assert!(SecretKey::try_from(valid).is_ok(), "{valid}");
    }
    for invalid in [
        "",
        "1st",
        "_lead",
        "trail_",
        "double__under",
        "dash-name",
        "dot.name",
        "space name",
    ] {
        assert!(SecretName::try_from(invalid).is_err(), "{invalid}");
        assert!(SecretKey::try_from(invalid).is_err(), "{invalid}");
    }
}

#[test]
fn names_and_keys_are_at_most_63_characters() {
    assert!(SecretName::try_from("a".repeat(63)).is_ok());
    assert!(SecretName::try_from("a".repeat(64)).is_err());
    assert!(SecretKey::try_from("a".repeat(63)).is_ok());
    assert!(SecretKey::try_from("a".repeat(64)).is_err());
}

#[test]
fn distinct_names_and_keys_are_valid() {
    let secrets = [
        secret("warehouse", &["user", "password"]),
        secret("api", &["token"]),
    ];
    assert!(validate_secrets(&secrets).is_ok());
}

#[test]
fn a_name_repeated_ignoring_case_is_refused() {
    let secrets = [
        secret("warehouse", &["user"]),
        secret("Warehouse", &["token"]),
    ];
    assert!(validate_secrets(&secrets).is_err());
}

#[test]
fn a_key_repeated_ignoring_case_is_refused() {
    let secrets = [secret("warehouse", &["password", "PASSWORD"])];
    assert!(validate_secrets(&secrets).is_err());
}

#[test]
fn a_secret_without_keys_is_refused() {
    let secrets = [secret("warehouse", &[])];
    assert!(validate_secrets(&secrets).is_err());
}

#[test]
fn a_value_never_appears_in_debug_output() {
    let shown = format!("{:?}", secret("warehouse", &["password"]));
    assert!(!shown.contains("value\": Some(\"value\")"), "{shown}");
    assert!(shown.contains("<redacted>"), "{shown}");
}

#[test]
fn a_project_owns_its_secrets_in_the_project_scope() {
    let project_id = ProjectId::default();
    let owner = SecretOwner::project(&project_id).unwrap();
    assert_eq!(owner.scope, SecretScope::Project);
    assert_eq!(owner.entity_id.to_string(), project_id.to_string());
    assert_eq!(owner.scope.as_ref(), "prj");
}
