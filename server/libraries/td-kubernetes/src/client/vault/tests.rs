//
// Copyright 2026 Tabsdata Inc.
//

use super::*;

#[test]
fn a_provisioned_value_is_kept_on_the_next_pass() {
    let mut vault = vault();
    assert!(vault.provision_category("postgres_apiserver_password"));
    let first = vault.secrets().get("postgres_apiserver_password").cloned();
    assert!(!vault.provision_category("postgres_apiserver_password"));
    assert_eq!(
        vault.secrets().get("postgres_apiserver_password").cloned(),
        first
    );
}

#[test]
fn each_category_gets_its_own_value() {
    let mut vault = vault();
    assert!(vault.provision_category("postgres_apiserver_password"));
    assert!(vault.provision_category("postgres_system_password"));
    assert_ne!(
        vault.secrets().get("postgres_apiserver_password"),
        vault.secrets().get("postgres_system_password")
    );
    assert_eq!(vault.secrets().len(), 2);
}

#[test]
fn an_empty_vault_reports_empty() {
    let mut vault = vault();
    assert!(vault.is_empty());
    assert!(vault.provision_category("postgres_apiserver_password"));
    assert!(!vault.is_empty());
}

#[test]
fn a_saved_vault_round_trips() {
    let mut vault = vault();
    assert!(vault.provision_category("postgres_apiserver_password"));
    let value = vault.secrets().get("postgres_apiserver_password").cloned();
    let data = serde_json::to_string_pretty(&vault).expect("serialize");
    let loaded: Vault = serde_json::from_str(&data).expect("deserialize");
    assert_eq!(
        loaded.secrets().get("postgres_apiserver_password").cloned(),
        value
    );
}

#[test]
fn a_generated_password_is_not_the_system_user_constant() {
    let mut vault = vault();
    assert!(vault.provision_category("postgres_apiserver_password"));
    let value = vault
        .secrets()
        .get("postgres_apiserver_password")
        .expect("value");
    assert!(!value.is_empty());
    assert_ne!(value, "postgres");
}

fn vault() -> Vault {
    Vault::default()
}

#[test]
fn provisioning_fills_a_known_storable_category() {
    let mut vault = vault();
    assert!(vault.provision_category("postgres_system_user"));
    assert_eq!(
        vault
            .secrets()
            .get("postgres_system_user")
            .map(String::as_str),
        Some("postgres")
    );
}

#[test]
fn provisioning_skips_the_non_storable_generators() {
    let mut vault = vault();
    assert!(!vault.provision_category("jwt_signing_key"));
    assert!(!vault.provision_category("apiserver_service_token"));
    assert!(vault.is_empty());
}

#[test]
fn provisioning_skips_categories_without_a_generator() {
    let mut vault = vault();
    assert!(!vault.provision_category("openai-key"));
    assert!(vault.is_empty());
}

#[test]
fn provisioning_keeps_an_existing_value() {
    let mut vault = Vault {
        secrets: BTreeMap::from([("postgres_system_password".to_owned(), "kept".to_owned())]),
        ..Vault::default()
    };
    assert!(!vault.provision_category("postgres_system_password"));
    assert_eq!(
        vault
            .secrets()
            .get("postgres_system_password")
            .map(String::as_str),
        Some("kept")
    );
}

#[test]
fn storing_overwrites_a_supplied_value() {
    let mut vault = Vault {
        secrets: BTreeMap::from([("postgres_system_password".to_owned(), "old".to_owned())]),
        ..Vault::default()
    };
    assert!(vault.store("postgres_system_password", "new"));
    assert_eq!(
        vault
            .secrets()
            .get("postgres_system_password")
            .map(String::as_str),
        Some("new")
    );
}

#[test]
fn storing_reports_no_change_when_the_value_matches() {
    let mut vault = Vault {
        secrets: BTreeMap::from([("postgres_system_password".to_owned(), "same".to_owned())]),
        ..Vault::default()
    };
    assert!(!vault.store("postgres_system_password", "same"));
}

#[test]
fn storing_keeps_the_non_storable_generators_out() {
    let mut vault = vault();
    assert!(!vault.store("jwt_signing_key", "signing"));
    assert!(!vault.store("apiserver_service_token", "token"));
    assert!(vault.is_empty());
}

#[test]
fn storing_accepts_a_category_without_a_generator() {
    let mut vault = vault();
    assert!(vault.store("openai-key", "sk-real"));
    assert_eq!(
        vault.secrets().get("openai-key").map(String::as_str),
        Some("sk-real")
    );
}

#[test]
fn storing_ignores_an_empty_value() {
    let mut vault = vault();
    assert!(!vault.store("openai-key", ""));
    assert!(vault.is_empty());
}

#[test]
fn the_on_disk_shape_is_a_secrets_object() {
    let mut vault = vault();
    assert!(vault.provision_category("postgres_system_user"));
    let data = serde_json::to_string_pretty(&vault).expect("serialize");
    assert!(data.contains("\"secrets\""), "{data}");
    assert!(!data.contains("instance"), "{data}");
    let value: serde_json::Value = serde_json::from_str(&data).expect("json");
    assert_eq!(value["secrets"]["postgres_system_user"], "postgres");
}
