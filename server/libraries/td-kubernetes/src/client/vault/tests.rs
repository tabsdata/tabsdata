//
// Copyright 2026 Tabsdata Inc.
//

use super::*;

#[test]
fn a_generated_value_is_remembered_for_the_next_call() {
    let mut vault = InstanceVault::default();
    let first = vault.value_for("postgres_apiserver_password");
    let second = vault.value_for("postgres_apiserver_password");
    assert_eq!(first, second);
    assert!(!first.is_empty());
}

#[test]
fn each_category_gets_its_own_value() {
    let mut vault = InstanceVault::default();
    let apiserver = vault.value_for("postgres_apiserver_password");
    let aiagent = vault.value_for("postgres_aiagent_password");
    assert_ne!(apiserver, aiagent);
    assert_eq!(vault.secrets.len(), 2);
}

#[test]
fn a_stored_value_is_kept_instead_of_generating_a_new_one() {
    let mut vault = InstanceVault::default();
    vault
        .secrets
        .insert("postgres_apiserver_password".to_owned(), "kept".to_owned());
    assert_eq!(vault.value_for("postgres_apiserver_password"), "kept");
}

#[test]
fn an_empty_vault_reports_empty() {
    let mut vault = InstanceVault::default();
    assert!(vault.is_empty());
    vault.value_for("postgres_apiserver_password");
    assert!(!vault.is_empty());
}

#[test]
fn a_saved_vault_round_trips() {
    let mut vault = InstanceVault::default();
    let value = vault.value_for("postgres_apiserver_password");
    let data = serde_json::to_string_pretty(&vault).expect("serialize");
    let loaded: InstanceVault = serde_json::from_str(&data).expect("deserialize");
    assert_eq!(
        loaded.secrets.get("postgres_apiserver_password"),
        Some(&value)
    );
}

#[test]
fn a_generator_backed_category_gets_its_own_value() {
    let mut vault = InstanceVault::default();
    assert_eq!(vault.value_for("postgres_system_user"), "postgres");
}

#[test]
fn an_unknown_category_gets_a_password() {
    let mut vault = InstanceVault::default();
    let value = vault.value_for("postgres_apiserver_password");
    assert!(!value.is_empty());
    assert_ne!(value, "postgres");
}
