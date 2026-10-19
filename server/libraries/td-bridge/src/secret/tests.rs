//
// Copyright 2026 Tabsdata Inc.
//

use super::file::FileSecretStore;
use super::memory::InMemorySecretStore;
use super::noop::NoopSecretStore;
use super::project::ProjectSecrets;
use super::*;
use td_objects::types::basic::{ProjectId, ProjectName, SecretKey, SecretName};

fn entry(key: &str, value: Option<&str>) -> SecretEntry {
    SecretEntry {
        key: SecretKey::try_from(key).unwrap(),
        value: value.map(str::to_owned),
    }
}

fn secret(name: &str, entries: Vec<SecretEntry>) -> SecretUpsert {
    SecretUpsert {
        name: SecretName::try_from(name).unwrap(),
        entries,
    }
}

fn hydrated(name: &str, entries: Vec<SecretEntry>) -> SecretHydrated {
    SecretHydrated {
        name: SecretName::try_from(name).unwrap(),
        entries,
    }
}

fn values(secrets: &[SecretHydrated]) -> Vec<(String, String, Option<String>)> {
    secrets
        .iter()
        .flat_map(|secret| {
            secret.entries.iter().map(|entry| {
                (
                    secret.name.to_string(),
                    entry.key.to_string(),
                    entry.value.clone(),
                )
            })
        })
        .collect()
}

fn owner() -> SecretOwner {
    SecretOwner::project(&ProjectId::default()).unwrap()
}

fn entity() -> EntityName {
    EntityName::try_from("project").unwrap()
}

#[test]
fn a_missing_value_keeps_the_stored_one() {
    let stored = [hydrated("warehouse", vec![entry("password", Some("old"))])];
    let desired = [secret(
        "warehouse",
        vec![entry("password", None), entry("user", Some("admin"))],
    )];
    let merged = merge_kept(&desired, &stored).unwrap();
    assert_eq!(
        values(&merged),
        vec![
            ("warehouse".into(), "password".into(), Some("old".into())),
            ("warehouse".into(), "user".into(), Some("admin".into())),
        ]
    );
}

#[test]
fn a_missing_value_without_a_stored_one_is_refused() {
    let stored = [hydrated("warehouse", vec![entry("user", Some("admin"))])];
    let desired = [secret("warehouse", vec![entry("password", None)])];
    assert!(merge_kept(&desired, &stored).is_err());
}

#[test]
fn a_missing_value_does_not_borrow_from_another_secret() {
    let stored = [hydrated("api", vec![entry("password", Some("old"))])];
    let desired = [secret("warehouse", vec![entry("password", None)])];
    assert!(merge_kept(&desired, &stored).is_err());
}

#[test]
fn merging_replaces_everything_not_given() {
    let stored = [
        hydrated("warehouse", vec![entry("password", Some("old"))]),
        hydrated("api", vec![entry("token", Some("t"))]),
    ];
    let desired = [secret("warehouse", vec![entry("user", Some("admin"))])];
    let merged = merge_kept(&desired, &stored).unwrap();
    assert_eq!(
        values(&merged),
        vec![("warehouse".into(), "user".into(), Some("admin".into()))]
    );
}

#[test]
fn merging_validates_the_desired_secrets() {
    let desired = [
        secret("warehouse", vec![entry("user", Some("a"))]),
        secret("WAREHOUSE", vec![entry("user", Some("b"))]),
    ];
    assert!(merge_kept(&desired, &[]).is_err());
}

#[test]
fn reads_carry_names_and_keys_only() {
    let read = read_of(&[hydrated(
        "warehouse",
        vec![entry("user", Some("admin")), entry("password", Some("p"))],
    )]);
    assert_eq!(read.len(), 1);
    assert_eq!(read[0].name.to_string(), "warehouse");
    let keys: Vec<String> = read[0].keys.iter().map(ToString::to_string).collect();
    assert_eq!(keys, vec!["user", "password"]);
}

#[tokio::test]
async fn the_memory_store_round_trips_and_keeps_values() {
    let store = InMemorySecretStore::default();
    let owner = owner();
    store
        .upsert(
            &owner,
            &entity(),
            &[secret("warehouse", vec![entry("password", Some("p"))])],
        )
        .await
        .unwrap();
    let read = store
        .upsert(
            &owner,
            &entity(),
            &[secret("warehouse", vec![entry("password", None)])],
        )
        .await
        .unwrap();
    assert_eq!(read[0].keys[0].to_string(), "password");
    assert_eq!(
        values(&store.fetch_hydrated(&owner).await.unwrap()),
        vec![("warehouse".into(), "password".into(), Some("p".into()))]
    );
    store.delete(&owner).await.unwrap();
    store.delete(&owner).await.unwrap();
    assert!(store.fetch(&owner).await.unwrap().is_empty());
}

#[tokio::test]
async fn the_memory_store_keeps_owners_apart() {
    let store = InMemorySecretStore::default();
    let first = owner();
    let second = owner();
    store
        .upsert(
            &first,
            &entity(),
            &[secret("warehouse", vec![entry("password", Some("p"))])],
        )
        .await
        .unwrap();
    assert!(store.fetch(&second).await.unwrap().is_empty());
}

#[tokio::test]
async fn project_secrets_are_stored_under_the_project_owner() {
    let store = InMemorySecretStore::default();
    let project_id = ProjectId::default();
    let project = ProjectName::try_from("finance").unwrap();
    ProjectSecrets::new(&store)
        .upsert(
            &project_id,
            &project,
            &[secret("warehouse", vec![entry("password", Some("p"))])],
        )
        .await
        .unwrap();
    let owner = SecretOwner::project(&project_id).unwrap();
    assert_eq!(store.fetch(&owner).await.unwrap().len(), 1);
    assert_eq!(
        ProjectSecrets::new(&store)
            .fetch(&project_id)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn the_noop_store_is_unavailable() {
    let store = NoopSecretStore;
    assert!(store.fetch(&owner()).await.is_err());
    assert!(store.delete(&owner()).await.is_err());
}

#[tokio::test]
async fn the_file_store_accepts_everything_and_keeps_nothing() {
    let store = FileSecretStore;
    let owner = owner();
    let written = store
        .upsert(
            &owner,
            &entity(),
            &[secret("warehouse", vec![entry("password", Some("p"))])],
        )
        .await
        .unwrap();
    assert!(written.is_empty());
    assert!(store.fetch(&owner).await.unwrap().is_empty());
    assert!(store.fetch_hydrated(&owner).await.unwrap().is_empty());
    assert!(store.delete(&owner).await.is_ok());
}

#[tokio::test]
async fn renaming_a_project_keeps_its_secret_values() {
    let store = InMemorySecretStore::default();
    let project_id = ProjectId::default();
    let secrets = ProjectSecrets::new(&store);
    secrets
        .upsert(
            &project_id,
            &ProjectName::try_from("finance").unwrap(),
            &[secret(
                "warehouse",
                vec![entry("user", Some("admin")), entry("password", Some("p"))],
            )],
        )
        .await
        .unwrap();
    secrets
        .rename(&project_id, &ProjectName::try_from("treasury").unwrap())
        .await
        .unwrap();
    assert_eq!(
        values(&secrets.fetch_hydrated(&project_id).await.unwrap()),
        vec![
            ("warehouse".into(), "user".into(), Some("admin".into())),
            ("warehouse".into(), "password".into(), Some("p".into())),
        ]
    );
}
