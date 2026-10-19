//
// Copyright 2026 Tabsdata Inc.
//

use super::*;
use td_objects::types::basic::ProjectId;

fn owner() -> SecretOwner {
    SecretOwner::project(&ProjectId::default()).unwrap()
}

fn hydrated(name: &str, entries: &[(&str, &str)]) -> SecretHydrated {
    SecretHydrated {
        name: SecretName::try_from(name).unwrap(),
        entries: entries
            .iter()
            .map(|(key, value)| SecretEntry {
                key: SecretKey::try_from(*key).unwrap(),
                value: Some((*value).to_owned()),
            })
            .collect(),
    }
}

fn bytes(data: &[(&str, &str)]) -> BTreeMap<String, Vec<u8>> {
    data.iter()
        .map(|(key, value)| ((*key).to_owned(), value.as_bytes().to_vec()))
        .collect()
}

#[test]
fn project_secrets_live_in_a_secret_named_after_the_project() {
    let owner = owner();
    assert_eq!(
        object_name(&owner).unwrap(),
        format!(
            "td-prj-secret-{}",
            owner.entity_id.to_string().to_lowercase()
        )
    );
}

#[test]
fn project_secrets_carry_the_project_scope_label_and_id() {
    let owner = owner();
    let labels = labels(&owner);
    assert_eq!(labels.get(LABEL_SCOPE).map(String::as_str), Some("prj"));
    assert_eq!(
        labels.get(LABEL_PROJECT_ID),
        Some(&owner.entity_id.to_string())
    );
}

#[test]
fn project_secrets_carry_the_project_annotation() {
    let annotations = annotations(&owner(), &EntityName::try_from("finance").unwrap());
    assert_eq!(
        annotations.get(ANNOTATION_PROJECT).map(String::as_str),
        Some("finance")
    );
}

#[test]
fn entries_become_data_keys_joined_by_a_double_underscore() {
    let data = encode(&[
        hydrated("warehouse", &[("user", "admin"), ("password", "p")]),
        hydrated("api", &[("token", "t")]),
    ])
    .unwrap();
    assert_eq!(
        data,
        BTreeMap::from([
            ("api__token".to_owned(), "t".to_owned()),
            ("warehouse__password".to_owned(), "p".to_owned()),
            ("warehouse__user".to_owned(), "admin".to_owned()),
        ])
    );
}

#[test]
fn data_keys_decode_back_into_secrets() {
    let secrets = decode(bytes(&[
        ("api__token", "t"),
        ("warehouse__password", "p"),
        ("warehouse__user", "admin"),
    ]))
    .unwrap();
    assert_eq!(
        secrets
            .iter()
            .map(|secret| secret.name.to_string())
            .collect::<Vec<_>>(),
        vec!["api", "warehouse"]
    );
    assert_eq!(secrets[1].entries.len(), 2);
    assert_eq!(secrets[1].entries[1].key.to_string(), "user");
    assert_eq!(secrets[1].entries[1].value.as_deref(), Some("admin"));
}

#[test]
fn an_empty_project_has_no_secrets() {
    assert!(encode(&[]).unwrap().is_empty());
    assert!(decode(BTreeMap::new()).unwrap().is_empty());
}

#[test]
fn a_data_key_without_the_separator_is_refused() {
    let error = decode(bytes(&[("warehouse", "p")])).unwrap_err();
    assert!(matches!(
        error.domain_err::<SecretError>(),
        SecretError::EntitySecretKeyMalformed { .. }
    ));
}

#[test]
fn a_data_key_with_an_invalid_name_is_refused() {
    let error = decode(bytes(&[("ware-house__user", "p")])).unwrap_err();
    assert!(matches!(
        error.domain_err::<SecretError>(),
        SecretError::EntitySecretKeyInvalid { .. }
    ));
}

#[test]
fn a_value_that_is_not_utf8_is_refused() {
    let data = BTreeMap::from([("warehouse__password".to_owned(), vec![0xff, 0xfe])]);
    let error = decode(data).unwrap_err();
    assert!(matches!(
        error.domain_err::<SecretError>(),
        SecretError::EntitySecretValueNotUtf8 { .. }
    ));
}

#[test]
fn secrets_above_one_mebibyte_are_refused() {
    let value = "a".repeat(SECRET_SIZE_LIMIT);
    let error = encode(&[hydrated("warehouse", &[("blob", &value)])]).unwrap_err();
    assert!(matches!(
        error.domain_err::<SecretError>(),
        SecretError::EntitySecretsTooLarge { .. }
    ));
    let value = "a".repeat(SECRET_SIZE_LIMIT - "warehouse__blob".len());
    assert!(encode(&[hydrated("warehouse", &[("blob", &value)])]).is_ok());
}
