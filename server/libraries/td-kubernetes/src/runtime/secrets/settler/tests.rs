//
// Copyright 2026 Tabsdata Inc.
//

use super::*;

fn keys(items: &[&str]) -> BTreeSet<String> {
    items.iter().map(|item| (*item).to_owned()).collect()
}

#[test]
fn no_leaves_and_empty_secret_is_ok() {
    let input = Settlement {
        config: "noseed.yaml",
        config_yaml: "name: noseed\nhost: example.com\n",
        secret: "td-app-noseed-secret-config-noseed.yaml",
        actual_keys: Some(keys(&[])),
    };
    settle_secret_keys(&input).expect("ok when no leaves and empty secret");
}

#[test]
fn no_leaves_and_no_secret_fails() {
    let input = Settlement {
        config: "noseed.yaml",
        config_yaml: "name: noseed\nhost: example.com\n",
        secret: "td-app-noseed-secret-config-noseed.yaml",
        actual_keys: None,
    };
    let error = settle_secret_keys(&input).expect_err("missing secret");
    assert!(matches!(error, SecretError::SettlementSecretMissing { .. }));
}

#[test]
fn no_leaves_but_secret_has_keys_fails() {
    let input = Settlement {
        config: "noseed.yaml",
        config_yaml: "name: noseed\nhost: example.com\n",
        secret: "td-app-noseed-secret-config-noseed.yaml",
        actual_keys: Some(keys(&["STRAY"])),
    };
    let error = settle_secret_keys(&input).expect_err("stray keys");
    match error {
        SecretError::SettlementKeysMismatch { extra, missing, .. } => {
            assert!(extra.contains("STRAY"));
            assert_eq!(missing.trim(), "none.");
        }
        other => panic!("unexpected variant: {other:?}"),
    }
}

#[test]
fn leaves_and_no_secret_fails() {
    let input = Settlement {
        config: "apiserver.yaml",
        config_yaml: "database:\n  postgres:\n    password: ${{secret}}\n",
        secret: "td-app-apiserver-secret-config-apiserver.yaml",
        actual_keys: None,
    };
    let error = settle_secret_keys(&input).expect_err("missing secret");
    assert!(matches!(error, SecretError::SettlementSecretMissing { .. }));
}

#[test]
fn matching_keys_pass() {
    let input = Settlement {
        config: "apiserver.yaml",
        config_yaml: "database:\n  postgres:\n    password: ${{secret}}\n",
        secret: "td-app-apiserver-secret-config-apiserver.yaml",
        actual_keys: Some(keys(&["DATABASE__POSTGRES__PASSWORD"])),
    };
    settle_secret_keys(&input).expect("keys match");
}

#[test]
fn extra_keys_fail() {
    let input = Settlement {
        config: "apiserver.yaml",
        config_yaml: "database:\n  postgres:\n    password: ${{secret}}\n",
        secret: "td-app-apiserver-secret-config-apiserver.yaml",
        actual_keys: Some(keys(&["DATABASE__POSTGRES__PASSWORD", "EXTRA"])),
    };
    let error = settle_secret_keys(&input).expect_err("mismatch");
    match error {
        SecretError::SettlementKeysMismatch { extra, missing, .. } => {
            assert!(extra.contains("EXTRA"));
            assert_eq!(missing.trim(), "none.");
        }
        other => panic!("unexpected variant: {other:?}"),
    }
}

#[test]
fn missing_keys_fail() {
    let input = Settlement {
        config: "apiserver.yaml",
        config_yaml: "database:\n  postgres:\n    password: ${{secret}}\n  user: ${{secret}}\n",
        secret: "td-app-apiserver-secret-config-apiserver.yaml",
        actual_keys: Some(keys(&["DATABASE__POSTGRES__PASSWORD"])),
    };
    let error = settle_secret_keys(&input).expect_err("mismatch");
    match error {
        SecretError::SettlementKeysMismatch { extra, missing, .. } => {
            assert_eq!(extra.trim(), "none.");
            assert!(missing.contains("DATABASE__USER"));
        }
        other => panic!("unexpected variant: {other:?}"),
    }
}

#[test]
fn malformed_config_yaml_fails() {
    let input = Settlement {
        config: "broken.yaml",
        config_yaml: "not: : valid: yaml",
        secret: "td-app-broken-secret-config-broken.yaml",
        actual_keys: None,
    };
    let error = settle_secret_keys(&input).expect_err("parse fail");
    assert!(matches!(error, SecretError::ConfigYamlParseFailed { .. }));
}

fn seed(pairs: &[(Generator, &str)]) -> BTreeMap<Generator, String> {
    pairs
        .iter()
        .map(|(generator, value)| (*generator, (*value).to_owned()))
        .collect()
}

#[test]
fn self_contained_generators_are_produced() {
    let needs_fill = [
        ("USER", Generator::PostgresSystemUser),
        ("PASSWORD", Generator::PostgresSystemPassword),
    ];
    let filled = resolve_generated_values(&needs_fill, &mut BTreeMap::new());
    let filled: BTreeMap<&str, String> = filled.into_iter().collect();
    assert_eq!(filled.get("USER").map(String::as_str), Some("postgres"));
    assert!(!filled.get("PASSWORD").expect("password").is_empty());
}

#[test]
fn leaf_value_from_seed_is_reused_not_regenerated() {
    let needs_fill = [("KEY", Generator::JwtSigningKey)];
    let mut existing = seed(&[(Generator::JwtSigningKey, "existing-signing-key")]);
    let filled = resolve_generated_values(&needs_fill, &mut existing);
    let filled: BTreeMap<&str, String> = filled.into_iter().collect();
    assert_eq!(
        filled.get("KEY").map(String::as_str),
        Some("existing-signing-key")
    );
}

#[test]
fn divergent_values_across_configs_fail() {
    let mut existing = BTreeMap::new();
    merge_existing_value(&mut existing, Generator::JwtSigningKey, "one".to_owned())
        .expect("first value");
    let error = merge_existing_value(&mut existing, Generator::JwtSigningKey, "two".to_owned())
        .expect_err("divergent");
    match error {
        SecretError::GeneratorValueDivergent { generator } => {
            assert_eq!(generator, "jwt_signing_key");
        }
        other => panic!("unexpected variant: {other:?}"),
    }
}

#[test]
fn identical_values_across_configs_merge() {
    let mut existing = BTreeMap::new();
    merge_existing_value(&mut existing, Generator::JwtSigningKey, "same".to_owned())
        .expect("first value");
    merge_existing_value(&mut existing, Generator::JwtSigningKey, "same".to_owned())
        .expect("identical value merges");
    assert_eq!(existing.get(&Generator::JwtSigningKey).unwrap(), "same");
}

fn filled_map(pairs: Vec<(&str, String)>) -> BTreeMap<&str, String> {
    pairs.into_iter().collect()
}

#[test]
fn same_generator_two_keys_gets_same_value() {
    let needs_fill = [
        ("KEY_A", Generator::JwtSigningKey),
        ("KEY_B", Generator::JwtSigningKey),
    ];
    let filled = filled_map(resolve_generated_values(&needs_fill, &mut BTreeMap::new()));
    let a = filled.get("KEY_A").expect("A");
    let b = filled.get("KEY_B").expect("B");
    assert_eq!(a, b);
    assert!(!a.is_empty());
}

#[test]
fn second_run_reuses_first_runs_values() {
    let needs_fill = [
        ("SIGNING_KEY", Generator::JwtSigningKey),
        ("PASSWORD", Generator::PostgresSystemPassword),
        ("SERVICE_TOKEN", Generator::ServiceToken),
    ];
    let first = filled_map(resolve_generated_values(&needs_fill, &mut BTreeMap::new()));

    let mut reseed = seed(&[
        (Generator::JwtSigningKey, first.get("SIGNING_KEY").unwrap()),
        (
            Generator::PostgresSystemPassword,
            first.get("PASSWORD").unwrap(),
        ),
        (Generator::ServiceToken, first.get("SERVICE_TOKEN").unwrap()),
    ]);
    let second = filled_map(resolve_generated_values(&needs_fill, &mut reseed));

    assert_eq!(first.get("SIGNING_KEY"), second.get("SIGNING_KEY"));
    assert_eq!(first.get("PASSWORD"), second.get("PASSWORD"));
    assert_eq!(first.get("SERVICE_TOKEN"), second.get("SERVICE_TOKEN"));
}

#[test]
fn sequential_fills_share_freshly_generated_values() {
    let mut resolved = BTreeMap::new();
    let first = filled_map(resolve_generated_values(
        &[("PASSWORD", Generator::PostgresSystemPassword)],
        &mut resolved,
    ));
    let second = filled_map(resolve_generated_values(
        &[("OTHER_PASSWORD", Generator::PostgresSystemPassword)],
        &mut resolved,
    ));
    assert!(!first.get("PASSWORD").expect("first value").is_empty());
    assert_eq!(first.get("PASSWORD"), second.get("OTHER_PASSWORD"));
}

#[test]
fn a_generated_service_token_is_a_well_formed_token() {
    let needs_fill = [("SERVICE_TOKEN", Generator::ServiceToken)];
    let filled = filled_map(resolve_generated_values(&needs_fill, &mut BTreeMap::new()));
    let token = filled.get("SERVICE_TOKEN").expect("token");
    assert!(td_security::token::parse(token).is_some());
}

#[test]
fn a_service_token_reuses_the_existing_value_across_configs() {
    let needs_fill = [("SERVICE_TOKEN", Generator::ServiceToken)];
    let mut existing = seed(&[(Generator::ServiceToken, "already-issued-token")]);
    let filled = filled_map(resolve_generated_values(&needs_fill, &mut existing));
    assert_eq!(
        filled.get("SERVICE_TOKEN").map(String::as_str),
        Some("already-issued-token")
    );
}

#[test]
fn empty_needs_fill_yields_empty() {
    let mut existing = seed(&[(Generator::JwtSigningKey, "k")]);
    let filled = resolve_generated_values(&[], &mut existing);
    assert!(filled.is_empty());
}

#[test]
fn merge_treats_distinct_nonempty_values_as_divergent_only() {
    let mut existing = BTreeMap::new();
    merge_existing_value(
        &mut existing,
        Generator::PostgresSystemPassword,
        "p1".to_owned(),
    )
    .unwrap();
    merge_existing_value(
        &mut existing,
        Generator::PostgresSystemPassword,
        "p1".to_owned(),
    )
    .unwrap();
    let error = merge_existing_value(
        &mut existing,
        Generator::PostgresSystemPassword,
        "p2".to_owned(),
    )
    .expect_err("divergent");
    assert!(matches!(error, SecretError::GeneratorValueDivergent { .. }));
    merge_existing_value(
        &mut existing,
        Generator::PostgresSystemUser,
        "postgres".to_owned(),
    )
    .expect("independent generator");
}

#[test]
fn validation_accepts_two_generated_keys_together() {
    let config_yaml = "\
jwt:
  secret: ${{secretAuto:jwt_signing_key}}
service_token: ${{secretAuto:service_token}}
";
    let input = Settlement {
        config: "apiserver.service.yaml",
        config_yaml,
        secret: "td-app-apiserver-service-secret-config-apiserver.service.yaml",
        actual_keys: Some(keys(&["JWT__SECRET", "SERVICE_TOKEN"])),
    };
    settle_secret_keys(&input).expect("both generated keys accepted");
}

fn leaf(key: &str, generator: Option<Generator>) -> SecretLeaf {
    required_leaf(key, generator, Requirement::Mandatory)
}

fn required_leaf(key: &str, generator: Option<Generator>, requirement: Requirement) -> SecretLeaf {
    SecretLeaf {
        path: vec![key.to_owned()],
        key: key.to_owned(),
        generator,
        requirement,
    }
}

fn secret(items: &[(&str, &str)]) -> BTreeMap<String, Vec<u8>> {
    items
        .iter()
        .map(|(key, value)| ((*key).to_owned(), value.as_bytes().to_vec()))
        .collect()
}

#[test]
fn a_plain_key_with_no_secret_at_all_is_unsupplied() {
    let leaves = vec![leaf("password", None)];
    assert_eq!(unsupplied_keys(&leaves, None), vec!["password".to_owned()]);
}

#[test]
fn a_plain_key_already_supplied_is_not_reported() {
    let leaves = vec![leaf("password", None)];
    let live = secret(&[("password", "s3cret")]);
    assert!(unsupplied_keys(&leaves, Some(&live)).is_empty());
}

#[test]
fn a_plain_key_present_but_empty_is_unsupplied() {
    let leaves = vec![leaf("password", None)];
    let live = secret(&[("password", "")]);
    assert_eq!(
        unsupplied_keys(&leaves, Some(&live)),
        vec!["password".to_owned()]
    );
}

#[test]
fn a_generated_key_is_never_reported() {
    let leaves = vec![leaf("token", Some(Generator::JwtSigningKey))];
    assert!(unsupplied_keys(&leaves, None).is_empty());
}

#[test]
fn the_reported_keys_are_sorted_and_deduplicated() {
    let leaves = vec![leaf("zeta", None), leaf("alpha", None), leaf("zeta", None)];
    assert_eq!(
        unsupplied_keys(&leaves, None),
        vec!["alpha".to_owned(), "zeta".to_owned()]
    );
}
#[test]
fn the_pending_values_error_names_the_keys_and_the_commands() {
    let error = SecretError::SettlementValuesPending {
        instance: "demo1".to_owned(),
        count: 2,
        pending: "\n  - apiserver.storage.yaml: access_key, secret_key".to_owned(),
    };
    let rendered = error.to_string();
    assert!(rendered.contains("demo1"));
    assert!(rendered.contains("2 configuration secret(s)"));
    assert!(rendered.contains("access_key, secret_key"));
    assert!(rendered.contains("tdkserver config get --instance demo1"));
    assert!(rendered.contains("tdkserver config put --instance demo1"));
}

#[test]
fn an_optional_key_with_no_value_is_not_reported() {
    let leaves = vec![required_leaf("api_key", None, Requirement::Optional)];
    assert!(unsupplied_keys(&leaves, None).is_empty());
}

#[test]
fn an_optional_key_beside_a_mandatory_one_reports_only_the_mandatory() {
    let leaves = vec![
        required_leaf("api_key", None, Requirement::Optional),
        required_leaf("password", None, Requirement::Mandatory),
    ];
    assert_eq!(unsupplied_keys(&leaves, None), vec!["password".to_owned()]);
}

#[test]
fn a_held_value_of_a_retired_format_is_not_usable() {
    assert!(!usable(
        Generator::ServiceToken,
        "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.e30.sig"
    ));
    assert!(!usable(Generator::ServiceToken, ""));
    assert!(usable(
        Generator::ServiceToken,
        &td_security::token::generate()
    ));
    assert!(usable(Generator::JwtSigningKey, "held-key"));
}
