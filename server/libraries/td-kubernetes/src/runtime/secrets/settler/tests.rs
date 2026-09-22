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
    let filled = resolve_generated_values(&needs_fill, &mut BTreeMap::new()).expect("resolve");
    let filled: BTreeMap<&str, String> = filled.into_iter().collect();
    assert_eq!(filled.get("USER").map(String::as_str), Some("postgres"));
    assert!(!filled.get("PASSWORD").expect("password").is_empty());
}

#[test]
fn leaf_value_from_seed_is_reused_not_regenerated() {
    let needs_fill = [("KEY", Generator::JwtSigningKey)];
    let mut existing = seed(&[(Generator::JwtSigningKey, "existing-signing-key")]);
    let filled = resolve_generated_values(&needs_fill, &mut existing).expect("resolve");
    let filled: BTreeMap<&str, String> = filled.into_iter().collect();
    assert_eq!(
        filled.get("KEY").map(String::as_str),
        Some("existing-signing-key")
    );
}

#[test]
fn derived_token_resolves_from_cross_config_dependency() {
    let needs_fill = [("AGENT_TOKEN", Generator::APIServerServiceToken)];
    let mut existing = seed(&[(Generator::JwtSigningKey, "the-signing-key")]);
    let filled = resolve_generated_values(&needs_fill, &mut existing).expect("resolve");
    let filled: BTreeMap<&str, String> = filled.into_iter().collect();
    let token = filled.get("AGENT_TOKEN").expect("token");
    assert!(!token.is_empty());
}

#[test]
fn derived_and_dependency_resolve_together_in_one_pass() {
    let needs_fill = [
        ("AGENT_TOKEN", Generator::APIServerServiceToken),
        ("SIGNING_KEY", Generator::JwtSigningKey),
    ];
    let filled = resolve_generated_values(&needs_fill, &mut BTreeMap::new()).expect("resolve");
    let filled: BTreeMap<&str, String> = filled.into_iter().collect();
    assert!(!filled.get("SIGNING_KEY").expect("key").is_empty());
    assert!(!filled.get("AGENT_TOKEN").expect("token").is_empty());
}

#[test]
fn derived_token_signed_with_seeded_key_matches_that_key() {
    use td_common::jwt::{JwtConfig, decode_token};

    let signing_key = "stable-signing-key";
    let needs_fill = [("AGENT_TOKEN", Generator::APIServerServiceToken)];
    let mut existing = seed(&[(Generator::JwtSigningKey, signing_key)]);
    let filled = resolve_generated_values(&needs_fill, &mut existing).expect("resolve");
    let filled: BTreeMap<&str, String> = filled.into_iter().collect();
    let token = filled.get("AGENT_TOKEN").expect("token");

    let jwt = JwtConfig::new(signing_key.to_owned(), 4_102_444_800);
    decode_token(&jwt, token).expect("token verifies against the seeded signing key");
}

#[test]
fn derived_token_missing_dependency_fails() {
    let needs_fill = [("AGENT_TOKEN", Generator::APIServerServiceToken)];
    let error =
        resolve_generated_values(&needs_fill, &mut BTreeMap::new()).expect_err("missing dep");
    match error {
        SecretError::SecretDependencyMissing {
            generator,
            dependency,
        } => {
            assert_eq!(generator, "apiserver_service_token");
            assert_eq!(dependency, "jwt_signing_key");
        }
        other => panic!("unexpected variant: {other:?}"),
    }
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
fn derived_before_dependency_in_list_still_resolves() {
    let token_first = [
        ("AGENT_TOKEN", Generator::APIServerServiceToken),
        ("SIGNING_KEY", Generator::JwtSigningKey),
    ];
    let key_first = [
        ("SIGNING_KEY", Generator::JwtSigningKey),
        ("AGENT_TOKEN", Generator::APIServerServiceToken),
    ];
    let a = filled_map(resolve_generated_values(&token_first, &mut BTreeMap::new()).expect("a"));
    let b = filled_map(resolve_generated_values(&key_first, &mut BTreeMap::new()).expect("b"));
    for map in [&a, &b] {
        assert!(!map.get("SIGNING_KEY").expect("key").is_empty());
        assert!(!map.get("AGENT_TOKEN").expect("token").is_empty());
    }
}

#[test]
fn freshly_generated_token_matches_its_own_fresh_key() {
    use td_common::jwt::{JwtConfig, decode_token};

    let needs_fill = [
        ("SIGNING_KEY", Generator::JwtSigningKey),
        ("AGENT_TOKEN", Generator::APIServerServiceToken),
    ];
    let filled =
        filled_map(resolve_generated_values(&needs_fill, &mut BTreeMap::new()).expect("run"));
    let key = filled.get("SIGNING_KEY").expect("key");
    let token = filled.get("AGENT_TOKEN").expect("token");

    let good = JwtConfig::new(key.clone(), 4_102_444_800);
    decode_token(&good, token).expect("token verifies against its own key");

    let wrong = JwtConfig::new(format!("{key}-tampered"), 4_102_444_800);
    decode_token(&wrong, token).expect_err("token must NOT verify against a different key");
}

#[test]
fn same_generator_two_keys_gets_same_value() {
    let needs_fill = [
        ("KEY_A", Generator::JwtSigningKey),
        ("KEY_B", Generator::JwtSigningKey),
    ];
    let filled =
        filled_map(resolve_generated_values(&needs_fill, &mut BTreeMap::new()).expect("run"));
    let a = filled.get("KEY_A").expect("A");
    let b = filled.get("KEY_B").expect("B");
    assert_eq!(a, b);
    assert!(!a.is_empty());
}

#[test]
fn derived_signs_with_seeded_dependency_not_a_fresh_one() {
    use td_common::jwt::{JwtConfig, decode_token};

    let signing_key = "seeded-key-from-another-config";
    let needs_fill = [
        ("AGENT_TOKEN", Generator::APIServerServiceToken),
        ("SIGNING_KEY", Generator::JwtSigningKey),
    ];
    let mut existing = seed(&[(Generator::JwtSigningKey, signing_key)]);
    let filled = filled_map(resolve_generated_values(&needs_fill, &mut existing).expect("run"));

    assert_eq!(
        filled.get("SIGNING_KEY").map(String::as_str),
        Some(signing_key)
    );
    let jwt = JwtConfig::new(signing_key.to_owned(), 4_102_444_800);
    decode_token(&jwt, filled.get("AGENT_TOKEN").expect("token"))
        .expect("token must verify against the seeded key");
}

#[test]
fn second_run_reuses_first_runs_values() {
    use td_common::jwt::{JwtConfig, decode_token};

    let needs_fill = [
        ("SIGNING_KEY", Generator::JwtSigningKey),
        ("PASSWORD", Generator::PostgresSystemPassword),
        ("AGENT_TOKEN", Generator::APIServerServiceToken),
    ];
    let first =
        filled_map(resolve_generated_values(&needs_fill, &mut BTreeMap::new()).expect("first"));

    let mut reseed = seed(&[
        (Generator::JwtSigningKey, first.get("SIGNING_KEY").unwrap()),
        (
            Generator::PostgresSystemPassword,
            first.get("PASSWORD").unwrap(),
        ),
    ]);
    let second = filled_map(resolve_generated_values(&needs_fill, &mut reseed).expect("second"));

    assert_eq!(first.get("SIGNING_KEY"), second.get("SIGNING_KEY"));
    assert_eq!(first.get("PASSWORD"), second.get("PASSWORD"));
    let key = second.get("SIGNING_KEY").unwrap();
    let jwt = JwtConfig::new(key.clone(), 4_102_444_800);
    decode_token(&jwt, second.get("AGENT_TOKEN").unwrap())
        .expect("re-derived token still verifies against the reused key");
}

#[test]
fn partial_fill_only_missing_derived_key() {
    use td_common::jwt::{JwtConfig, decode_token};

    let signing_key = "already-present-key";
    let needs_fill = [("AGENT_TOKEN", Generator::APIServerServiceToken)];
    let mut existing = seed(&[(Generator::JwtSigningKey, signing_key)]);
    let filled = filled_map(resolve_generated_values(&needs_fill, &mut existing).expect("run"));

    assert!(!filled.contains_key("SIGNING_KEY"));
    assert_eq!(filled.len(), 1);
    let jwt = JwtConfig::new(signing_key.to_owned(), 4_102_444_800);
    decode_token(&jwt, filled.get("AGENT_TOKEN").expect("token")).expect("verifies");
}

#[test]
fn sequential_fills_share_freshly_generated_values() {
    let mut resolved = BTreeMap::new();
    let first = filled_map(
        resolve_generated_values(
            &[("PASSWORD", Generator::PostgresSystemPassword)],
            &mut resolved,
        )
        .expect("first"),
    );
    let second = filled_map(
        resolve_generated_values(
            &[("OTHER_PASSWORD", Generator::PostgresSystemPassword)],
            &mut resolved,
        )
        .expect("second"),
    );
    assert!(!first.get("PASSWORD").expect("first value").is_empty());
    assert_eq!(first.get("PASSWORD"), second.get("OTHER_PASSWORD"));
}

#[test]
fn derived_token_reuses_existing_value_across_configs() {
    let needs_fill = [("AGENT_TOKEN", Generator::APIServerServiceToken)];
    let mut existing = seed(&[(Generator::APIServerServiceToken, "already-issued-token")]);
    let filled = filled_map(resolve_generated_values(&needs_fill, &mut existing).expect("run"));
    assert_eq!(
        filled.get("AGENT_TOKEN").map(String::as_str),
        Some("already-issued-token")
    );
}

#[test]
fn empty_needs_fill_yields_empty() {
    let mut existing = seed(&[(Generator::JwtSigningKey, "k")]);
    let filled = resolve_generated_values(&[], &mut existing).expect("run");
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
fn validation_accepts_generated_and_derived_keys_together() {
    let config_yaml = "\
jwt:
  secret: ${{secretAuto:jwt_signing_key}}
agent_token: ${{secretAuto:apiserver_service_token}}
";
    let input = Settlement {
        config: "apiserver.service.yaml",
        config_yaml,
        secret: "td-app-apiserver-service-secret-config-apiserver.service.yaml",
        actual_keys: Some(keys(&["JWT__SECRET", "AGENT_TOKEN"])),
    };
    settle_secret_keys(&input).expect("both generated keys accepted");
}

fn declared(pairs: &[(&str, &str, Generator)]) -> Vec<(String, Vec<SecretLeaf>)> {
    let mut by_secret: Vec<(String, Vec<SecretLeaf>)> = Vec::new();
    for (secret, key, generator) in pairs {
        let leaf = SecretLeaf {
            path: Vec::new(),
            key: (*key).to_owned(),
            generator: Some(*generator),
            requirement: Requirement::Mandatory,
        };
        match by_secret.iter_mut().find(|(name, _)| name == secret) {
            Some((_, leaves)) => leaves.push(leaf),
            None => by_secret.push(((*secret).to_owned(), vec![leaf])),
        }
    }
    by_secret
}

#[test]
fn invalidated_generators_include_declared_but_missing() {
    let leaves = declared(&[
        ("a", "PASSWORD", Generator::PostgresSystemPassword),
        ("b", "KEY", Generator::JwtSigningKey),
    ]);
    let existing = seed(&[(Generator::JwtSigningKey, "key")]);
    let invalidated = invalidated_generators(&leaves, &existing);
    assert!(invalidated.contains(&Generator::PostgresSystemPassword));
    assert!(!invalidated.contains(&Generator::JwtSigningKey));
}

#[test]
fn invalidated_generators_cascade_to_stored_dependents() {
    let leaves = declared(&[
        ("a", "KEY", Generator::JwtSigningKey),
        ("b", "AGENT_TOKEN", Generator::APIServerServiceToken),
    ]);
    let existing = seed(&[(Generator::APIServerServiceToken, "stale-token")]);
    let invalidated = invalidated_generators(&leaves, &existing);
    assert!(invalidated.contains(&Generator::JwtSigningKey));
    assert!(
        invalidated.contains(&Generator::APIServerServiceToken),
        "a stored dependent must be regenerated when its dependency is regenerated"
    );
}

#[test]
fn invalidated_generators_empty_when_all_values_exist() {
    let leaves = declared(&[
        ("a", "KEY", Generator::JwtSigningKey),
        ("b", "AGENT_TOKEN", Generator::APIServerServiceToken),
    ]);
    let existing = seed(&[
        (Generator::JwtSigningKey, "key"),
        (Generator::APIServerServiceToken, "token"),
    ]);
    let invalidated = invalidated_generators(&leaves, &existing);
    assert!(invalidated.is_empty());
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
