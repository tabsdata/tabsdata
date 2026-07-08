//
// Copyright 2026 Tabs Data Inc.
//

use super::*;

fn keys(items: &[&str]) -> BTreeSet<String> {
    items.iter().map(|item| (*item).to_owned()).collect()
}

#[test]
fn no_leaves_and_no_secret_is_ok() {
    let input = Settlement {
        config: "noseed.yaml",
        config_yaml: "name: noseed\nhost: example.com\n",
        secret: "td-app-noseed-secret-config-noseed.yaml",
        actual_keys: None,
    };
    settle_secret_keys(&input).expect("ok when no leaves and no secret");
}

#[test]
fn no_leaves_but_secret_exists_fails() {
    let input = Settlement {
        config: "noseed.yaml",
        config_yaml: "name: noseed\nhost: example.com\n",
        secret: "td-app-noseed-secret-config-noseed.yaml",
        actual_keys: Some(keys(&["STRAY"])),
    };
    let error = settle_secret_keys(&input).expect_err("unexpected secret");
    assert!(matches!(
        error,
        SecretError::SettlementSecretUnexpected { .. }
    ));
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
        ("PASSWORD", Generator::PostgresPassword),
    ];
    let filled = resolve_generated_values(&needs_fill, &BTreeMap::new()).expect("resolve");
    let filled: BTreeMap<&str, String> = filled.into_iter().collect();
    assert_eq!(filled.get("USER").map(String::as_str), Some("postgres"));
    assert!(!filled.get("PASSWORD").expect("password").is_empty());
}

#[test]
fn leaf_value_from_seed_is_reused_not_regenerated() {
    let needs_fill = [("KEY", Generator::JwtSigningKey)];
    let existing = seed(&[(Generator::JwtSigningKey, "existing-signing-key")]);
    let filled = resolve_generated_values(&needs_fill, &existing).expect("resolve");
    let filled: BTreeMap<&str, String> = filled.into_iter().collect();
    assert_eq!(
        filled.get("KEY").map(String::as_str),
        Some("existing-signing-key")
    );
}

#[test]
fn derived_token_resolves_from_cross_config_dependency() {
    let needs_fill = [("AGENT_TOKEN", Generator::APIServerServiceToken)];
    let existing = seed(&[(Generator::JwtSigningKey, "the-signing-key")]);
    let filled = resolve_generated_values(&needs_fill, &existing).expect("resolve");
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
    let filled = resolve_generated_values(&needs_fill, &BTreeMap::new()).expect("resolve");
    let filled: BTreeMap<&str, String> = filled.into_iter().collect();
    assert!(!filled.get("SIGNING_KEY").expect("key").is_empty());
    assert!(!filled.get("AGENT_TOKEN").expect("token").is_empty());
}

#[test]
fn derived_token_signed_with_seeded_key_matches_that_key() {
    use td_common::jwt::{JwtConfig, decode_token};

    let signing_key = "stable-signing-key";
    let needs_fill = [("AGENT_TOKEN", Generator::APIServerServiceToken)];
    let existing = seed(&[(Generator::JwtSigningKey, signing_key)]);
    let filled = resolve_generated_values(&needs_fill, &existing).expect("resolve");
    let filled: BTreeMap<&str, String> = filled.into_iter().collect();
    let token = filled.get("AGENT_TOKEN").expect("token");

    let jwt = JwtConfig::new(signing_key.to_owned(), 4_102_444_800);
    decode_token(&jwt, token).expect("token verifies against the seeded signing key");
}

#[test]
fn derived_token_missing_dependency_fails() {
    let needs_fill = [("AGENT_TOKEN", Generator::APIServerServiceToken)];
    let error = resolve_generated_values(&needs_fill, &BTreeMap::new()).expect_err("missing dep");
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

fn filled_map<'a>(pairs: Vec<(&'a str, String)>) -> BTreeMap<&'a str, String> {
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
    let a = filled_map(resolve_generated_values(&token_first, &BTreeMap::new()).expect("a"));
    let b = filled_map(resolve_generated_values(&key_first, &BTreeMap::new()).expect("b"));
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
    let filled = filled_map(resolve_generated_values(&needs_fill, &BTreeMap::new()).expect("run"));
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
    let filled = filled_map(resolve_generated_values(&needs_fill, &BTreeMap::new()).expect("run"));
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
    let existing = seed(&[(Generator::JwtSigningKey, signing_key)]);
    let filled = filled_map(resolve_generated_values(&needs_fill, &existing).expect("run"));

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
        ("PASSWORD", Generator::PostgresPassword),
        ("AGENT_TOKEN", Generator::APIServerServiceToken),
    ];
    let first = filled_map(resolve_generated_values(&needs_fill, &BTreeMap::new()).expect("first"));

    let reseed = seed(&[
        (Generator::JwtSigningKey, first.get("SIGNING_KEY").unwrap()),
        (Generator::PostgresPassword, first.get("PASSWORD").unwrap()),
    ]);
    let second = filled_map(resolve_generated_values(&needs_fill, &reseed).expect("second"));

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
    let existing = seed(&[(Generator::JwtSigningKey, signing_key)]);
    let filled = filled_map(resolve_generated_values(&needs_fill, &existing).expect("run"));

    assert!(!filled.contains_key("SIGNING_KEY"));
    assert_eq!(filled.len(), 1);
    let jwt = JwtConfig::new(signing_key.to_owned(), 4_102_444_800);
    decode_token(&jwt, filled.get("AGENT_TOKEN").expect("token")).expect("verifies");
}

#[test]
fn empty_needs_fill_yields_empty() {
    let existing = seed(&[(Generator::JwtSigningKey, "k")]);
    let filled = resolve_generated_values(&[], &existing).expect("run");
    assert!(filled.is_empty());
}

#[test]
fn merge_treats_distinct_nonempty_values_as_divergent_only() {
    let mut existing = BTreeMap::new();
    merge_existing_value(&mut existing, Generator::PostgresPassword, "p1".to_owned()).unwrap();
    merge_existing_value(&mut existing, Generator::PostgresPassword, "p1".to_owned()).unwrap();
    let error = merge_existing_value(&mut existing, Generator::PostgresPassword, "p2".to_owned())
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
