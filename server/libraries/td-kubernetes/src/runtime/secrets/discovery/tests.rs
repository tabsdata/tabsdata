//
// Copyright 2026 Tabsdata Inc.
//

use super::*;

fn parse(yaml: &str) -> Value {
    serde_yaml::from_str(yaml).expect("yaml parses")
}

fn leaves_of(yaml: &str) -> Vec<SecretLeaf> {
    extract_secret_leaves("c.yaml", &mut parse(yaml)).expect("extract succeeds")
}

fn error_of(yaml: &str) -> SecretError {
    extract_secret_leaves("c.yaml", &mut parse(yaml)).expect_err("extract fails")
}

#[test]
fn empty_yaml_yields_no_leaves() {
    assert!(leaves_of("{}").is_empty());
}

#[test]
fn no_placeholders_yields_no_leaves() {
    let leaves = leaves_of(
        r#"
            database:
              host: localhost
              port: 5432
        "#,
    );
    assert!(leaves.is_empty());
}

#[test]
fn single_top_level_placeholder() {
    let leaves = leaves_of(r#"password: ${{secret}}"#);
    assert_eq!(leaves.len(), 1);
    assert_eq!(leaves[0].path, vec!["password".to_owned()]);
    assert_eq!(leaves[0].key, "PASSWORD");
}

#[test]
fn nested_placeholders_collected_in_order() {
    let leaves = leaves_of(
        r#"
            database:
              postgres:
                password: ${{secret}}
            auth:
              jwt_signing_key: ${{secret}}
            storage:
              s3:
                access_key: ${{secret}}
                secret_key: ${{secret}}
        "#,
    );
    let keys: Vec<&str> = leaves.iter().map(|leaf| leaf.key.as_str()).collect();
    assert_eq!(
        keys,
        vec![
            "DATABASE__POSTGRES__PASSWORD",
            "AUTH__JWT_SIGNING_KEY",
            "STORAGE__S3__ACCESS_KEY",
            "STORAGE__S3__SECRET_KEY",
        ]
    );
}

#[test]
fn placeholder_must_match_exactly() {
    let leaves = leaves_of(
        r#"
            a: " ${{secret}} "
            b: ${{secret}}-suffix
            c: ${SECRET}
        "#,
    );
    assert!(leaves.is_empty());
}

#[test]
fn sequence_without_placeholder_is_ignored() {
    let leaves = leaves_of(
        r#"
            allowed_origins:
              - https://a
              - https://b
            secret_value: ${{secret}}
        "#,
    );
    assert_eq!(leaves.len(), 1);
    assert_eq!(leaves[0].key, "SECRET_VALUE");
}

#[test]
fn sequence_of_maps_promotes_by_first_key() {
    let leaves = leaves_of(
        r#"
            storage:
              mounts:
                - id: ROOT
                  path: /
                  uri: ${{secret}}
                - id: BUNDLES
                  path: /bundles
                  uri: ${{secret}}
                - id: PREFIX
                  path: /foo
                  uri: ${{secret}}
        "#,
    );
    let keys: Vec<&str> = leaves.iter().map(|leaf| leaf.key.as_str()).collect();
    assert_eq!(
        keys,
        vec![
            "STORAGE__MOUNTS__ROOT__URI",
            "STORAGE__MOUNTS__BUNDLES__URI",
            "STORAGE__MOUNTS__PREFIX__URI",
        ]
    );
}

#[test]
fn promoted_sequence_with_mixed_items_is_rejected() {
    let error = error_of(
        r#"
            mounts:
              - id: ROOT
                uri: ${{secret}}
              - "not a map"
        "#,
    );
    assert!(matches!(
        error,
        SecretError::SequenceMixedItems { item_index: 1, .. }
    ));
}

#[test]
fn promoted_sequence_with_mismatched_first_keys_is_rejected() {
    let error = error_of(
        r#"
            mounts:
              - id: ROOT
                uri: ${{secret}}
              - name: BUNDLES
                uri: ${{secret}}
        "#,
    );
    let SecretError::SequenceFirstKeyMismatch {
        first_key,
        item_index,
        other_key,
        ..
    } = error
    else {
        panic!("expected SequenceFirstKeyMismatch");
    };
    assert_eq!(first_key, "id");
    assert_eq!(item_index, 1);
    assert_eq!(other_key, "name");
}

#[test]
fn promoted_sequence_with_duplicate_first_values_is_rejected() {
    let error = error_of(
        r#"
            mounts:
              - id: ROOT
                uri: ${{secret}}
              - id: ROOT
                uri: ${{secret}}
        "#,
    );
    let SecretError::SequenceDuplicateFirstValues {
        value,
        first_index,
        second_index,
        ..
    } = error
    else {
        panic!("expected SequenceDuplicateFirstValues");
    };
    assert_eq!(value, "ROOT");
    assert_eq!(first_index, 0);
    assert_eq!(second_index, 1);
}

#[test]
fn promoted_sequence_with_secret_as_first_value_is_rejected() {
    let error = error_of(
        r#"
            options:
              - option_1: ${{secret}}
                option_2: value2
        "#,
    );
    let SecretError::PromotedKeyIsSecret { path, .. } = error else {
        panic!("expected PromotedKeyIsSecret");
    };
    assert_eq!(path, "options");
}

#[test]
fn promoted_sequence_with_non_scalar_first_value_is_rejected() {
    let error = error_of(
        r#"
            mounts:
              - id: {nested: yes}
                uri: ${{secret}}
        "#,
    );
    assert!(matches!(
        error,
        SecretError::SequenceFirstValueNonScalar { item_index: 0, .. }
    ));
}

#[test]
fn sequence_without_secret_ignores_promotion_rules() {
    let leaves = leaves_of(
        r#"
            ports:
              - 80
              - 443
            secret_value: ${{secret}}
        "#,
    );
    let keys: Vec<&str> = leaves.iter().map(|leaf| leaf.key.as_str()).collect();
    assert_eq!(keys, vec!["SECRET_VALUE"]);
}

#[test]
fn key_with_dash_encodes_to_underscore() {
    let leaves = leaves_of(r#"jwt-key: ${{secret}}"#);
    assert_eq!(leaves[0].key, "JWT_KEY");
}

#[test]
fn key_with_dot_encodes_to_underscore() {
    let leaves = leaves_of(r#""app.kubernetes.io/component": ${{secret}}"#);
    assert_eq!(leaves[0].key, "APP_KUBERNETES_IO_COMPONENT");
}

#[test]
fn key_starting_with_digit_gets_underscore_prefix() {
    let leaves = leaves_of(r#""2fa": ${{secret}}"#);
    assert_eq!(leaves[0].key, "_2FA");
}

#[test]
fn collision_after_normalization_is_rejected() {
    let error = error_of(
        r#"
            jwt-key: ${{secret}}
            jwt_key: ${{secret}}
        "#,
    );
    let SecretError::KeyCollision {
        key, first, second, ..
    } = error
    else {
        panic!("expected KeyCollision");
    };
    assert_eq!(key, "JWT_KEY");
    assert_eq!(first, "jwt-key");
    assert_eq!(second, "jwt_key");
}

#[test]
fn empty_yaml_key_is_rejected() {
    let error = error_of(
        r#"
            ? ""
            : ${{secret}}
        "#,
    );
    let SecretError::EmptySegment { path, .. } = error else {
        panic!("expected EmptySegment");
    };
    assert_eq!(path, "");
}

#[test]
fn encode_single_segment() {
    let key = encode_secret_key("c.yaml", &["password".to_owned()]).unwrap();
    assert_eq!(key, "PASSWORD");
}

#[test]
fn encode_empty_path_is_empty_string() {
    let key = encode_secret_key("c.yaml", &[]).unwrap();
    assert_eq!(key, "");
}

#[test]
fn encode_rejects_empty_segment() {
    let error = encode_secret_key("c.yaml", &["a".to_owned(), "".to_owned()]).unwrap_err();
    assert!(matches!(error, SecretError::EmptySegment { .. }));
}

#[test]
fn bare_placeholder_has_no_generator() {
    let leaves = leaves_of(r#"password: ${{secret}}"#);
    assert_eq!(leaves.len(), 1);
    assert_eq!(leaves[0].generator, None);
}

#[test]
fn named_placeholder_jwt_signing_key_picks_jwt_signing_key_generator() {
    let leaves = leaves_of(r#"jwt: ${{secretAuto:jwt_signing_key}}"#);
    assert_eq!(leaves.len(), 1);
    assert_eq!(leaves[0].generator, Some(Generator::JwtSigningKey));
}

#[test]
fn named_placeholder_postgres_password_picks_postgres_password_generator() {
    let leaves = leaves_of(r#"password: ${{secretAuto:postgres_password}}"#);
    assert_eq!(leaves.len(), 1);
    assert_eq!(leaves[0].generator, Some(Generator::PostgresPassword));
}

#[test]
fn named_placeholder_postgres_system_user_picks_postgres_system_user_generator() {
    let leaves = leaves_of(r#"username: ${{secretAuto:postgres_system_user}}"#);
    assert_eq!(leaves.len(), 1);
    assert_eq!(leaves[0].generator, Some(Generator::PostgresSystemUser));
}

#[test]
fn mixed_placeholders_keep_their_kinds() {
    let leaves = leaves_of(
        r#"
            external: ${{secret}}
            jwt: ${{secretAuto:jwt_signing_key}}
            username: ${{secretAuto:postgres_system_user}}
            password: ${{secretAuto:postgres_password}}
        "#,
    );
    assert_eq!(leaves.len(), 4);
    let by_path: std::collections::HashMap<_, _> = leaves
        .iter()
        .map(|leaf| (leaf.path.join("."), leaf.generator))
        .collect();
    assert_eq!(by_path.get("external").copied(), Some(None));
    assert_eq!(
        by_path.get("jwt").copied(),
        Some(Some(Generator::JwtSigningKey))
    );
    assert_eq!(
        by_path.get("username").copied(),
        Some(Some(Generator::PostgresSystemUser))
    );
    assert_eq!(
        by_path.get("password").copied(),
        Some(Some(Generator::PostgresPassword))
    );
}

#[test]
fn named_secret_without_generator_is_a_bare_leaf() {
    let leaves = leaves_of(r#"key: ${{secret:openai-key}}"#);
    assert_eq!(leaves.len(), 1);
    assert_eq!(leaves[0].key, "KEY");
    assert_eq!(leaves[0].generator, None);
}

#[test]
fn optional_named_secret_is_a_bare_leaf() {
    let leaves = leaves_of(r#"key: ${{secret:openai-key?}}"#);
    assert_eq!(leaves.len(), 1);
    assert_eq!(leaves[0].key, "KEY");
    assert_eq!(leaves[0].generator, None);
}

#[test]
fn unknown_auto_generator_name_is_rejected() {
    let error = error_of(r#"key: ${{secretAuto:nope}}"#);
    match error {
        SecretError::UnknownGeneratorName {
            name, supported, ..
        } => {
            assert_eq!(name, "nope");
            assert!(supported.contains("jwt_signing_key"));
            assert!(supported.contains("postgres_system_user"));
            assert!(supported.contains("postgres_password"));
        }
        other => panic!("unexpected variant: {other:?}"),
    }
}

#[test]
fn anonymous_secret_with_empty_category_is_a_bare_leaf() {
    let leaves = leaves_of(r#"key: "${{secret:}}""#);
    assert_eq!(leaves.len(), 1);
    assert_eq!(leaves[0].key, "KEY");
    assert_eq!(leaves[0].generator, None);
}
