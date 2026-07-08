//
// Copyright 2026 Tabs Data Inc.
//

use crate::runtime::gateway::resolver::{ConfigurationResolver, SystemValues};

fn resolver_with_system(system: SystemValues) -> ConfigurationResolver {
    ConfigurationResolver::new(system)
}

fn resolver() -> ConfigurationResolver {
    resolver_with_system(SystemValues::default())
}

#[tokio::test]
async fn resolves_unsubstituted_text_unchanged() {
    let resolver = resolver();
    let resolved = resolver
        .resolve("plain content with no placeholders")
        .await
        .expect("resolve");
    assert_eq!(resolved.trim(), "plain content with no placeholders");
}

#[tokio::test]
async fn resolves_required_env_placeholder() {
    let key = "TD_TEST_RESOLVER_REQUIRED";
    unsafe { std::env::set_var(key, "world") };

    let resolver = resolver();
    let resolved = resolver
        .materialize("hello ${{env:TD_TEST_RESOLVER_REQUIRED}}")
        .await
        .expect("resolve");
    assert_eq!(resolved.trim(), "hello world");

    unsafe { std::env::remove_var(key) };
}

#[tokio::test]
async fn missing_required_env_returns_error() {
    unsafe { std::env::remove_var("TD_TEST_RESOLVER_MISSING") };

    let resolver = resolver();
    let result = resolver
        .materialize("${{env:TD_TEST_RESOLVER_MISSING}}")
        .await;
    assert!(result.is_err(), "expected missing env var to error");
}

#[tokio::test]
async fn optional_env_placeholder_falls_back_to_empty_string() {
    unsafe { std::env::remove_var("TD_TEST_RESOLVER_OPTIONAL") };

    let resolver = resolver();
    let resolved = resolver
        .materialize("prefix-${{env:TD_TEST_RESOLVER_OPTIONAL?}}-suffix")
        .await
        .expect("resolve");
    assert_eq!(resolved.trim(), "prefix--suffix");
}

#[tokio::test]
async fn multiple_env_placeholders_resolve_left_to_right() {
    unsafe {
        std::env::set_var("TD_TEST_RESOLVER_A", "alpha");
        std::env::set_var("TD_TEST_RESOLVER_B", "beta");
    }

    let resolver = resolver();
    let resolved = resolver
        .materialize("${{env:TD_TEST_RESOLVER_A}}/${{env:TD_TEST_RESOLVER_B}}")
        .await
        .expect("resolve");
    assert_eq!(resolved.trim(), "alpha/beta");

    unsafe {
        std::env::remove_var("TD_TEST_RESOLVER_A");
        std::env::remove_var("TD_TEST_RESOLVER_B");
    }
}

#[tokio::test]
async fn resolves_system_repository_url() {
    let resolver = resolver_with_system(SystemValues {
        repository_url: Some("file:///var/lib/tabsdata/instance/repo".to_string()),
        ..SystemValues::default()
    });
    let resolved = resolver
        .resolve("uri: ${sys:tdk_repository_url}/database/tabsdata.db")
        .await
        .expect("resolve");
    assert_eq!(
        resolved.trim(),
        "uri: file:///var/lib/tabsdata/instance/repo/database/tabsdata.db"
    );
}

#[tokio::test]
async fn missing_system_value_returns_error() {
    let resolver = resolver_with_system(SystemValues {
        repository_url: None,
        ..SystemValues::default()
    });
    let result = resolver.resolve("${sys:tdk_repository_url}").await;
    assert!(result.is_err(), "missing system value should error");
}

#[tokio::test]
async fn unknown_system_key_returns_error() {
    let resolver = resolver();
    let result = resolver.resolve("${sys:UNKNOWN_KEY}").await;
    assert!(result.is_err(), "unknown system key should error");
}

#[tokio::test]
async fn resolve_secret_substitutes_bare_placeholder() {
    let key = "TD_RESOLVE_TEST_BARE__VALUE";
    unsafe { std::env::set_var(key, "supersecret") };

    let resolver = resolver();
    let yaml = "td_resolve_test_bare:\n  value: ${{secret}}\n";
    let resolved = resolver.materialize(yaml).await.expect("resolve");
    let parsed: serde_yaml::Value = serde_yaml::from_str(&resolved).expect("yaml");
    assert_eq!(
        parsed["td_resolve_test_bare"]["value"].as_str(),
        Some("supersecret")
    );

    unsafe { std::env::remove_var(key) };
}

#[tokio::test]
async fn resolve_secret_substitutes_named_placeholder_using_path_encoding() {
    let key = "TD_RESOLVE_TEST_NAMED__VALUE";
    unsafe { std::env::set_var(key, "jwt-value") };

    let resolver = resolver();
    let yaml = "td_resolve_test_named:\n  value: ${{secret:jwt_signing_key}}\n";
    let resolved = resolver.materialize(yaml).await.expect("resolve");
    let parsed: serde_yaml::Value = serde_yaml::from_str(&resolved).expect("yaml");
    assert_eq!(
        parsed["td_resolve_test_named"]["value"].as_str(),
        Some("jwt-value")
    );

    unsafe { std::env::remove_var(key) };
}

#[tokio::test]
async fn connection_envfrom_secret_ref_materializes_without_identity_error() {
    let resolver = resolver_with_system(SystemValues {
        connection_secret_name: Some("td-conn-abc123".to_string()),
        ..SystemValues::default()
    });
    let yaml = "\
envFrom:
  - secretRef:
      name: ${{system:tdk_connection_secret_name}}
";
    let resolved = resolver
        .materialize(yaml)
        .await
        .expect("envFrom must materialize without keyed-sequence identity error");
    let parsed: serde_yaml::Value = serde_yaml::from_str(&resolved).expect("yaml");
    assert_eq!(
        parsed["envFrom"][0]["secretRef"]["name"].as_str(),
        Some("td-conn-abc123")
    );
}

#[tokio::test]
async fn resolve_secret_errors_when_env_var_missing() {
    unsafe { std::env::remove_var("TD_RESOLVE_TEST_MISSING__SECRET") };

    let resolver = resolver();
    let yaml = "td_resolve_test_missing:\n  secret: ${{secret}}\n";
    let result = resolver.materialize(yaml).await;
    assert!(result.is_err(), "missing env var should error");
}

#[tokio::test]
async fn materialize_still_resolves_system_placeholder() {
    let resolver = resolver_with_system(SystemValues {
        connection_secret_name: Some("td-conn-xyz".to_string()),
        ..SystemValues::default()
    });
    let yaml = "name: ${{system:tdk_connection_secret_name}}\n";
    let resolved = resolver.materialize(yaml).await.expect("materialize");
    let parsed: serde_yaml::Value = serde_yaml::from_str(&resolved).expect("yaml");
    assert_eq!(parsed["name"].as_str(), Some("td-conn-xyz"));
}

#[tokio::test]
async fn materialize_leaves_plain_scalars_untouched() {
    let resolver = resolver();
    let yaml = "a: plain\nb: 7\nc: true\n";
    let resolved = resolver.materialize(yaml).await.expect("materialize");
    let parsed: serde_yaml::Value = serde_yaml::from_str(&resolved).expect("yaml");
    assert_eq!(parsed["a"].as_str(), Some("plain"));
    assert_eq!(parsed["b"].as_i64(), Some(7));
    assert_eq!(parsed["c"].as_bool(), Some(true));
}

#[tokio::test]
async fn materialize_env_inside_keyed_sequence_item() {
    let key = "TD_TEST_RESOLVER_SEQ";
    unsafe { std::env::set_var(key, "seq-value") };

    let resolver = resolver();
    let yaml = "\
mounts:
- id: DATA
  uri: ${{env:TD_TEST_RESOLVER_SEQ}}
";
    let resolved = resolver.materialize(yaml).await.expect("materialize");
    let parsed: serde_yaml::Value = serde_yaml::from_str(&resolved).expect("yaml");
    assert_eq!(parsed["mounts"][0]["uri"].as_str(), Some("seq-value"));

    unsafe { std::env::remove_var(key) };
}

#[tokio::test]
async fn optional_secret_materializes_to_empty_when_absent() {
    unsafe { std::env::remove_var("TD_RESOLVE_TEST_OPTIONAL__SECRET") };

    let resolver = resolver();
    let yaml = "td_resolve_test_optional:\n  secret: ${{secret?}}\n";
    let resolved = resolver.materialize(yaml).await.expect("materialize");
    let parsed: serde_yaml::Value = serde_yaml::from_str(&resolved).expect("yaml");
    assert_eq!(
        parsed["td_resolve_test_optional"]["secret"].as_str(),
        Some("")
    );
}
