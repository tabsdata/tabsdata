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
        .resolve_sys("plain content with no placeholders")
        .await
        .expect("resolve");
    assert_eq!(resolved, "plain content with no placeholders");
}

#[tokio::test]
async fn resolves_system_repository_url() {
    let resolver = resolver_with_system(SystemValues {
        repository_url: Some("file:///var/lib/tabsdata/instance/repo".to_string()),
        ..SystemValues::default()
    });
    let resolved = resolver
        .resolve_sys("uri: ${sys:TDK_REPOSITORY_URL}/database/tabsdata.db")
        .await
        .expect("resolve");
    assert_eq!(
        resolved,
        "uri: file:///var/lib/tabsdata/instance/repo/database/tabsdata.db"
    );
}

#[tokio::test]
async fn missing_system_value_returns_error() {
    let resolver = resolver_with_system(SystemValues {
        repository_url: None,
        ..SystemValues::default()
    });
    let result = resolver.resolve_sys("${sys:TDK_REPOSITORY_URL}").await;
    assert!(result.is_err(), "missing system value should error");
}

#[tokio::test]
async fn unknown_system_key_returns_error() {
    let resolver = resolver();
    let result = resolver.resolve_sys("${sys:UNKNOWN_KEY}").await;
    assert!(result.is_err(), "unknown system key should error");
}

#[tokio::test]
async fn resolve_secret_substitutes_bare_placeholder() {
    let key = "TD_RESOLVE_TEST_BARE__VALUE";
    unsafe { std::env::set_var(key, "supersecret") };

    let resolver = resolver();
    let yaml = "td_resolve_test_bare:\n  value: ${secret}\n";
    let resolved = resolver.resolve_secret(yaml).await.expect("resolve");
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
    let yaml = "td_resolve_test_named:\n  value: ${secret:jwt_signing_key}\n";
    let resolved = resolver.resolve_secret(yaml).await.expect("resolve");
    let parsed: serde_yaml::Value = serde_yaml::from_str(&resolved).expect("yaml");
    assert_eq!(
        parsed["td_resolve_test_named"]["value"].as_str(),
        Some("jwt-value")
    );

    unsafe { std::env::remove_var(key) };
}

#[tokio::test]
async fn resolve_secret_errors_when_env_var_missing() {
    unsafe { std::env::remove_var("TD_RESOLVE_TEST_MISSING__SECRET") };

    let resolver = resolver();
    let yaml = "td_resolve_test_missing:\n  secret: ${secret}\n";
    let result = resolver.resolve_secret(yaml).await;
    assert!(result.is_err(), "missing env var should error");
}
