//
// Copyright 2026 Tabs Data Inc.
//

use td_kubernetes::gateway::resolver::{
    ConfigurationResolver, HashicorpConfig, SystemValues,
};

fn resolver_with_system(system: SystemValues) -> ConfigurationResolver {
    ConfigurationResolver::new(HashicorpConfig::default(), system)
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
    assert_eq!(resolved, "plain content with no placeholders");
}

#[tokio::test]
async fn resolves_required_env_placeholder() {
    let key = "TD_TEST_RESOLVER_REQUIRED";
    // SAFETY: tests are scoped to this process; env writes are intentional.
    unsafe { std::env::set_var(key, "world") };

    let resolver = resolver();
    let resolved = resolver
        .resolve("hello ${env:TD_TEST_RESOLVER_REQUIRED}")
        .await
        .expect("resolve");
    assert_eq!(resolved, "hello world");

    unsafe { std::env::remove_var(key) };
}

#[tokio::test]
async fn missing_required_env_returns_error() {
    // SAFETY: ensure the key is unset.
    unsafe { std::env::remove_var("TD_TEST_RESOLVER_MISSING") };

    let resolver = resolver();
    let result = resolver
        .resolve("${env:TD_TEST_RESOLVER_MISSING}")
        .await;
    assert!(result.is_err(), "expected missing env var to error");
}

#[tokio::test]
async fn optional_env_placeholder_falls_back_to_empty_string() {
    // SAFETY: ensure the key is unset.
    unsafe { std::env::remove_var("TD_TEST_RESOLVER_OPTIONAL") };

    let resolver = resolver();
    let resolved = resolver
        .resolve("prefix-${env:TD_TEST_RESOLVER_OPTIONAL?}-suffix")
        .await
        .expect("resolve");
    assert_eq!(resolved, "prefix--suffix");
}

#[tokio::test]
async fn multiple_env_placeholders_resolve_left_to_right() {
    // SAFETY: scoped env writes.
    unsafe {
        std::env::set_var("TD_TEST_RESOLVER_A", "alpha");
        std::env::set_var("TD_TEST_RESOLVER_B", "beta");
    }

    let resolver = resolver();
    let resolved = resolver
        .resolve("${env:TD_TEST_RESOLVER_A}/${env:TD_TEST_RESOLVER_B}")
        .await
        .expect("resolve");
    assert_eq!(resolved, "alpha/beta");

    unsafe {
        std::env::remove_var("TD_TEST_RESOLVER_A");
        std::env::remove_var("TD_TEST_RESOLVER_B");
    }
}

#[tokio::test]
async fn resolves_system_repository_url() {
    let resolver = resolver_with_system(SystemValues {
        repository_url: Some("file:///var/lib/tabsdata/instance/repo".to_string()),
    });
    let resolved = resolver
        .resolve("uri: ${sys:TDK_REPOSITORY_URL}/database/tabsdata.db")
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
    });
    let result = resolver.resolve("${sys:TDK_REPOSITORY_URL}").await;
    assert!(result.is_err(), "missing system value should error");
}

#[tokio::test]
async fn unknown_system_key_returns_error() {
    let resolver = resolver();
    let result = resolver.resolve("${sys:UNKNOWN_KEY}").await;
    assert!(result.is_err(), "unknown system key should error");
}

#[tokio::test]
async fn mixed_env_and_system_resolve_together() {
    // SAFETY: scoped env write.
    unsafe { std::env::set_var("TD_TEST_RESOLVER_MIXED", "marker") };

    let resolver = resolver_with_system(SystemValues {
        repository_url: Some("file:///root".to_string()),
    });
    let resolved = resolver
        .resolve("e=${env:TD_TEST_RESOLVER_MIXED} s=${sys:TDK_REPOSITORY_URL}")
        .await
        .expect("resolve");
    assert_eq!(resolved, "e=marker s=file:///root");

    unsafe { std::env::remove_var("TD_TEST_RESOLVER_MIXED") };
}
