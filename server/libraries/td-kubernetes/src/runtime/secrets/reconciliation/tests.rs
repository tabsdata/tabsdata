//
// Copyright 2026 Tabs Data Inc.
//

use super::*;

fn keys(items: &[&str]) -> BTreeSet<String> {
    items.iter().map(|item| (*item).to_owned()).collect()
}

#[test]
fn no_leaves_and_no_secret_is_ok() {
    let input = Reconciliation {
        config: "noseed.yaml",
        config_yaml: "name: noseed\nhost: example.com\n",
        secret: "tabsdata-secret-config-noseed.yaml",
        actual_keys: None,
    };
    reconcile_secret_keys(&input).expect("ok when no leaves and no secret");
}

#[test]
fn no_leaves_but_secret_exists_fails() {
    let input = Reconciliation {
        config: "noseed.yaml",
        config_yaml: "name: noseed\nhost: example.com\n",
        secret: "tabsdata-secret-config-noseed.yaml",
        actual_keys: Some(keys(&["STRAY"])),
    };
    let error = reconcile_secret_keys(&input).expect_err("unexpected secret");
    assert!(matches!(
        error,
        SecretError::ReconciliationSecretUnexpected { .. }
    ));
}

#[test]
fn leaves_and_no_secret_fails() {
    let input = Reconciliation {
        config: "apiserver.yaml",
        config_yaml: "database:\n  postgres:\n    password: ${secret}\n",
        secret: "tabsdata-secret-config-apiserver.yaml",
        actual_keys: None,
    };
    let error = reconcile_secret_keys(&input).expect_err("missing secret");
    assert!(matches!(
        error,
        SecretError::ReconciliationSecretMissing { .. }
    ));
}

#[test]
fn matching_keys_pass() {
    let input = Reconciliation {
        config: "apiserver.yaml",
        config_yaml: "database:\n  postgres:\n    password: ${secret}\n",
        secret: "tabsdata-secret-config-apiserver.yaml",
        actual_keys: Some(keys(&["DATABASE__POSTGRES__PASSWORD"])),
    };
    reconcile_secret_keys(&input).expect("keys match");
}

#[test]
fn extra_keys_fail() {
    let input = Reconciliation {
        config: "apiserver.yaml",
        config_yaml: "database:\n  postgres:\n    password: ${secret}\n",
        secret: "tabsdata-secret-config-apiserver.yaml",
        actual_keys: Some(keys(&["DATABASE__POSTGRES__PASSWORD", "EXTRA"])),
    };
    let error = reconcile_secret_keys(&input).expect_err("mismatch");
    match error {
        SecretError::ReconciliationKeysMismatch { extra, missing, .. } => {
            assert!(extra.contains("EXTRA"));
            assert_eq!(missing.trim(), "none.");
        }
        other => panic!("unexpected variant: {other:?}"),
    }
}

#[test]
fn missing_keys_fail() {
    let input = Reconciliation {
        config: "apiserver.yaml",
        config_yaml: "database:\n  postgres:\n    password: ${secret}\n  user: ${secret}\n",
        secret: "tabsdata-secret-config-apiserver.yaml",
        actual_keys: Some(keys(&["DATABASE__POSTGRES__PASSWORD"])),
    };
    let error = reconcile_secret_keys(&input).expect_err("mismatch");
    match error {
        SecretError::ReconciliationKeysMismatch { extra, missing, .. } => {
            assert_eq!(extra.trim(), "none.");
            assert!(missing.contains("DATABASE__USER"));
        }
        other => panic!("unexpected variant: {other:?}"),
    }
}

#[test]
fn malformed_config_yaml_fails() {
    let input = Reconciliation {
        config: "broken.yaml",
        config_yaml: "not: : valid: yaml",
        secret: "tabsdata-secret-config-broken.yaml",
        actual_keys: None,
    };
    let error = reconcile_secret_keys(&input).expect_err("parse fail");
    assert!(matches!(error, SecretError::ConfigYamlParseFailed { .. }));
}
