//
// Copyright 2026 Tabsdata Inc.
//

use std::io::Write;

use tempfile::NamedTempFile;

use super::*;

fn write_yaml(text: &str) -> NamedTempFile {
    let mut file = NamedTempFile::new().expect("tempfile");
    file.write_all(text.as_bytes()).expect("write");
    file
}

#[test]
fn parses_name_data_and_labels() {
    let file = write_yaml(
        r#"apiVersion: v1
kind: Secret
metadata:
  name: td-app-apiserver-secret-config-apiserver.yaml
  labels:
    app.kubernetes.io/part-of: tabsdata
    tabsdata.com/secret-family: config
    custom: keep-me
type: Opaque
stringData:
  DATABASE__POSTGRES__PASSWORD: pgpw
  AUTH__JWT_SIGNING_KEY: jwtkey
"#,
    );
    let parsed = parse_secret_manifest(file.path()).expect("parses");
    assert_eq!(parsed.name, "td-app-apiserver-secret-config-apiserver.yaml");
    assert_eq!(parsed.data.len(), 2);
    assert_eq!(
        parsed
            .data
            .get("DATABASE__POSTGRES__PASSWORD")
            .map(String::as_str),
        Some("pgpw")
    );
    assert_eq!(
        parsed.data.get("AUTH__JWT_SIGNING_KEY").map(String::as_str),
        Some("jwtkey")
    );
    assert_eq!(
        parsed.labels.get("custom").map(String::as_str),
        Some("keep-me")
    );
}

#[test]
fn merges_data_and_string_data() {
    let file = write_yaml(
        r#"apiVersion: v1
kind: Secret
metadata:
  name: example
type: Opaque
data:
  ENCODED: cGd3
stringData:
  PLAIN: pgw
"#,
    );
    let parsed = parse_secret_manifest(file.path()).expect("parses");
    assert_eq!(parsed.data.get("ENCODED").map(String::as_str), Some("pgw"));
    assert_eq!(parsed.data.get("PLAIN").map(String::as_str), Some("pgw"));
}

#[test]
fn string_data_wins_over_data_on_collision() {
    let file = write_yaml(
        r#"apiVersion: v1
kind: Secret
metadata:
  name: example
type: Opaque
data:
  KEY: ZnJvbV9kYXRh
stringData:
  KEY: from_string_data
"#,
    );
    let parsed = parse_secret_manifest(file.path()).expect("parses");
    assert_eq!(
        parsed.data.get("KEY").map(String::as_str),
        Some("from_string_data")
    );
}

#[test]
fn missing_name_is_an_error() {
    let file = write_yaml(
        r#"apiVersion: v1
kind: Secret
metadata: {}
type: Opaque
stringData:
  K: v
"#,
    );
    let error = parse_secret_manifest(file.path()).expect_err("rejected");
    assert!(matches!(error, SecretError::ManifestMissingName { .. }));
}

#[test]
fn empty_name_is_an_error() {
    let file = write_yaml(
        r#"apiVersion: v1
kind: Secret
metadata:
  name: ""
type: Opaque
"#,
    );
    let error = parse_secret_manifest(file.path()).expect_err("rejected");
    assert!(matches!(error, SecretError::ManifestMissingName { .. }));
}

#[test]
fn malformed_yaml_is_an_error() {
    let file = write_yaml("not: : valid: yaml");
    let error = parse_secret_manifest(file.path()).expect_err("rejected");
    assert!(matches!(error, SecretError::ManifestParseFailed { .. }));
}

#[test]
fn non_utf8_binary_data_is_an_error() {
    let file = write_yaml(
        r#"apiVersion: v1
kind: Secret
metadata:
  name: example
type: Opaque
data:
  BINARY: gA==
"#,
    );
    let error = parse_secret_manifest(file.path()).expect_err("rejected");
    assert!(matches!(error, SecretError::ManifestParseFailed { .. }));
}

#[test]
fn duplicate_stringdata_keys_are_an_error() {
    let file = write_yaml(
        r#"apiVersion: v1
kind: Secret
metadata:
  name: example
type: Opaque
stringData:
  K: a
  K: b
"#,
    );
    let error = parse_secret_manifest(file.path()).expect_err("rejected");
    assert!(matches!(error, SecretError::ManifestParseFailed { .. }));
}

#[test]
fn duplicate_data_keys_are_an_error() {
    let file = write_yaml(
        r#"apiVersion: v1
kind: Secret
metadata:
  name: example
type: Opaque
data:
  K: YQ==
  K: Yg==
"#,
    );
    let error = parse_secret_manifest(file.path()).expect_err("rejected");
    assert!(matches!(error, SecretError::ManifestParseFailed { .. }));
}

#[test]
fn duplicate_label_keys_are_an_error() {
    let file = write_yaml(
        r#"apiVersion: v1
kind: Secret
metadata:
  name: example
  labels:
    same: a
    same: b
type: Opaque
"#,
    );
    let error = parse_secret_manifest(file.path()).expect_err("rejected");
    assert!(matches!(error, SecretError::ManifestParseFailed { .. }));
}

#[test]
fn no_labels_returns_empty_map() {
    let file = write_yaml(
        r#"apiVersion: v1
kind: Secret
metadata:
  name: example
type: Opaque
stringData:
  K: v
"#,
    );
    let parsed = parse_secret_manifest(file.path()).expect("parses");
    assert!(parsed.labels.is_empty());
}
