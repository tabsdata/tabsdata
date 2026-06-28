//
// Copyright 2026 Tabs Data Inc.
//

use super::*;
use serde_json::json;

fn kv(key: &str, value: Option<&str>) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: value.map(str::to_string),
    }
}

#[test]
fn entries_to_map_keeps_valued_entries_only() {
    assert!(entries_to_map(None).is_empty());
    let entries = vec![kv("A", Some("1")), kv("B", None), kv("C", Some("3"))];
    let map = entries_to_map(Some(&entries));
    assert_eq!(map.len(), 2);
    assert_eq!(map.get("A").map(String::as_str), Some("1"));
    assert_eq!(map.get("C").map(String::as_str), Some("3"));
    assert!(!map.contains_key("B"));
}

#[test]
fn decode_secret_entries_decodes_utf8_values() {
    let data = BTreeMap::from([
        ("password".to_string(), b"hunter2".to_vec()),
        ("user".to_string(), b"etl".to_vec()),
    ]);
    let entries = decode_secret_entries(data).unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].key, "password");
    assert_eq!(entries[0].value.as_deref(), Some("hunter2"));
    assert_eq!(entries[1].key, "user");
    assert_eq!(entries[1].value.as_deref(), Some("etl"));
}

#[test]
fn decode_secret_entries_rejects_non_utf8() {
    let data = BTreeMap::from([("blob".to_string(), vec![0xff, 0xfe])]);
    let error = decode_secret_entries(data).unwrap_err();
    assert!(matches!(
        error.domain_err::<KubeConnectionError>(),
        KubeConnectionError::SecretNotUtf8 { .. }
    ));
}

#[test]
fn parse_document_reads_the_stored_yaml() {
    let document = json!({"kind": "mysqlSrcConn", "spec": {"uri": "mysql://h/db"}});
    let data = BTreeMap::from([(
        CONNECTION_CONFIGMAP_KEY.to_string(),
        serde_yaml::to_string(&document).unwrap(),
    )]);
    assert_eq!(parse_document(&data).unwrap(), document);
}

#[test]
fn parse_document_missing_entry_errors() {
    let data = BTreeMap::from([("other.yaml".to_string(), "kind: x".to_string())]);
    let error = parse_document(&data).unwrap_err();
    assert!(matches!(
        error.domain_err::<KubeConnectionError>(),
        KubeConnectionError::DocumentKeyMissing { .. }
    ));
}

#[test]
fn parse_document_corrupt_yaml_errors() {
    let data = BTreeMap::from([(CONNECTION_CONFIGMAP_KEY.to_string(), "[1, 2".to_string())]);
    let error = parse_document(&data).unwrap_err();
    assert!(matches!(
        error.domain_err::<KubeConnectionError>(),
        KubeConnectionError::DocumentCorrupt { .. }
    ));
}
