//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::BTreeMap;
use std::fs;

use serde::{Deserialize, Serialize};
use td_process::launcher::config::{Config, load_config};
use tempfile::TempDir;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Sample {
    name: String,
    port: u16,
}

impl Default for Sample {
    fn default() -> Self {
        Self {
            name: "default_name".to_string(),
            port: 1,
        }
    }
}

impl Config for Sample {}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Nested {
    #[serde(default)]
    server: Server,
    #[serde(default)]
    labels: BTreeMap<String, String>,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    workers: Vec<Worker>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Server {
    #[serde(default)]
    host: String,
    #[serde(default)]
    port: u16,
    #[serde(default)]
    tls: Tls,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Tls {
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    cert: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Worker {
    #[serde(default)]
    name: String,
    #[serde(default)]
    program: String,
}

impl Config for Nested {}

fn write_yaml(dir: &TempDir, file_name: &str, name: &str, port: u16) -> std::path::PathBuf {
    let path = dir.path().join(file_name);
    fs::write(&path, format!("name: {name}\nport: {port}\n")).expect("write yaml");
    path
}

#[test]
fn explicit_config_file_is_applied() {
    let dir = TempDir::new().expect("temp dir");
    let custom = write_yaml(&dir, "custom.yaml", "from_custom", 7);
    let config = load_config::<Sample>("td-1912", None, vec![custom], false);
    assert_eq!(config.name, "from_custom");
    assert_eq!(config.port, 7);
}

#[test]
fn explicit_config_file_overrides_default_layers() {
    let dir = TempDir::new().expect("temp dir");
    let custom = write_yaml(&dir, "custom.yaml", "from_custom", 99);

    let config = load_config::<Sample>("td-1912", None, vec![custom], false);
    assert_eq!(
        config.port, 99,
        "The explicit file should win over the defaults."
    );
}

#[test]
fn missing_config_folder_is_ignored_when_explicit_file_supplied() {
    let dir = TempDir::new().expect("temp dir");
    let custom = write_yaml(&dir, "custom.yaml", "supplied_only", 12);
    let nonexistent_folder = dir.path().join("does-not-exist");

    let config = load_config::<Sample>("td-1912", Some(nonexistent_folder), vec![custom], false);
    assert_eq!(config.name, "supplied_only");
    assert_eq!(config.port, 12);
}

#[test]
fn explicit_config_file_with_partial_yaml_falls_back_to_defaults() {
    let dir = TempDir::new().expect("temp dir");
    let partial = dir.path().join("partial.yaml");
    fs::write(&partial, "port: 42\n").expect("write partial");

    let config = load_config::<Sample>("td-1912", None, vec![partial], false);
    assert_eq!(
        config.port, 42,
        "The value present in the partial file should be honoured."
    );
    assert_eq!(
        config.name, "default_name",
        "A key missing from the partial file should fall back to the default."
    );
}

#[test]
fn multiple_explicit_config_files_merge_additively() {
    let dir = TempDir::new().expect("temp dir");
    let first = write_yaml(&dir, "first.yaml", "from_first", 100);
    let second = dir.path().join("second.yaml");
    fs::write(&second, "name: from_second\n").expect("write second");

    let config = load_config::<Sample>("td-1912", None, vec![first, second], false);
    assert_eq!(
        config.name, "from_second",
        "The later file should override the field it sets."
    );
    assert_eq!(
        config.port, 100,
        "A field omitted by the later file should be preserved from the earlier file."
    );
}

#[test]
fn nested_merge_is_deep_for_maps_and_wholesale_for_lists() {
    let dir = TempDir::new().expect("temp dir");

    let base_path = dir.path().join("base.yaml");
    fs::write(
        &base_path,
        r#"
server:
  host: base-host
  port: 5432
  tls:
    enabled: false
    cert: base-cert
labels:
  team: data
  tier: backend
tags:
  - alpha
  - beta
workers:
  - name: bootloader
    program: td_bootloader
  - name: apiserver
    program: td_apiserver
"#,
    )
    .expect("write base");

    let overlay_path = dir.path().join("overlay.yaml");
    fs::write(
        &overlay_path,
        r#"
server:
  port: 6543
  tls:
    enabled: true
labels:
  tier: edge
  region: eu
tags:
  - gamma
workers:
  - name: janitor
    program: td_janitor
"#,
    )
    .expect("write overlay");

    let config =
        load_config::<Nested>("td-1912-nested", None, vec![base_path, overlay_path], false);

    assert_eq!(
        config.server.host, "base-host",
        "A nested scalar the overlay does not set should be kept from the base file."
    );
    assert_eq!(
        config.server.port, 6543,
        "A nested scalar the overlay sets should be overridden."
    );
    assert!(
        config.server.tls.enabled,
        "A leaf the overlay sets two levels deep should be overridden."
    );
    assert_eq!(
        config.server.tls.cert, "base-cert",
        "A leaf the overlay does not set two levels deep should be kept from the base file."
    );

    assert_eq!(
        config.labels.get("team").map(String::as_str),
        Some("data"),
        "A map key the overlay does not set should be kept from the base file."
    );
    assert_eq!(
        config.labels.get("tier").map(String::as_str),
        Some("edge"),
        "A map key the overlay sets should be overridden."
    );
    assert_eq!(
        config.labels.get("region").map(String::as_str),
        Some("eu"),
        "A map key only the overlay defines should be added."
    );

    assert_eq!(
        config.tags,
        vec!["gamma".to_string()],
        "A list of scalars should be replaced wholesale, not merged element by element."
    );
    assert_eq!(
        config.workers.len(),
        1,
        "A list of maps should be replaced wholesale, not merged element by element."
    );
    assert_eq!(config.workers[0].name, "janitor");
    assert_eq!(config.workers[0].program, "td_janitor");
}
