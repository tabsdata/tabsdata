//
// Copyright 2026 Tabs Data Inc.
//

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

fn write_yaml(dir: &TempDir, file_name: &str, name: &str, port: u16) -> std::path::PathBuf {
    let path = dir.path().join(file_name);
    fs::write(&path, format!("name: {name}\nport: {port}\n")).expect("write yaml");
    path
}

#[test]
fn explicit_config_file_is_applied() {
    let dir = TempDir::new().expect("temp dir");
    let custom = write_yaml(&dir, "custom.yaml", "from_custom", 7);
    let config = load_config::<Sample>("td-1912", None, Some(custom), false);
    assert_eq!(config.name, "from_custom");
    assert_eq!(config.port, 7);
}

#[test]
fn explicit_config_file_overrides_default_layers() {
    let dir = TempDir::new().expect("temp dir");
    let custom = write_yaml(&dir, "custom.yaml", "from_custom", 99);

    let config = load_config::<Sample>("td-1912", None, Some(custom), false);
    assert_eq!(config.port, 99, "explicit file should win over defaults");
}

#[test]
fn missing_config_folder_is_ignored_when_explicit_file_supplied() {
    let dir = TempDir::new().expect("temp dir");
    let custom = write_yaml(&dir, "custom.yaml", "supplied_only", 12);
    let nonexistent_folder = dir.path().join("does-not-exist");

    let config = load_config::<Sample>(
        "td-1912",
        Some(nonexistent_folder),
        Some(custom),
        false,
    );
    assert_eq!(config.name, "supplied_only");
    assert_eq!(config.port, 12);
}

#[test]
fn explicit_config_file_with_partial_yaml_falls_back_to_defaults() {
    let dir = TempDir::new().expect("temp dir");
    let partial = dir.path().join("partial.yaml");
    fs::write(&partial, "port: 42\n").expect("write partial");

    let config = load_config::<Sample>("td-1912", None, Some(partial), false);
    assert_eq!(config.port, 42, "partial value should be honoured");
    assert_eq!(
        config.name, "default_name",
        "missing keys should fall back to default"
    );
}
