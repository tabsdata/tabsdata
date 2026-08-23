//
// Copyright 2026 Tabsdata Inc.
//

use std::fs;

use super::*;

fn lay_out(entries: &[(&str, bool)], specs: &[&str]) -> tempfile::TempDir {
    let root = tempfile::tempdir().expect("temp dir");
    let listed = entries
        .iter()
        .map(|(version, upgrader)| format!("  - version: {version}\n    upgrader: {upgrader}\n"))
        .collect::<String>();
    fs::write(
        root.path().join(INVENTORY_FILE),
        format!("schema: 1\nversions:\n{listed}"),
    )
    .expect("inventory");
    let folder = root.path().join(UPGRADES_FOLDER);
    fs::create_dir_all(&folder).expect("specs folder");
    for spec in specs {
        fs::write(folder.join(format!("{spec}.yaml")), "schema: 1\n").expect("spec");
    }
    root
}

#[test]
fn a_matching_inventory_and_spec_set_loads() {
    let root = lay_out(&[("2.0.1", true), ("2.0.0", false)], &["2.0.1"]);
    let inventory = load_inventory(root.path()).expect("loads");
    assert_eq!(inventory.floor(), "2.0.0");
}

#[test]
fn an_inventory_with_no_upgraders_needs_no_specs() {
    let root = lay_out(&[("2.0.1", false), ("2.0.0", false)], &[]);
    assert!(load_inventory(root.path()).is_ok());
}

#[test]
fn a_declared_upgrader_without_a_spec_is_rejected() {
    let root = lay_out(&[("2.0.1", true), ("2.0.0", false)], &[]);
    let outcome = load_inventory(root.path());
    assert!(outcome.is_err(), "a missing spec must be rejected");
}

#[test]
fn a_spec_without_a_declared_upgrader_is_rejected() {
    let root = lay_out(&[("2.0.1", false), ("2.0.0", false)], &["2.0.1"]);
    let outcome = load_inventory(root.path());
    assert!(outcome.is_err(), "an undeclared spec must be rejected");
}

#[test]
fn a_spec_for_an_unlisted_version_is_rejected() {
    let root = lay_out(&[("2.0.1", true), ("2.0.0", false)], &["2.0.1", "2.0.9"]);
    let outcome = load_inventory(root.path());
    assert!(outcome.is_err(), "an orphan spec must be rejected");
}

#[test]
fn a_missing_inventory_is_rejected() {
    let root = tempfile::tempdir().expect("temp dir");
    let outcome = load_inventory(root.path());
    assert!(outcome.is_err(), "a missing inventory must be rejected");
}

#[test]
fn the_shipped_catalog_loads() {
    let root = PathBuf::from(format!(
        "{}/../../../variant/resources/kupgrade",
        env!("CARGO_MANIFEST_DIR")
    ));
    let inventory = load_inventory(&root).expect("the shipped catalog must load");
    assert_eq!(inventory.floor(), "2.0.0");
    for entry in &inventory.versions {
        if entry.upgrader {
            load_spec(&root, &entry.version)
                .unwrap_or_else(|error| panic!("{} must load: {error:?}", entry.version));
        }
    }
}

#[test]
fn a_create_without_content_is_rejected() {
    let root = lay_out(&[("2.0.1", true), ("2.0.0", false)], &["2.0.1"]);
    std::fs::write(
        root.path().join(UPGRADES_FOLDER).join("2.0.1.yaml"),
        "schema: 1\nconfigs:\n  a.yaml:\n    a.yaml:\n      action: create\n",
    )
    .expect("spec");
    assert!(load_spec(root.path(), "2.0.1").is_err());
}

#[test]
fn an_unclaimed_content_file_is_rejected() {
    let root = lay_out(&[("2.0.1", true), ("2.0.0", false)], &["2.0.1"]);
    std::fs::write(
        root.path().join(UPGRADES_FOLDER).join("2.0.1.yaml"),
        "schema: 1\nconfigs:\n  a.yaml:\n    a.yaml:\n      action: retain\n",
    )
    .expect("spec");
    let configs = root
        .path()
        .join(UPGRADES_FOLDER)
        .join("2.0.1")
        .join("configs");
    std::fs::create_dir_all(&configs).expect("folder");
    std::fs::write(configs.join("a.yaml"), "x: 1\n").expect("content");
    assert!(load_spec(root.path(), "2.0.1").is_err());
}

#[test]
fn a_create_with_content_loads() {
    let root = lay_out(&[("2.0.1", true), ("2.0.0", false)], &["2.0.1"]);
    std::fs::write(
        root.path().join(UPGRADES_FOLDER).join("2.0.1.yaml"),
        "schema: 1\nconfigs:\n  a.yaml:\n    a.yaml:\n      action: create\n",
    )
    .expect("spec");
    let configs = root
        .path()
        .join(UPGRADES_FOLDER)
        .join("2.0.1")
        .join("configs");
    std::fs::create_dir_all(&configs).expect("folder");
    std::fs::write(configs.join("a.yaml"), "x: 1\n").expect("content");
    assert!(load_spec(root.path(), "2.0.1").is_ok());
    assert_eq!(
        read_content(root.path(), "2.0.1", "configs", "a.yaml").expect("content"),
        "x: 1\n"
    );
}

#[test]
fn the_sibling_ee_catalog_loads() {
    let root = PathBuf::from(format!(
        "{}/../../../../tabsdata-ee/variant/resources/kupgrade",
        env!("CARGO_MANIFEST_DIR")
    ));
    if !root.is_dir() {
        return;
    }
    let inventory = load_inventory(&root).expect("the ee catalog must load");
    for entry in &inventory.versions {
        if entry.upgrader {
            load_spec(&root, &entry.version)
                .unwrap_or_else(|error| panic!("ee {} must load: {error:?}", entry.version));
        }
    }
}
