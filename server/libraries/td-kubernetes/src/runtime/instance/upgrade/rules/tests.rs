//
// Copyright 2026 Tabsdata Inc.
//

use std::fs;

use super::*;
use crate::runtime::instance::upgrade::inventory::{INVENTORY_SCHEMA, InventoryEntry};

fn inventory(versions: &[&str]) -> Inventory {
    Inventory {
        schema: INVENTORY_SCHEMA,
        versions: versions
            .iter()
            .enumerate()
            .map(|(position, version)| InventoryEntry {
                version: version.to_string(),
                upgrader: position + 1 < versions.len(),
            })
            .collect(),
    }
}

fn lay_out(rules: &[(&str, &str)]) -> tempfile::TempDir {
    let root = tempfile::tempdir().expect("temp dir");
    let folder = root.path().join(RULES_FOLDER);
    fs::create_dir_all(&folder).expect("rules folder");
    for (version, yaml) in rules {
        fs::write(folder.join(format!("{version}.yaml")), yaml).expect("rules");
    }
    root
}

fn hop(from: &str, to: &str) -> Hop {
    Hop {
        from: from.to_owned(),
        to: to.to_owned(),
        upgrader: true,
    }
}

#[test]
fn a_rules_file_lists_its_rules() {
    let rules = deserialize_rules("2.3.0", "schema: 1\nrules:\n  - first\n  - second\n")
        .expect("deserializes");
    assert_eq!(rules.rules, ["first", "second"]);
}

#[test]
fn an_unsupported_schema_is_rejected() {
    let outcome = deserialize_rules("2.3.0", "schema: 99\nrules: []\n");
    assert!(matches!(
        outcome,
        Err(UpgradeError::RulesSchemaUnsupported { .. })
    ));
}

#[test]
fn an_unknown_field_is_rejected() {
    let outcome = deserialize_rules("2.3.0", "schema: 1\nrules: []\nother: 1\n");
    assert!(matches!(
        outcome,
        Err(UpgradeError::RulesParseFailed { .. })
    ));
}

#[test]
fn a_missing_rules_file_cannot_be_loaded() {
    let root = lay_out(&[]);
    assert!(
        load_rules(root.path(), "2.3.0").is_err(),
        "a missing rules file must be rejected"
    );
}

#[test]
fn an_upgrade_applies_the_rules_of_the_versions_its_hops_reach() {
    let root = lay_out(&[
        ("2.1.0", "schema: 1\nrules:\n  - old\n"),
        ("2.2.0", "schema: 1\nrules:\n  - middle\n"),
        ("2.3.0", "schema: 1\nrules:\n  - new\n  - middle\n"),
    ]);
    let inventory = inventory(&["2.3.0", "2.2.0", "2.1.0", "2.0.0"]);
    let hops = [hop("2.1.0", "2.2.0"), hop("2.2.0", "2.3.0")];
    let rules = rules_to_apply(root.path(), &inventory, &hops, "2.3.0", false).expect("collects");
    assert_eq!(rules, ["middle", "new"]);
}

#[test]
fn an_interrupted_upgrade_applies_the_rules_of_every_version_up_to_its_target() {
    let root = lay_out(&[
        ("2.1.0", "schema: 1\nrules:\n  - old\n"),
        ("2.2.0", "schema: 1\nrules:\n  - middle\n"),
        ("2.3.0", "schema: 1\nrules:\n  - new\n"),
    ]);
    let inventory = inventory(&["2.3.0", "2.2.0", "2.1.0", "2.0.0"]);
    let rules = rules_to_apply(root.path(), &inventory, &[], "2.3.0", true).expect("collects");
    assert_eq!(rules, ["old", "middle", "new"]);
}

#[test]
fn every_upgrader_version_with_rules_is_accepted() {
    let root = lay_out(&[
        ("2.3.0", "schema: 1\nrules: []\n"),
        ("2.2.1", "schema: 1\nrules: []\n"),
    ]);
    ensure_rules(root.path(), &inventory(&["2.3.0", "2.2.1", "2.0.0"])).expect("accepted");
}

#[test]
fn an_upgrader_version_without_rules_is_refused() {
    let root = lay_out(&[("2.3.0", "schema: 1\nrules: []\n")]);
    let outcome = ensure_rules(root.path(), &inventory(&["2.3.0", "2.2.1", "2.0.0"]));
    assert!(outcome.is_err(), "missing rules must be rejected");
}

#[test]
fn rules_for_the_floor_version_are_refused() {
    let root = lay_out(&[
        ("2.3.0", "schema: 1\nrules: []\n"),
        ("2.0.0", "schema: 1\nrules: []\n"),
    ]);
    let outcome = ensure_rules(root.path(), &inventory(&["2.3.0", "2.0.0"]));
    assert!(outcome.is_err(), "rules for the floor must be rejected");
}

#[test]
fn rules_for_a_version_outside_the_inventory_are_an_orphan() {
    let root = lay_out(&[
        ("2.3.0", "schema: 1\nrules: []\n"),
        ("2.9.0", "schema: 1\nrules: []\n"),
    ]);
    let outcome = ensure_rules(root.path(), &inventory(&["2.3.0", "2.0.0"]));
    assert!(outcome.is_err(), "orphan rules must be rejected");
}
