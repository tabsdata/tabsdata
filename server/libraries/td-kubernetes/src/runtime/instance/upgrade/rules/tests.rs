//
// Copyright 2026 Tabsdata Inc.
//

use std::fs;

use indoc::indoc;

use super::*;
use crate::runtime::instance::upgrade::inventory::{INVENTORY_SCHEMA, InventoryEntry};

const FIRST_AND_SECOND: &str = indoc! {"
    schema: 1
    rules:
      - first
      - second
"};

const UNSUPPORTED_SCHEMA: &str = indoc! {"
    schema: 99
    rules: []
"};

const UNKNOWN_FIELD: &str = indoc! {"
    schema: 1
    rules: []
    other: 1
"};

const OLD: &str = indoc! {"
    schema: 1
    rules:
      - old
"};

const MIDDLE: &str = indoc! {"
    schema: 1
    rules:
      - middle
"};

const NEW_AND_MIDDLE: &str = indoc! {"
    schema: 1
    rules:
      - new
      - middle
"};

const NEW: &str = indoc! {"
    schema: 1
    rules:
      - new
"};

const NO_RULES: &str = indoc! {"
    schema: 1
    rules: []
"};

const ENSURE_PROJECT_SECRET: &str = indoc! {"
    schema: 1
    rules:
      - ensure-project-secret
"};

const SCHEMA_TWO: &str = indoc! {"
    schema: 2
    rules: []
"};

const MISSPELLED_FIELD: &str = indoc! {"
    schema: 1
    rule: []
"};

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
    let rules = deserialize_rules("2.3.0", FIRST_AND_SECOND).expect("deserializes");
    assert_eq!(rules.rules, ["first", "second"]);
}

#[test]
fn an_unsupported_schema_is_rejected() {
    let outcome = deserialize_rules("2.3.0", UNSUPPORTED_SCHEMA);
    assert!(matches!(
        outcome,
        Err(UpgradeError::RulesSchemaUnsupported { .. })
    ));
}

#[test]
fn an_unknown_field_is_rejected() {
    let outcome = deserialize_rules("2.3.0", UNKNOWN_FIELD);
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
    let root = lay_out(&[("2.1.0", OLD), ("2.2.0", MIDDLE), ("2.3.0", NEW_AND_MIDDLE)]);
    let inventory = inventory(&["2.3.0", "2.2.0", "2.1.0", "2.0.0"]);
    let hops = [hop("2.1.0", "2.2.0"), hop("2.2.0", "2.3.0")];
    let rules = rules_to_apply(root.path(), &inventory, &hops, "2.3.0", false).expect("collects");
    assert_eq!(rules, ["middle", "new"]);
}

#[test]
fn an_interrupted_upgrade_applies_the_rules_of_every_version_up_to_its_target() {
    let root = lay_out(&[("2.1.0", OLD), ("2.2.0", MIDDLE), ("2.3.0", NEW)]);
    let inventory = inventory(&["2.3.0", "2.2.0", "2.1.0", "2.0.0"]);
    let rules = rules_to_apply(root.path(), &inventory, &[], "2.3.0", true).expect("collects");
    assert_eq!(rules, ["old", "middle", "new"]);
}

#[test]
fn every_upgrader_version_with_rules_is_accepted() {
    let root = lay_out(&[("2.3.0", NO_RULES), ("2.2.1", NO_RULES)]);
    ensure_rules(root.path(), &inventory(&["2.3.0", "2.2.1", "2.0.0"])).expect("accepted");
}

#[test]
fn an_upgrader_version_without_rules_is_refused() {
    let root = lay_out(&[("2.3.0", NO_RULES)]);
    let outcome = ensure_rules(root.path(), &inventory(&["2.3.0", "2.2.1", "2.0.0"]));
    assert!(outcome.is_err(), "missing rules must be rejected");
}

#[test]
fn rules_for_the_floor_version_are_refused() {
    let root = lay_out(&[("2.3.0", NO_RULES), ("2.0.0", NO_RULES)]);
    let outcome = ensure_rules(root.path(), &inventory(&["2.3.0", "2.0.0"]));
    assert!(outcome.is_err(), "rules for the floor must be rejected");
}

#[test]
fn rules_for_a_version_outside_the_inventory_are_an_orphan() {
    let root = lay_out(&[("2.3.0", NO_RULES), ("2.9.0", NO_RULES)]);
    let outcome = ensure_rules(root.path(), &inventory(&["2.3.0", "2.0.0"]));
    assert!(outcome.is_err(), "orphan rules must be rejected");
}

#[test]
fn verifying_a_sound_tree_finds_no_problem() {
    let root = lay_out(&[("2.3.0", ENSURE_PROJECT_SECRET), ("2.2.1", NO_RULES)]);
    let problems = verify_rules(root.path(), &inventory(&["2.3.0", "2.2.1", "2.0.0"]));
    assert!(problems.is_empty(), "{problems:?}");
}

#[test]
fn a_micro_version_with_rules_is_a_problem() {
    let root = lay_out(&[("2.3.0", NO_RULES), ("2.2.1", ENSURE_PROJECT_SECRET)]);
    let problems = verify_rules(root.path(), &inventory(&["2.3.0", "2.2.1", "2.0.0"]));
    assert_eq!(problems.len(), 1, "{problems:?}");
    assert!(problems[0].contains("2.2.1"), "{problems:?}");
    assert!(
        problems[0].contains("ensure-project-secret"),
        "{problems:?}"
    );
}

#[test]
fn a_malformed_rules_file_is_a_problem() {
    let root = lay_out(&[("2.3.0", SCHEMA_TWO), ("2.2.1", MISSPELLED_FIELD)]);
    let problems = verify_rules(root.path(), &inventory(&["2.3.0", "2.2.1", "2.0.0"]));
    assert_eq!(problems.len(), 2, "{problems:?}");
}

#[test]
fn verifying_reports_the_file_problems_together_with_the_content_ones() {
    let root = lay_out(&[("2.2.1", ENSURE_PROJECT_SECRET)]);
    let problems = verify_rules(root.path(), &inventory(&["2.3.0", "2.2.1", "2.0.0"]));
    assert_eq!(problems.len(), 2, "{problems:?}");
    assert!(
        problems.iter().any(|problem| problem.contains("2.3.0")),
        "{problems:?}"
    );
}
