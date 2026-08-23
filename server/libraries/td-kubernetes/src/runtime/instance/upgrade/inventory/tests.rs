//
// Copyright 2026 Tabsdata Inc.
//

use super::*;

fn inventory(entries: &[(&str, bool)]) -> Inventory {
    Inventory {
        schema: INVENTORY_SCHEMA,
        versions: entries
            .iter()
            .map(|(version, upgrader)| InventoryEntry {
                version: (*version).to_owned(),
                upgrader: *upgrader,
            })
            .collect(),
    }
}

fn sample() -> Inventory {
    inventory(&[
        ("2.0.5", true),
        ("2.0.4", false),
        ("2.0.2", false),
        ("2.0.1", true),
        ("2.0.0", false),
    ])
}

#[test]
fn the_floor_is_the_last_entry_and_the_newest_is_the_first() {
    let inventory = sample();
    assert_eq!(inventory.floor(), "2.0.0");
    assert_eq!(inventory.newest(), "2.0.5");
}

#[test]
fn hops_walk_oldest_first_and_carry_the_upgrader_flag() {
    let hops = sample().hops("2.0.0", "2.0.5").expect("hops");
    let walked: Vec<_> = hops
        .iter()
        .map(|hop| (hop.from.as_str(), hop.to.as_str(), hop.upgrader))
        .collect();
    assert_eq!(
        walked,
        vec![
            ("2.0.0", "2.0.1", true),
            ("2.0.1", "2.0.2", false),
            ("2.0.2", "2.0.4", false),
            ("2.0.4", "2.0.5", true),
        ]
    );
}

#[test]
fn a_partial_walk_covers_only_the_requested_span() {
    let hops = sample().hops("2.0.2", "2.0.5").expect("hops");
    assert_eq!(hops.len(), 2);
    assert_eq!(hops[0].from, "2.0.2");
    assert_eq!(hops[1].to, "2.0.5");
}

#[test]
fn an_up_to_date_instance_walks_no_hops() {
    assert!(sample().hops("2.0.5", "2.0.5").expect("hops").is_empty());
}

#[test]
fn an_older_product_than_instance_is_rejected() {
    let outcome = sample().hops("2.0.5", "2.0.1");
    assert!(matches!(
        outcome,
        Err(UpgradeError::ProductOlderThanInstance { .. })
    ));
}

#[test]
fn a_version_below_the_floor_is_rejected() {
    let outcome = sample().hops("1.9.0", "2.0.5");
    assert!(matches!(
        outcome,
        Err(UpgradeError::VersionBelowFloor { .. })
    ));
}

#[test]
fn a_version_newer_than_the_inventory_is_unknown() {
    let outcome = sample().hops("2.0.0", "2.9.9");
    assert!(matches!(
        outcome,
        Err(UpgradeError::VersionNotInInventory { .. })
    ));
}

#[test]
fn an_unlisted_version_inside_the_range_is_unknown() {
    let outcome = sample().hops("2.0.3", "2.0.5");
    assert!(matches!(
        outcome,
        Err(UpgradeError::VersionNotInInventory { .. })
    ));
}

#[test]
fn a_descending_inventory_validates() {
    let yaml = serde_yaml::to_string(&sample()).expect("renders");
    assert!(deserialize_inventory(&yaml).is_ok());
}

#[test]
fn an_ascending_inventory_is_rejected() {
    let yaml =
        serde_yaml::to_string(&inventory(&[("2.0.0", false), ("2.0.1", true)])).expect("renders");
    let outcome = deserialize_inventory(&yaml);
    assert!(matches!(
        outcome,
        Err(UpgradeError::InventoryNotDescending { .. })
    ));
}

#[test]
fn a_duplicate_version_is_rejected() {
    let yaml =
        serde_yaml::to_string(&inventory(&[("2.0.1", true), ("2.0.1", false)])).expect("renders");
    let outcome = deserialize_inventory(&yaml);
    assert!(matches!(
        outcome,
        Err(UpgradeError::InventoryDuplicateVersion { .. })
    ));
}

#[test]
fn an_upgrader_on_the_floor_is_rejected() {
    let yaml =
        serde_yaml::to_string(&inventory(&[("2.0.1", true), ("2.0.0", true)])).expect("renders");
    let outcome = deserialize_inventory(&yaml);
    assert!(matches!(
        outcome,
        Err(UpgradeError::InventoryFloorHasUpgrader { .. })
    ));
}

#[test]
fn an_empty_inventory_is_rejected() {
    let yaml = serde_yaml::to_string(&inventory(&[])).expect("renders");
    assert!(matches!(
        deserialize_inventory(&yaml),
        Err(UpgradeError::InventoryEmpty)
    ));
}

#[test]
fn an_unsupported_schema_is_rejected() {
    let yaml = "schema: 99\nversions:\n  - version: 2.0.0\n    upgrader: false\n";
    assert!(matches!(
        deserialize_inventory(yaml),
        Err(UpgradeError::InventorySchemaUnsupported { .. })
    ));
}

#[test]
fn pep440_versions_order_as_python_does() {
    let inventory = inventory(&[
        ("2.0.1.1", false),
        ("2.0.1", true),
        ("2.0.1rc1", false),
        ("2.0.0", false),
    ]);
    let yaml = serde_yaml::to_string(&inventory).expect("renders");
    assert!(deserialize_inventory(&yaml).is_ok());

    let hops = inventory.hops("2.0.1rc1", "2.0.1.1").expect("hops");
    let walked: Vec<_> = hops.iter().map(|hop| hop.to.as_str()).collect();
    assert_eq!(walked, vec!["2.0.1", "2.0.1.1"]);
}

#[test]
fn the_shipped_inventory_is_valid() {
    let path = format!(
        "{}/../../../variant/resources/kupgrade/{INVENTORY_FILE}",
        env!("CARGO_MANIFEST_DIR")
    );
    let yaml = std::fs::read_to_string(&path).expect(&path);
    let inventory = deserialize_inventory(&yaml).expect("the shipped inventory must be valid");
    assert_eq!(inventory.floor(), "2.0.0");
}
