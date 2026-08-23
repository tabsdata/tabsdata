//
// Copyright 2026 Tabsdata Inc.
//

use chrono::Utc;

use super::*;
use crate::runtime::instance::manifest::{MANIFEST_SCHEMA, ManifestEntry, VersionStamp};

fn manifest(global: &str, entries: &[(&str, &str, EntryState)]) -> Manifest {
    let now = Utc::now();
    Manifest {
        schema: MANIFEST_SCHEMA,
        version: global.to_owned(),
        created: now,
        updated: now,
        entries: entries
            .iter()
            .map(|(path, version, state)| {
                (
                    (*path).to_owned(),
                    ManifestEntry {
                        state: *state,
                        version: (*version).to_owned(),
                        created: now,
                        source: (*path).to_owned(),
                        versions: vec![VersionStamp {
                            version: (*version).to_owned(),
                            when: now,
                            changed: true,
                        }],
                    },
                )
            })
            .collect(),
    }
}

#[test]
fn a_consistent_instance_on_the_running_version_is_settled() {
    let manifest = manifest(
        "2.0.1",
        &[
            ("pods/a.yaml", "2.0.1", EntryState::Active),
            ("pods/b.yaml", "2.0.1", EntryState::Active),
        ],
    );
    assert_eq!(
        stand(&manifest, None, Some("2.0.1"), "2.0.1"),
        Standing::Settled
    );
}

#[test]
fn a_deleted_entry_at_another_version_does_not_disturb_a_settled_instance() {
    let manifest = manifest(
        "2.0.1",
        &[
            ("pods/a.yaml", "2.0.1", EntryState::Active),
            ("pods/gone.yaml", "2.0.0", EntryState::Deleted),
        ],
    );
    assert_eq!(
        stand(&manifest, None, Some("2.0.1"), "2.0.1"),
        Standing::Settled
    );
}

#[test]
fn a_held_lock_reports_the_upgrade_in_flight() {
    let manifest = manifest("2.0.0", &[("pods/a.yaml", "2.0.0", EntryState::Active)]);
    let lock = UpgradeLock::issue("2.0.1");
    assert_eq!(
        stand(&manifest, Some(&lock), Some("2.0.0"), "2.0.1"),
        Standing::Upgrading {
            target: "2.0.1".to_owned(),
            held: true,
            entries_behind: 0,
        }
    );
}

#[test]
fn an_unlocked_lock_still_reports_the_upgrade_in_flight() {
    let manifest = manifest("2.0.0", &[("pods/a.yaml", "2.0.0", EntryState::Active)]);
    let lock = UpgradeLock::issue("2.0.1").unlocked();
    let standing = stand(&manifest, Some(&lock), Some("2.0.0"), "2.0.1");
    assert!(matches!(
        standing,
        Standing::Upgrading {
            held: false,
            ref target,
            ..
        } if target == "2.0.1"
    ));
}

#[test]
fn entries_ahead_of_the_global_version_are_a_partial_upgrade() {
    let manifest = manifest(
        "2.0.0",
        &[
            ("pods/a.yaml", "2.0.1", EntryState::Active),
            ("pods/b.yaml", "2.0.0", EntryState::Active),
        ],
    );
    assert_eq!(
        stand(&manifest, None, Some("2.0.0"), "2.0.0"),
        Standing::Upgrading {
            target: "2.0.0".to_owned(),
            held: false,
            entries_behind: 1,
        }
    );
}

#[test]
fn a_consistent_instance_built_by_another_tabsdata_is_mismatched() {
    let manifest = manifest("2.0.0", &[("pods/a.yaml", "2.0.0", EntryState::Active)]);
    assert_eq!(
        stand(&manifest, None, Some("2.0.0"), "2.0.1"),
        Standing::Mismatched {
            instance: "2.0.0".to_owned(),
            product: "2.0.1".to_owned(),
        }
    );
}

#[test]
fn a_label_disagreeing_with_the_global_version_is_mismatched() {
    let manifest = manifest("2.0.1", &[("pods/a.yaml", "2.0.1", EntryState::Active)]);
    assert_eq!(
        stand(&manifest, None, Some("2.0.0"), "2.0.1"),
        Standing::Mismatched {
            instance: "2.0.1".to_owned(),
            product: "2.0.1".to_owned(),
        }
    );
}

#[test]
fn an_absent_label_reads_as_the_legacy_version() {
    assert_eq!(stamped(None), LEGACY_VERSION);
    assert_eq!(stamped(Some("2.0.1")), "2.0.1");
}

#[test]
fn a_legacy_instance_under_its_own_tabsdata_is_settled() {
    let manifest = manifest(
        LEGACY_VERSION,
        &[("pods/a.yaml", LEGACY_VERSION, EntryState::Active)],
    );
    assert_eq!(
        stand(&manifest, None, None, LEGACY_VERSION),
        Standing::Settled
    );
}

#[test]
fn a_legacy_instance_under_a_newer_tabsdata_is_mismatched() {
    let manifest = manifest(
        LEGACY_VERSION,
        &[("pods/a.yaml", LEGACY_VERSION, EntryState::Active)],
    );
    assert_eq!(
        stand(&manifest, None, None, "2.0.1"),
        Standing::Mismatched {
            instance: LEGACY_VERSION.to_owned(),
            product: "2.0.1".to_owned(),
        }
    );
}

#[test]
fn an_absent_label_on_a_post_legacy_instance_is_mismatched() {
    let manifest = manifest("2.0.1", &[("pods/a.yaml", "2.0.1", EntryState::Active)]);
    assert_eq!(
        stand(&manifest, None, None, "2.0.1"),
        Standing::Mismatched {
            instance: "2.0.1".to_owned(),
            product: "2.0.1".to_owned(),
        }
    );
}
