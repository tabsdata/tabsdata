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
fn a_manifest_whose_entries_all_agree_has_none_behind() {
    let manifest = manifest(
        "2.0.1",
        &[
            ("pods/a.yaml", "2.0.1", EntryState::Active),
            ("pods/b.yaml", "2.0.1", EntryState::Active),
        ],
    );
    assert!(entries_behind(&manifest).is_empty());
}

#[test]
fn entries_at_another_version_are_reported_with_their_version() {
    let manifest = manifest(
        "2.0.0",
        &[
            ("pods/a.yaml", "2.0.1", EntryState::Active),
            ("pods/b.yaml", "2.0.0", EntryState::Active),
        ],
    );
    assert_eq!(entries_behind(&manifest), vec!["pods/a.yaml (at 2.0.1)"]);
}

#[test]
fn deleted_entries_are_never_reported() {
    let manifest = manifest(
        "2.0.1",
        &[
            ("pods/a.yaml", "2.0.1", EntryState::Active),
            ("pods/gone.yaml", "2.0.0", EntryState::Deleted),
        ],
    );
    assert!(entries_behind(&manifest).is_empty());
}

#[test]
fn every_stray_entry_is_reported() {
    let manifest = manifest(
        "2.0.0",
        &[
            ("pods/a.yaml", "2.0.1", EntryState::Active),
            ("pods/b.yaml", "2.0.1", EntryState::Active),
            ("pods/c.yaml", "2.0.0", EntryState::Active),
        ],
    );
    assert_eq!(entries_behind(&manifest).len(), 2);
}
