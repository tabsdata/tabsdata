//
// Copyright 2026 Tabs Data Inc.
//

use chrono::TimeZone;

use super::*;

fn fixed_time() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 5, 8, 12, 0, 0).unwrap()
}

fn manifest_with_entries(entries: &[(&str, &str, EntryState)]) -> Manifest {
    let now = fixed_time();
    let stamp = VersionStamp {
        version: "1.4.0".to_owned(),
        when: now,
        changed: true,
    };
    let mut map = BTreeMap::new();
    for (path, version, state) in entries {
        map.insert(
            (*path).to_owned(),
            ManifestEntry {
                state: *state,
                version: (*version).to_owned(),
                created: now,
                source: (*path).to_owned(),
                versions: vec![VersionStamp {
                    version: (*version).to_owned(),
                    ..stamp.clone()
                }],
            },
        );
    }
    Manifest {
        schema: MANIFEST_SCHEMA,
        version: "1.4.0".to_owned(),
        created: now,
        updated: now,
        entries: map,
    }
}

#[test]
fn initial_install_seeds_every_entry_as_active_with_one_changed_stamp() {
    let now = fixed_time();
    let manifest = Manifest::initial_install(
        "1.4.0",
        now,
        [
            (
                "pods/foo.yaml".to_owned(),
                "pods/foo_kminus.yaml".to_owned(),
            ),
            ("config.yaml".to_owned(), "config.yaml".to_owned()),
        ],
    );
    assert_eq!(manifest.schema, MANIFEST_SCHEMA);
    assert_eq!(manifest.version, "1.4.0");
    assert_eq!(manifest.created, now);
    assert_eq!(manifest.updated, now);
    assert_eq!(manifest.entries.len(), 2);
    for entry in manifest.entries.values() {
        assert_eq!(entry.state, EntryState::Active);
        assert_eq!(entry.version, "1.4.0");
        assert_eq!(entry.created, now);
        assert_eq!(entry.versions.len(), 1);
        let stamp = &entry.versions[0];
        assert_eq!(stamp.version, "1.4.0");
        assert_eq!(stamp.when, now);
        assert!(stamp.changed);
    }
}

#[test]
fn initial_install_records_source_for_each_entry() {
    let now = fixed_time();
    let manifest = Manifest::initial_install(
        "1.4.0",
        now,
        [
            (
                "pods/foo.yaml".to_owned(),
                "pods/foo_kminus.yaml".to_owned(),
            ),
            ("config.yaml".to_owned(), "config.yaml".to_owned()),
        ],
    );
    assert_eq!(
        manifest.entries.get("pods/foo.yaml").unwrap().source,
        "pods/foo_kminus.yaml"
    );
    assert_eq!(
        manifest.entries.get("config.yaml").unwrap().source,
        "config.yaml"
    );
}

#[test]
fn manifest_yaml_round_trip_preserves_every_field() {
    let manifest = manifest_with_entries(&[
        ("pods/foo.yaml", "1.4.0", EntryState::Active),
        ("configs/bar.yaml", "1.3.2", EntryState::Deleted),
    ]);
    let yaml = serde_yaml::to_string(&manifest).expect("serialize");
    let parsed: Manifest = serde_yaml::from_str(&yaml).expect("deserialize");
    assert_eq!(parsed.schema, manifest.schema);
    assert_eq!(parsed.version, manifest.version);
    assert_eq!(parsed.created, manifest.created);
    assert_eq!(parsed.updated, manifest.updated);
    assert_eq!(parsed.entries.len(), manifest.entries.len());
    for (path, entry) in &manifest.entries {
        let other = parsed.entries.get(path).expect("entry preserved");
        assert_eq!(other.state, entry.state);
        assert_eq!(other.version, entry.version);
        assert_eq!(other.created, entry.created);
        assert_eq!(other.versions.len(), entry.versions.len());
    }
}

#[test]
fn entry_state_serializes_lowercase() {
    let active = serde_yaml::to_string(&EntryState::Active).unwrap();
    let deleted = serde_yaml::to_string(&EntryState::Deleted).unwrap();
    assert_eq!(active.trim(), "active");
    assert_eq!(deleted.trim(), "deleted");
}

#[test]
fn check_versions_passes_when_major_minor_match() {
    let manifest = manifest_with_entries(&[
        ("pods/foo.yaml", "1.4.0", EntryState::Active),
        ("pods/bar.yaml", "1.4.7", EntryState::Active),
    ]);
    assert!(check_versions(&manifest, "1.4.0").is_ok());
    assert!(check_versions(&manifest, "1.4.99").is_ok());
}

#[test]
fn check_versions_ignores_micro_difference() {
    let manifest = manifest_with_entries(&[("pods/foo.yaml", "1.4.5", EntryState::Active)]);
    assert!(check_versions(&manifest, "1.4.0").is_ok());
}

#[test]
fn check_versions_fails_on_minor_mismatch() {
    let manifest = manifest_with_entries(&[("pods/foo.yaml", "1.4.0", EntryState::Active)]);
    let outcome = check_versions(&manifest, "1.5.0");
    let Err(VersionMismatch::Mismatch { running, entries }) = outcome else {
        panic!("expected mismatch, got {outcome:?}");
    };
    assert_eq!(running, "1.5.0");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].path, "pods/foo.yaml");
}

#[test]
fn check_versions_fails_on_major_mismatch() {
    let manifest = manifest_with_entries(&[("pods/foo.yaml", "1.4.0", EntryState::Active)]);
    let outcome = check_versions(&manifest, "2.4.0");
    assert!(matches!(outcome, Err(VersionMismatch::Mismatch { .. })));
}

#[test]
fn check_versions_skips_deleted_entries() {
    let manifest = manifest_with_entries(&[
        ("pods/foo.yaml", "1.4.0", EntryState::Active),
        ("pods/old.yaml", "0.9.0", EntryState::Deleted),
    ]);
    assert!(check_versions(&manifest, "1.4.0").is_ok());
}

#[test]
fn check_versions_reports_every_active_mismatch() {
    let manifest = manifest_with_entries(&[
        ("pods/foo.yaml", "1.4.0", EntryState::Active),
        ("pods/bar.yaml", "1.3.0", EntryState::Active),
        ("pods/baz.yaml", "1.4.5", EntryState::Active),
    ]);
    let Err(VersionMismatch::Mismatch { entries, .. }) = check_versions(&manifest, "1.4.0") else {
        panic!("expected mismatch");
    };
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].path, "pods/bar.yaml");
}

#[test]
fn check_versions_returns_parse_error_for_bad_running_version() {
    let manifest = manifest_with_entries(&[("pods/foo.yaml", "1.4.0", EntryState::Active)]);
    let outcome = check_versions(&manifest, "not-a-version");
    assert!(matches!(outcome, Err(VersionMismatch::Parse { .. })));
}

#[test]
fn check_versions_returns_parse_error_for_bad_entry_version() {
    let manifest = manifest_with_entries(&[("pods/foo.yaml", "garbage", EntryState::Active)]);
    let outcome = check_versions(&manifest, "1.4.0");
    assert!(matches!(outcome, Err(VersionMismatch::Parse { .. })));
}
