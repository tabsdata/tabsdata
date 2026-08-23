//
// Copyright 2026 Tabsdata Inc.
//

use super::*;

#[test]
fn an_issued_lock_is_held_and_carries_the_id_timestamp() {
    let lock = UpgradeLock::issue("2.0.5");
    assert!(lock.held());
    assert_eq!(lock.target, "2.0.5");
    let id = Id::try_from(lock.id.as_ref().expect("an id")).expect("a base32hex id");
    let timestamp: DateTime<Utc> = id_time(&id).into();
    assert_eq!(lock.timestamp, Some(timestamp));
}

#[test]
fn issued_locks_are_distinct() {
    let one = UpgradeLock::issue("2.0.5");
    let other = UpgradeLock::issue("2.0.5");
    assert_ne!(one.id, other.id);
    assert!(!one.holds(&other));
}

#[test]
fn a_lock_holds_only_itself() {
    let lock = UpgradeLock::issue("2.0.5");
    assert!(lock.holds(&lock.clone()));

    let mut drifted = lock.clone();
    drifted.id = UpgradeLock::issue("2.0.5").id;
    assert!(!lock.holds(&drifted));

    let mut retimed = lock.clone();
    retimed.timestamp = lock
        .timestamp
        .map(|when| when + chrono::Duration::seconds(1));
    assert!(!lock.holds(&retimed));
}

#[test]
fn unlocking_keeps_the_target_and_drops_the_holder() {
    let lock = UpgradeLock::issue("2.0.5");
    let unlocked = lock.unlocked();
    assert_eq!(unlocked.target, "2.0.5");
    assert!(!unlocked.held());
    assert_eq!(unlocked.id, None);
    assert_eq!(unlocked.timestamp, None);
}

#[test]
fn an_unlocked_lock_is_held_by_nobody() {
    let unlocked = UpgradeLock::issue("2.0.5").unlocked();
    assert!(!unlocked.holds(&unlocked.clone()));
    assert!(!UpgradeLock::issue("2.0.5").holds(&unlocked));
    assert!(!unlocked.holds(&UpgradeLock::issue("2.0.5")));
}

#[test]
fn a_lock_round_trips_through_yaml() {
    let lock = UpgradeLock::issue("2.0.5");
    let yaml = serialize_lock(&lock).expect("renders");
    let parsed = deserialize_lock(&yaml).expect("parses").expect("present");
    assert_eq!(parsed, lock);
}

#[test]
fn an_unlocked_lock_round_trips_through_yaml() {
    let unlocked = UpgradeLock::issue("2.0.5").unlocked();
    let yaml = serialize_lock(&unlocked).expect("renders");
    assert!(!yaml.contains("id"), "the holder must not be rendered");
    let parsed = deserialize_lock(&yaml).expect("parses").expect("present");
    assert_eq!(parsed, unlocked);
}

#[test]
fn a_missing_or_empty_lock_reads_as_unlocked() {
    assert_eq!(deserialize_lock("").expect("parses"), None);
    assert_eq!(deserialize_lock("   \n  ").expect("parses"), None);
}

#[test]
fn a_malformed_lock_is_an_error() {
    let error = deserialize_lock("target: [unclosed").expect_err("must reject");
    assert!(matches!(error, UpgradeError::LockParseFailed(_)));
}

#[test]
fn a_lock_without_a_target_is_an_error() {
    let error = deserialize_lock("id: abc\n").expect_err("must reject");
    assert!(matches!(error, UpgradeError::LockParseFailed(_)));
}
