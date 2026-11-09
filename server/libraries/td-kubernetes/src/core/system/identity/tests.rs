//
// Copyright 2026 Tabsdata Inc.
//

use std::ffi::OsString;

use super::*;

fn environ(entries: &[&str]) -> Vec<OsString> {
    entries.iter().map(OsString::from).collect()
}

#[test]
fn the_stamp_matches_the_format_km_writes() {
    assert_eq!(owner_stamp(1_787_000_000), "20260817T205320Z");
}

#[test]
fn an_owner_drops_its_mint_suffix() {
    assert_eq!(
        parse_owner("4242@20260817T205320Z.123"),
        Some((4242, "20260817T205320Z"))
    );
    assert_eq!(
        owner_key("4242@20260817T205320Z\n").as_deref(),
        Some("4242@20260817T205320Z")
    );
}

#[test]
fn unverifiable_owners_are_rejected() {
    for owner in [
        "4242",
        "4242@",
        "abc@20260817T205320Z",
        "4242@00000000T000000Z",
        "",
    ] {
        assert_eq!(parse_owner(owner), None, "owner: {owner:?}");
    }
}

#[test]
fn a_scoped_token_names_its_run_owner() {
    assert_eq!(
        token_owner_key("4242@20260817T205320Z.007/pod-runner/apiserver").as_deref(),
        Some("4242@20260817T205320Z")
    );
}

#[test]
fn only_the_kill_token_variable_is_read() {
    let entries = environ(&[
        "TD_PR_KILL_TOKEN=1@20260817T205320Z.000",
        "PATH=/bin",
        "TD_KM_KILL_TOKEN=4242@20260817T205320Z.007/pod",
    ]);
    assert_eq!(
        carried_token(entries.iter().map(OsString::as_os_str), KM_KILL_TOKEN_VAR),
        Some("4242@20260817T205320Z.007/pod")
    );
}

#[test]
fn a_process_belongs_only_to_the_recorded_run() {
    let owners = vec!["4242@20260817T205320Z".to_owned()];
    let ours = environ(&["TD_KM_KILL_TOKEN=4242@20260817T205320Z.007/pod"]);
    let other_run = environ(&["TD_KM_KILL_TOKEN=4242@20260817T205321Z.007/pod"]);
    let untokened = environ(&["PATH=/bin"]);
    assert!(carries_owner(ours.iter().map(OsString::as_os_str), &owners));
    assert!(!carries_owner(
        other_run.iter().map(OsString::as_os_str),
        &owners
    ));
    assert!(!carries_owner(
        untokened.iter().map(OsString::as_os_str),
        &owners
    ));
    assert!(!carries_owner(ours.iter().map(OsString::as_os_str), &[]));
}

#[test]
fn the_current_process_runs_since_its_own_stamp() {
    let pid = std::process::id();
    let mut system = system_with(RefreshKind::nothing()).expect("a process table");
    system.refresh_processes(ProcessesToUpdate::Some(&[Pid::from_u32(pid)]), false);
    let started = system
        .process(Pid::from_u32(pid))
        .expect("the current process")
        .start_time();
    assert!(running_since(pid, &owner_stamp(started)).expect("a verdict"));
    assert!(!running_since(pid, "20000101T000000Z").expect("a verdict"));
}

#[test]
fn the_formats_match_kms_identity_contract() {
    for row in km_identity_contract() {
        match row[0].as_str() {
            "var" => assert_eq!(KM_KILL_TOKEN_VAR, row[2]),
            "stamp" => assert_eq!(owner_stamp(row[1].parse().unwrap()), row[2]),
            "unknown_stamp" => assert_eq!(UNKNOWN_STAMP, row[1]),
            "owner" => {
                let expected = row.get(2).map(|pid| format!("{pid}{OWNER_SEP}{}", row[3]));
                assert_eq!(owner_key(&row[1]), expected, "{row:?}");
            }
            "token" => assert_eq!(token_owner_key(&row[1]).as_deref(), Some(row[2].as_str())),
            "path" | "pid_file" => {}
            kind => panic!("unknown contract row {kind:?}"),
        }
    }
}
