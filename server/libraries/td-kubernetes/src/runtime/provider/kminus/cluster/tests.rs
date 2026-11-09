//
// Copyright 2026 Tabsdata Inc.
//

use super::*;

const REPORT: &str = "
Tabsdata K- (K minus) instance cannot be created

  ports      kube-apiserver api  2461  IN USE (Address already in use (os error 48))
             kube-controller-manager  2462  free
             pod-runner  2463  IN USE (Address already in use (os error 48))
             pod-runner metrics  2464  free
             virtual-kubelet  2465  free
             etcd  2466  free
             etcd peer  2467  free

ERROR: kube-apiserver api port 2461 is unusable: Address already in use (os error 48)
Cannot create: 2 check(s) failed
";

#[test]
fn only_busy_ports_are_reported() {
    let bound = parse_bound_ports(REPORT);
    assert_eq!(
        bound.iter().map(|entry| entry.port).collect::<Vec<_>>(),
        vec![2461, 2463]
    );
}

#[test]
fn multi_word_labels_survive_parsing() {
    let bound = parse_bound_ports(REPORT);
    assert_eq!(bound[0].label, "kube-apiserver api");
    assert_eq!(bound[1].label, "pod-runner");
}

#[test]
fn every_reason_reads_the_same_as_a_locally_detected_one() {
    for entry in parse_bound_ports(REPORT) {
        assert_eq!(entry.reason, PORT_BOUND_REASON);
    }
}

#[test]
fn a_nested_parenthesis_does_not_leak_into_the_report() {
    let bound = parse_bound_ports(REPORT);
    for entry in &bound {
        let opened = entry.reason.matches('(').count();
        let closed = entry.reason.matches(')').count();
        assert_eq!(opened, closed, "{}", entry.reason);
    }
    assert!(!bound.is_empty());
}

#[test]
fn a_clean_report_yields_nothing() {
    let clean = "  ports      kube-apiserver api  2461  free\n             etcd  2466  free";
    assert!(parse_bound_ports(clean).is_empty());
}

#[test]
fn unrelated_output_is_ignored() {
    assert!(parse_bound_ports("Nothing was created.\nERROR: something else").is_empty());
}

#[test]
fn the_status_contract_maps_every_documented_code() {
    assert_eq!(ClusterState::from_exit_code(Some(0)), ClusterState::Started);
    assert_eq!(
        ClusterState::from_exit_code(Some(1)),
        ClusterState::Starting
    );
    assert_eq!(ClusterState::from_exit_code(Some(2)), ClusterState::Dead);
}

#[test]
fn an_unexpected_code_is_not_taken_as_started_or_dead() {
    for code in [3, 4, 127] {
        assert_eq!(
            ClusterState::from_exit_code(Some(code)),
            ClusterState::Unknown,
            "{code}"
        );
    }
    assert_eq!(ClusterState::from_exit_code(None), ClusterState::Unknown);
}

fn own_identity() -> String {
    let pid = std::process::id();
    let mut system =
        td_common::os::system_with(sysinfo::RefreshKind::nothing()).expect("a process table");
    system.refresh_processes(
        sysinfo::ProcessesToUpdate::Some(&[sysinfo::Pid::from_u32(pid)]),
        false,
    );
    let started = system
        .process(sysinfo::Pid::from_u32(pid))
        .expect("the current process")
        .start_time();
    format!("{pid}@{}.042", identity::owner_stamp(started))
}

fn pid_file(folder: &Path, pid: u32, content: &str) {
    fs::create_dir_all(folder).expect("a pids folder");
    fs::write(folder.join(format!("{pid}.{KM_BINARY}")), content).expect("a pid file");
}

#[test]
fn a_missing_pid_file_is_absent() {
    let root = tempfile::tempdir().expect("a temporary folder");
    assert_eq!(read_km_pid(&root.path().join("pids")), KmRecord::Absent);
}

#[test]
fn a_pid_file_naming_a_running_process_is_live() {
    let root = tempfile::tempdir().expect("a temporary folder");
    let pid = std::process::id();
    pid_file(root.path(), pid, &format!("{}\n", own_identity()));
    assert_eq!(read_km_pid(root.path()), KmRecord::Live(pid));
}

#[test]
fn a_pid_file_from_another_start_is_stale() {
    let root = tempfile::tempdir().expect("a temporary folder");
    let pid = std::process::id();
    pid_file(root.path(), pid, &format!("{pid}@20000101T000000Z"));
    assert_eq!(read_km_pid(root.path()), KmRecord::Stale(pid));
}

#[test]
fn an_unverifiable_pid_file_is_stale() {
    let pid = std::process::id();
    for content in [
        String::new(),
        pid.to_string(),
        format!("{pid}@00000000T000000Z"),
        own_identity().replacen(&pid.to_string(), &(pid + 1).to_string(), 1),
    ] {
        let root = tempfile::tempdir().expect("a temporary folder");
        pid_file(root.path(), pid, &content);
        assert_eq!(
            read_km_pid(root.path()),
            KmRecord::Stale(pid),
            "content: {content:?}"
        );
    }
}

#[test]
fn a_stopped_or_replaced_km_is_gone() {
    assert!(km_gone(KmRecord::Absent, 7));
    assert!(km_gone(KmRecord::Stale(7), 7));
    assert!(km_gone(KmRecord::Live(8), 7));
    assert!(!km_gone(KmRecord::Live(7), 7));
    assert!(!km_gone(KmRecord::Unknown(7), 7));
    assert!(km_live(KmRecord::Live(7), 7));
    assert!(!km_live(KmRecord::Unknown(7), 7));
}

#[test]
fn the_run_owners_come_from_the_pid_file_and_the_run_token() {
    let root = tempfile::tempdir().expect("a temporary folder");
    let cluster = KminusCluster::new(root.path(), root.path().join("images"), false, None, None);
    pid_file(&cluster.pids_path(), 4242, "4242@20260817T205320Z");
    fs::write(cluster.run_token_path(), "4242@20260817T205320Z.007").expect("a run token");
    assert_eq!(cluster.run_owners(), ["4242@20260817T205320Z"]);
    fs::write(cluster.run_token_path(), "5151@20260817T205321Z.007").expect("a run token");
    assert_eq!(
        cluster.run_owners(),
        ["4242@20260817T205320Z", "5151@20260817T205321Z"]
    );
}

#[test]
fn the_stop_event_names_the_folder_km_resolves() {
    let root = tempfile::tempdir().expect("a temporary folder");
    let cluster = root.path().join("cluster");
    fs::create_dir_all(&cluster).expect("a cluster folder");
    let roundabout = cluster.join("..").join("cluster");
    let resolved = dunce::canonicalize(&cluster).expect("a canonical folder");
    assert_eq!(stop_event_folder(&roundabout), resolved.to_string_lossy());
    assert_eq!(
        stopevent::stop_event_name(&stop_event_folder(&roundabout)),
        stopevent::stop_event_name(&resolved.to_string_lossy())
    );
    assert!(!stop_event_folder(&cluster).starts_with(r"\\?\"));
}

#[test]
fn a_missing_folder_keeps_its_given_name_for_the_stop_event() {
    let root = tempfile::tempdir().expect("a temporary folder");
    let absent = root.path().join("absent");
    assert_eq!(stop_event_folder(&absent), absent.to_string_lossy());
}

#[test]
fn the_layout_matches_kms_identity_contract() {
    let root = tempfile::tempdir().expect("a temporary folder");
    let cluster = KminusCluster::new(root.path(), root.path().join("images"), false, None, None);
    for row in crate::core::system::identity::km_identity_contract() {
        match (row[0].as_str(), row.get(1).map(String::as_str)) {
            ("path", Some("km_pids")) => assert_eq!(cluster.pids_path(), root.path().join(&row[2])),
            ("path", Some("km_run_token")) => {
                assert_eq!(cluster.run_token_path(), root.path().join(&row[2]))
            }
            ("path", kind) => panic!("unknown contract path {kind:?}"),
            ("pid_file", Some(sample)) => {
                fs::create_dir_all(cluster.pids_path()).expect("a pids folder");
                fs::write(cluster.pids_path().join(&row[2]), "").expect("a pid file");
                let pids: Vec<u32> = km_pid_files(&cluster.pids_path())
                    .into_iter()
                    .map(|(pid, _)| pid)
                    .collect();
                assert_eq!(pids, [sample.parse::<u32>().unwrap()]);
            }
            _ => {}
        }
    }
}
