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
