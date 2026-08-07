//
// Copyright 2026 Tabsdata Inc.
//

use super::*;

#[test]
fn to_fn_pod_name_normalizes_logical_worker_id() {
    let name = to_fn_pod_name("pub", "WKR20260713.0000000005").unwrap();
    assert_eq!(name, "pub-wkr202607130000000005");
    assert!(DNS_SUBDOMAIN_PATTERN.is_match(&name), "{name}");
}

#[test]
fn to_fn_pod_name_accepts_legacy_alnum_worker_id() {
    let name = to_fn_pod_name("tfr", "06ABCDEF0123456789ABCDEFGH").unwrap();
    assert_eq!(name, "tfr-06abcdef0123456789abcdefgh");
}

#[test]
fn normalize_segment_rejects_all_separators() {
    assert!(normalize_segment(NameSegment::WorkerId, "...").is_err());
}

#[test]
fn worker_name_prefix_maps_each_worker_and_decorator() {
    assert_eq!(
        worker_name_prefix(&WorkerName::Function, Some(&Decorator::Publisher)),
        "pub"
    );
    assert_eq!(
        worker_name_prefix(&WorkerName::Function, Some(&Decorator::Subscriber)),
        "sub"
    );
    assert_eq!(
        worker_name_prefix(&WorkerName::Function, Some(&Decorator::Transformer)),
        "tfr"
    );
    assert_eq!(worker_name_prefix(&WorkerName::Function, None), "fun");
    assert_eq!(worker_name_prefix(&WorkerName::CronTrigger, None), "crn");
    assert_eq!(worker_name_prefix(&WorkerName::StageTrigger, None), "stm");
}
