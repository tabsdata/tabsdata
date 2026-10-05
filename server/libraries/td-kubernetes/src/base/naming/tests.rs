//
// Copyright 2026 Tabsdata Inc.
//

use super::*;

#[test]
fn to_fn_pod_name_spells_the_worker_id_as_the_api_does() {
    let worker_id = WorkerId::try_from("wkr20260713-0000000005").unwrap();
    let name = to_fn_pod_name("pub", &worker_id).unwrap();
    assert_eq!(name, "pub-wkr20260713-5");
    assert!(DNS_SUBDOMAIN_PATTERN.is_match(&name), "{name}");
}

#[test]
fn logical_id_text_is_the_unpadded_spelling() {
    let worker_id = WorkerId::try_from("wkr20260713-0000000005").unwrap();
    assert_eq!(logical_id_text(&worker_id), "wkr20260713-5");
}

#[test]
fn normalize_segment_keeps_the_shape_of_the_value() {
    let normalized = normalize_segment(NameSegment::WorkerId, "WKR20260713.5").unwrap();
    assert_eq!(normalized, "wkr20260713-5");
    let normalized = normalize_segment(NameSegment::WorkerId, "-a__b-").unwrap();
    assert_eq!(normalized, "a-b");
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
