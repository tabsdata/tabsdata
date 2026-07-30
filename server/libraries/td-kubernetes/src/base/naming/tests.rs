//
// Copyright 2026 Tabsdata Inc.
//

use super::*;

#[test]
fn to_fn_pod_name_normalizes_logical_worker_id() {
    let name = to_fn_pod_name("function", "WKR20260713.0000000005").unwrap();
    assert!(name.ends_with("wkr202607130000000005"), "{name}");
    assert!(DNS_SUBDOMAIN_PATTERN.is_match(&name), "{name}");
}

#[test]
fn to_fn_pod_name_accepts_legacy_alnum_worker_id() {
    let name = to_fn_pod_name("function", "06ABCDEF0123456789ABCDEFGH").unwrap();
    assert!(name.ends_with("06abcdef0123456789abcdefgh"), "{name}");
}

#[test]
fn normalize_segment_rejects_all_separators() {
    assert!(normalize_segment(NameSegment::WorkerId, "...").is_err());
}
