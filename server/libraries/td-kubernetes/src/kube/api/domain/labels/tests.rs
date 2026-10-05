//
// Copyright 2026 Tabsdata Inc.
//

use super::*;

#[test]
fn non_ephemeral_selector_excludes_ephemeral_tier() {
    let rendered = tabsdata_non_ephemeral_selector().to_string();
    assert!(rendered.contains(&format!("{PART_OF_LABEL}={PART_OF_TABSDATA}")));
    assert!(rendered.contains(&format!("{TIER_LABEL} notin (ephemeral)")));
}

#[test]
fn the_worker_selector_narrows_the_listing_to_observable_workloads() {
    let rendered = tabsdata_worker_selector().to_string();
    assert!(
        rendered.contains(&format!("{PART_OF_LABEL}={PART_OF_TABSDATA}")),
        "{rendered}"
    );
    assert!(rendered.contains(POD_YAML_WORKER_LABEL), "{rendered}");
    assert!(rendered.contains(LABEL_WORKER_ID), "{rendered}");
}

#[test]
fn the_worker_selector_is_stricter_than_the_plain_one() {
    let plain = tabsdata_selector(None).to_string();
    let worker = tabsdata_worker_selector().to_string();
    assert!(worker.len() > plain.len(), "{worker} vs {plain}");
}
