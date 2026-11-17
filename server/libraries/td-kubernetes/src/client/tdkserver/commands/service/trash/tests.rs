//
// Copyright 2026 Tabsdata Inc.
//

use chrono::NaiveDate;
use strum::IntoEnumIterator;

use super::*;

fn stamp(windows: bool, compact: Option<&str>) -> String {
    NaiveDate::from_ymd_opt(2026, 10, 30)
        .unwrap()
        .and_hms_milli_opt(6, 7, 41, 515)
        .unwrap()
        .format(trash_timestamp_format(windows, compact))
        .to_string()
}

#[test]
fn trash_stamps_are_dotted_off_windows_unless_compact() {
    for compact in [None, Some(""), Some("false")] {
        assert_eq!(stamp(false, compact), "2026.10.30.06.07.41.515");
    }
}

#[test]
fn trash_stamps_have_no_separators_on_windows_or_when_compact() {
    for (windows, compact) in [
        (true, None),
        (true, Some("false")),
        (false, Some("true")),
        (false, Some(" TRUE ")),
    ] {
        assert_eq!(stamp(windows, compact), "20261030060741515");
    }
}

fn mismatched(provider: ClusterProvider) -> Lineage {
    Lineage::Mismatched {
        found: "2.4.0".to_owned(),
        running: "2.5.0".to_owned(),
        provider,
    }
}

#[test]
fn matching_instances_get_the_full_stop_without_a_warning() {
    assert_eq!(Lineage::Matching.stop_plan(), StopPlan::Full);
    assert_eq!(Lineage::Matching.warning("tabsdata"), None);
}

#[test]
fn mismatched_kminus_instances_get_a_best_effort_stop() {
    let lineage = mismatched(ClusterProvider::Kminus);
    assert_eq!(lineage.stop_plan(), StopPlan::BestEffort);
    let warning = lineage.warning("tabsdata").unwrap();
    assert!(warning.contains("2.4.0") && warning.contains("2.5.0"));
}

#[test]
fn unreadable_instances_get_a_best_effort_stop() {
    let lineage = Lineage::Unreadable {
        reason: "missing".to_owned(),
    };
    assert_eq!(lineage.stop_plan(), StopPlan::BestEffort);
    assert!(lineage.warning("tabsdata").unwrap().contains("missing"));
}

#[test]
fn mismatched_external_instances_are_not_stopped() {
    for provider in ClusterProvider::iter().filter(|provider| *provider != ClusterProvider::Kminus)
    {
        assert_eq!(
            mismatched(provider).stop_plan(),
            StopPlan::Unsupported(provider)
        );
    }
}
