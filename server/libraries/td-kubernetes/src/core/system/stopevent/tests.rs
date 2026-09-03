//
// Copyright 2026 Tabsdata Inc.
//

use super::*;

#[test]
fn separators_case_and_trailing_slashes_do_not_change_the_name() {
    let canonical = stop_event_name("C:\\Instances\\Tabsdata\\cluster");
    for variant in [
        "C:/Instances/Tabsdata/cluster",
        "c:\\instances\\tabsdata\\cluster",
        "C:\\Instances\\Tabsdata\\cluster\\",
        "C:/Instances/Tabsdata/cluster/",
    ] {
        assert_eq!(canonical, stop_event_name(variant), "variant: {variant}");
    }
}

#[test]
fn different_instances_yield_different_names() {
    assert_ne!(
        stop_event_name("C:\\instances\\one\\cluster"),
        stop_event_name("C:\\instances\\two\\cluster"),
    );
}

#[test]
fn the_name_matches_the_one_kminus_opens() {
    assert_eq!(
        stop_event_name("C:\\instances\\tabsdata\\cluster"),
        "Local\\tabsdata-km-stop-a2b0536564787962",
    );
}
