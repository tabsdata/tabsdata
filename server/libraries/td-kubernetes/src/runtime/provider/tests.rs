//
// Copyright 2026 Tabsdata Inc.
//

use strum::IntoEnumIterator;

use super::*;

#[test]
fn every_provider_is_a_known_value_of_the_provider_axis() {
    let providers: Vec<&str> = ClusterProvider::iter()
        .map(ClusterProvider::as_str)
        .collect();
    assert_eq!(providers, PROVIDER_VALUES);
}
