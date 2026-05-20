//
// Copyright 2026 Tabs Data Inc.
//

use crate::topology::Topology;
use crate::topology::kminus::KminusTopology;

use super::*;

#[test]
fn validate_returns_empty_for_default_pod_with_no_rules_in_place() {
    let pod = Pod::default();
    let errors = validate(&pod);
    assert!(
        errors.is_empty(),
        "kminus provider rule catalogue is empty for now; got {:?}",
        errors
    );
}

#[test]
fn topology_validate_pod_routes_to_kminus_validate() {
    let topology = KminusTopology;
    let pod = Pod::default();
    assert_eq!(topology.validate_regular_pod(&pod), validate(&pod));
}
