//
// Copyright 2026 Tabs Data Inc.
//

use crate::topology::{FABRIC_PREFIX_K4S, FABRIC_PREFIX_K8S, FabricPrefixes, Topology};

pub struct KminusTopology;

const PREFIXES: FabricPrefixes = FabricPrefixes {
    keep: FABRIC_PREFIX_K4S,
    drop: &[FABRIC_PREFIX_K8S],
};

impl Topology for KminusTopology {
    fn fabric_prefixes(&self) -> &'static FabricPrefixes {
        &PREFIXES
    }
}
