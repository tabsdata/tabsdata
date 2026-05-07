//
// Copyright 2026 Tabs Data Inc.
//

use crate::constants::FABRIC_KUBERNETES;
use crate::topology::{FABRIC_PREFIX_K4S, FABRIC_PREFIX_K8S, FabricPrefixes, Topology};

pub struct KubernetesTopology;

const PREFIXES: FabricPrefixes = FabricPrefixes {
    keep: FABRIC_PREFIX_K8S,
    drop: &[FABRIC_PREFIX_K4S],
};

impl Topology for KubernetesTopology {
    fn name(&self) -> &'static str {
        FABRIC_KUBERNETES
    }

    fn fabric_prefixes(&self) -> &'static FabricPrefixes {
        &PREFIXES
    }
}
