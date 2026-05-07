//
// Copyright 2026 Tabs Data Inc.
//

use crate::constants::FABRIC_KUBERNETES;
use crate::topology::Topology;

pub struct KubernetesTopology;

impl Topology for KubernetesTopology {
    fn name(&self) -> &'static str {
        FABRIC_KUBERNETES
    }
}
