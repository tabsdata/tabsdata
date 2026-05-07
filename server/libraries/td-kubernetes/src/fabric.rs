//
// Copyright 2026 Tabs Data Inc.
//

use clap_derive::ValueEnum;

use crate::topology::Topology;
use crate::topology::kminus::KminusTopology;
use crate::topology::kubernetes::KubernetesTopology;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ClusterFabric {
    Kubernetes,
    Kminus,
}

pub fn topology_for(fabric: ClusterFabric) -> &'static dyn Topology {
    match fabric {
        ClusterFabric::Kubernetes => &KubernetesTopology,
        ClusterFabric::Kminus => &KminusTopology,
    }
}
