//
// Copyright 2026 Tabs Data Inc.
//

pub mod kminus;
pub mod kubernetes;

use clap_derive::ValueEnum;

use crate::topology::kminus::KminusTopology;
use crate::topology::kubernetes::KubernetesTopology;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ClusterFabric {
    Kubernetes,
    Kminus,
}

pub trait Topology: Send + Sync {
    fn name(&self) -> &'static str;
}

pub fn topology_for(fabric: ClusterFabric) -> &'static dyn Topology {
    match fabric {
        ClusterFabric::Kubernetes => &KubernetesTopology,
        ClusterFabric::Kminus => &KminusTopology,
    }
}
