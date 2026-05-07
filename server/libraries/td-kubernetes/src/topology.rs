//
// Copyright 2026 Tabs Data Inc.
//

pub mod kminus;
pub mod kubernetes;

use crate::fabric::ClusterFabric;
use crate::topology::kminus::KminusTopology;
use crate::topology::kubernetes::KubernetesTopology;

pub const FABRIC_PREFIX_K: &str = "k-";
pub const FABRIC_PREFIX_K8S: &str = "k8s-";
pub const FABRIC_PREFIX_K4S: &str = "k4s-";

pub struct FabricPrefixes {
    pub keep: &'static str,
    pub drop: &'static [&'static str],
}

pub trait Topology: Send + Sync {
    fn name(&self) -> &'static str;
    fn fabric_prefixes(&self) -> &'static FabricPrefixes;
}

pub fn topology_for(fabric: ClusterFabric) -> &'static dyn Topology {
    match fabric {
        ClusterFabric::Kubernetes => &KubernetesTopology,
        ClusterFabric::Kminus => &KminusTopology,
    }
}
