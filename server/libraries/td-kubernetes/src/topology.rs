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

/// A `Topology` is the combination of three orthogonal axes:
///
/// * Fabric:   kubernetes, kminus
/// * Hosting:  hosted, cloud, serverless
/// * Provider: local, aws, gcs, azure, ...
///
/// Only Fabric currently drives differing behavior, so the implementations
/// below are effectively zero-sized markers and [`topology_for`] dispatches
/// on [`ClusterFabric`] alone. That makes the current shape look reducible
/// to `impl Topology for ClusterFabric`, but the simplification is
/// incidental to this transient state: Hosting and Provider will eventually
/// contribute their own behavior, and keeping the trait plus per-topology
/// types lets that growth land here without churning every call site.
pub trait Topology: Send + Sync {
    fn fabric_prefixes(&self) -> &'static FabricPrefixes;
}

/// Resolves the [`Topology`] for a given [`ClusterFabric`]. See the
/// [`Topology`] trait docs for why dispatch is on Fabric alone today.
pub fn topology_for(fabric: ClusterFabric) -> &'static dyn Topology {
    match fabric {
        ClusterFabric::Kubernetes => &KubernetesTopology,
        ClusterFabric::Kminus => &KminusTopology,
    }
}
