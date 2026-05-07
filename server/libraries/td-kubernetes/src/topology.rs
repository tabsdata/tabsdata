//
// Copyright 2026 Tabs Data Inc.
//

pub mod kminus;
pub mod kubernetes;

use crate::provider::ClusterProvider;
use crate::topology::kminus::KminusTopology;
use crate::topology::kubernetes::KubernetesTopology;

pub const PROVIDER_PREFIX_K: &str = "k-";
pub const PROVIDER_PREFIX_K8S: &str = "k8s-";
pub const PROVIDER_PREFIX_K4S: &str = "k4s-";

pub struct ProviderPrefixes {
    pub keep: &'static str,
    pub drop: &'static [&'static str],
}

/// A `Topology` is the combination of three orthogonal axes:
///
/// * Provider: kubernetes, kminus
/// * Platform:  local, aws, gcs, azure, ...
/// * Service: hosted, cloud, serverless
///
/// Only Provider currently drives differing behavior, so the implementations
/// below are effectively zero-sized markers and [`topology_for`] dispatches
/// on [`ClusterProvider`] alone. That makes the current shape look reducible
/// to `impl Topology for ClusterProvider`, but the simplification is
/// incidental to this transient state: Platform and Service will eventually
/// contribute their own behavior, and keeping the trait plus per-topology
/// types lets that growth land here without churning every call site.
pub trait Topology: Send + Sync {
    fn provider_prefixes(&self) -> &'static ProviderPrefixes;
}

/// Resolves the [`Topology`] for a given [`ClusterProvider`]. See the
/// [`Topology`] trait docs for why dispatch is on Provider alone today.
pub fn topology_for(provider: ClusterProvider) -> &'static dyn Topology {
    match provider {
        ClusterProvider::Kubernetes => &KubernetesTopology,
        ClusterProvider::Kminus => &KminusTopology,
    }
}
