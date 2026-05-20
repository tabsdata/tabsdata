//
// Copyright 2026 Tabs Data Inc.
//

pub mod kminus;
pub mod kubernetes;

use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::Pod;

use crate::instance::profile::bundle::Axis;
use crate::pod::ValidationError;
use crate::provider::ClusterProvider;
use crate::system::exit::Result;
use crate::topology::kminus::KminusTopology;
use crate::topology::kubernetes::KubernetesTopology;

pub const PROVIDER_AXIS_NAME: &str = "provider";
pub const PROVIDER_VALUE_K8S: &str = "k8s";
pub const PROVIDER_VALUE_K4S: &str = "k4s";

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
    fn axes(&self) -> &'static [Axis];
    fn validate_regular_pod(&self, pod: &Pod) -> Vec<ValidationError>;

    fn enrich_regular_pod(&self, pod: &mut Pod) -> Result {
        let _ = pod;
        Ok(())
    }

    fn enrich_init_job(&self, job: &mut Job) -> Result {
        let _ = job;
        Ok(())
    }
    fn validate_init_job(&self, job: &Job) -> Vec<ValidationError> {
        let _ = job;
        Vec::new()
    }
}

/// Resolves the [`Topology`] for a given [`ClusterProvider`]. See the
/// [`Topology`] trait docs for why dispatch is on Provider alone today.
pub fn topology_for(provider: ClusterProvider) -> &'static dyn Topology {
    match provider {
        ClusterProvider::Kubernetes => &KubernetesTopology,
        ClusterProvider::Kminus => &KminusTopology,
    }
}

#[cfg(test)]
mod tests;
