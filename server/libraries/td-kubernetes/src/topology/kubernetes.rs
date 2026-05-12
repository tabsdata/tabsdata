//
// Copyright 2026 Tabs Data Inc.
//

use k8s_openapi::api::core::v1::Pod;

use crate::pod::{ValidationError, kubernetes as kubernetes_pod};
use crate::topology::{PROVIDER_PREFIX_K4S, PROVIDER_PREFIX_K8S, ProviderPrefixes, Topology};

pub struct KubernetesTopology;

const PREFIXES: ProviderPrefixes = ProviderPrefixes {
    keep: PROVIDER_PREFIX_K8S,
    drop: &[PROVIDER_PREFIX_K4S],
};

impl Topology for KubernetesTopology {
    fn provider_prefixes(&self) -> &'static ProviderPrefixes {
        &PREFIXES
    }

    fn validate_pod(&self, pod: &Pod) -> Vec<ValidationError> {
        kubernetes_pod::validate(pod)
    }
}
