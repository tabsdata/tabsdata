//
// Copyright 2026 Tabs Data Inc.
//

use super::*;

#[test]
fn prefix_constants_have_expected_values() {
    assert_eq!(PROVIDER_PREFIX_K, "k-");
    assert_eq!(PROVIDER_PREFIX_K8S, "k8s-");
    assert_eq!(PROVIDER_PREFIX_K4S, "k4s-");
}

#[test]
fn kubernetes_topology_keeps_k8s_and_drops_k4s() {
    let topology = KubernetesTopology;
    let prefixes = topology.provider_prefixes();
    assert_eq!(prefixes.keep, PROVIDER_PREFIX_K8S);
    assert_eq!(prefixes.drop, &[PROVIDER_PREFIX_K4S]);
}

#[test]
fn kminus_topology_keeps_k4s_and_drops_k8s() {
    let topology = KminusTopology;
    let prefixes = topology.provider_prefixes();
    assert_eq!(prefixes.keep, PROVIDER_PREFIX_K4S);
    assert_eq!(prefixes.drop, &[PROVIDER_PREFIX_K8S]);
}

#[test]
fn topology_prefixes_returned_as_static_singletons() {
    let kubernetes = KubernetesTopology;
    assert!(std::ptr::eq(
        kubernetes.provider_prefixes(),
        kubernetes.provider_prefixes()
    ));
    let kminus = KminusTopology;
    assert!(std::ptr::eq(
        kminus.provider_prefixes(),
        kminus.provider_prefixes()
    ));
}

#[test]
fn topology_for_kubernetes_returns_kubernetes_prefixes() {
    let topology = topology_for(ClusterProvider::Kubernetes);
    let prefixes = topology.provider_prefixes();
    assert_eq!(prefixes.keep, PROVIDER_PREFIX_K8S);
    assert_eq!(prefixes.drop, &[PROVIDER_PREFIX_K4S]);
}

#[test]
fn topology_for_kminus_returns_kminus_prefixes() {
    let topology = topology_for(ClusterProvider::Kminus);
    let prefixes = topology.provider_prefixes();
    assert_eq!(prefixes.keep, PROVIDER_PREFIX_K4S);
    assert_eq!(prefixes.drop, &[PROVIDER_PREFIX_K8S]);
}

#[test]
fn topology_for_dispatch_is_consistent_across_calls() {
    let first = topology_for(ClusterProvider::Kubernetes).provider_prefixes();
    let second = topology_for(ClusterProvider::Kubernetes).provider_prefixes();
    assert!(std::ptr::eq(first, second));
    let third = topology_for(ClusterProvider::Kminus).provider_prefixes();
    let fourth = topology_for(ClusterProvider::Kminus).provider_prefixes();
    assert!(std::ptr::eq(third, fourth));
}

#[test]
fn topology_for_returns_distinct_prefixes_per_provider() {
    let kubernetes = topology_for(ClusterProvider::Kubernetes).provider_prefixes();
    let kminus = topology_for(ClusterProvider::Kminus).provider_prefixes();
    assert_ne!(kubernetes.keep, kminus.keep);
    assert_ne!(kubernetes.drop, kminus.drop);
}
