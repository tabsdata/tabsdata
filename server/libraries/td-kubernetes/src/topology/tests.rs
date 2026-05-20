//
// Copyright 2026 Tabs Data Inc.
//

use super::*;

#[test]
fn provider_value_constants_have_expected_values() {
    assert_eq!(PROVIDER_AXIS_NAME, "provider");
    assert_eq!(PROVIDER_VALUE_K8S, "k8s");
    assert_eq!(PROVIDER_VALUE_K4S, "k4s");
}

#[test]
fn kubernetes_topology_prefers_k8s_on_provider_axis() {
    let topology = KubernetesTopology;
    let axes = topology.axes();
    assert_eq!(axes.len(), 1);
    assert_eq!(axes[0].name, PROVIDER_AXIS_NAME);
    assert_eq!(axes[0].preferred, PROVIDER_VALUE_K8S);
    assert!(axes[0].known.contains(&PROVIDER_VALUE_K8S));
    assert!(axes[0].known.contains(&PROVIDER_VALUE_K4S));
}

#[test]
fn kminus_topology_prefers_k4s_on_provider_axis() {
    let topology = KminusTopology;
    let axes = topology.axes();
    assert_eq!(axes.len(), 1);
    assert_eq!(axes[0].name, PROVIDER_AXIS_NAME);
    assert_eq!(axes[0].preferred, PROVIDER_VALUE_K4S);
    assert!(axes[0].known.contains(&PROVIDER_VALUE_K8S));
    assert!(axes[0].known.contains(&PROVIDER_VALUE_K4S));
}

#[test]
fn topology_axes_returned_as_static_singletons() {
    let kubernetes = KubernetesTopology;
    assert!(std::ptr::eq(kubernetes.axes(), kubernetes.axes()));
    let kminus = KminusTopology;
    assert!(std::ptr::eq(kminus.axes(), kminus.axes()));
}

#[test]
fn topology_for_kubernetes_returns_kubernetes_axes() {
    let topology = topology_for(ClusterProvider::Kubernetes);
    let axes = topology.axes();
    assert_eq!(axes[0].preferred, PROVIDER_VALUE_K8S);
}

#[test]
fn topology_for_kminus_returns_kminus_axes() {
    let topology = topology_for(ClusterProvider::Kminus);
    let axes = topology.axes();
    assert_eq!(axes[0].preferred, PROVIDER_VALUE_K4S);
}

#[test]
fn topology_for_dispatch_is_consistent_across_calls() {
    let first = topology_for(ClusterProvider::Kubernetes).axes();
    let second = topology_for(ClusterProvider::Kubernetes).axes();
    assert!(std::ptr::eq(first, second));
    let third = topology_for(ClusterProvider::Kminus).axes();
    let fourth = topology_for(ClusterProvider::Kminus).axes();
    assert!(std::ptr::eq(third, fourth));
}

#[test]
fn topology_for_returns_distinct_preferences_per_provider() {
    let kubernetes = topology_for(ClusterProvider::Kubernetes).axes();
    let kminus = topology_for(ClusterProvider::Kminus).axes();
    assert_ne!(kubernetes[0].preferred, kminus[0].preferred);
}
