//
// Copyright 2026 Tabs Data Inc.
//

use td_error::td_error;

#[td_error]
pub enum KubernetesError {
    #[error("Namespace '{0}' does not exist in the cluster")]
    NamespaceNotFound(String) = 1000,
    #[error("Kubernetes cluster is not reachable: {0}")]
    ClusterUnreachable(String) = 5000,
    #[error("Could not build the Kubernetes client: {0}")]
    ClientBuildFailed(String) = 5001,
    #[error("Could not load Kubernetes configuration: {0}")]
    ConfigLoadFailed(String) = 5002,
    #[error("Could not read kubeconfig at '{0}': {1}")]
    KubeconfigReadFailed(String, String) = 5003,
    #[error("Could not parse kubeconfig: {0}")]
    KubeconfigParseFailed(String) = 5004,
    #[error("Could not verify namespace '{0}': {1}")]
    NamespaceQueryFailed(String, String) = 5005,
    #[error("Could not serialize the active context: {0}")]
    ContextSerializeFailed(String) = 5006,
    #[error("Could not write kubeconfig at '{0}': {1}")]
    KubeconfigWriteFailed(String, String) = 5007,
    #[error("Could not delete kubeconfig at '{0}': {1}")]
    KubeconfigDeleteFailed(String, String) = 5008,
}
