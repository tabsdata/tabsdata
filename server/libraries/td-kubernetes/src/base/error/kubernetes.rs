//
// Copyright 2026 Tabs Data Inc.
//

//! Kubernetes error taxonomy.
//!
//! The umbrella [`KubernetesError`] enum groups scope-specific error enums
//! (`Kube*Error`) defined under `base::error::kubernetes::*`. Scope enums are
//! the only errors constructible from `kube::api::*`; other modules consume
//! them through this umbrella or through `td_error::TdError`.

pub mod client;
pub mod cluster;
pub mod configmap;
pub mod job;
pub mod kubeconfig;
pub mod messaging;
pub mod namespace;
pub mod pod;
pub mod podcontext;
pub mod secret;

use td_error::TdError;
use thiserror::Error;

pub use client::KubeClientError;
pub use cluster::KubeClusterError;
pub use configmap::KubeConfigMapError;
pub use job::KubeJobError;
pub use kubeconfig::KubeconfigError;
pub use messaging::KubeMessagingError;
pub use namespace::KubeNamespaceError;
pub use pod::KubePodError;
pub use podcontext::KubePodContextError;
pub use secret::KubeSecretError;

#[derive(Debug, Error)]
pub enum KubernetesError {
    #[error(transparent)]
    Client(#[from] KubeClientError),
    #[error(transparent)]
    Cluster(#[from] KubeClusterError),
    #[error(transparent)]
    ConfigMap(#[from] KubeConfigMapError),
    #[error(transparent)]
    Job(#[from] KubeJobError),
    #[error(transparent)]
    Kubeconfig(#[from] KubeconfigError),
    #[error(transparent)]
    Messaging(#[from] KubeMessagingError),
    #[error(transparent)]
    Namespace(#[from] KubeNamespaceError),
    #[error(transparent)]
    Pod(#[from] KubePodError),
    #[error(transparent)]
    PodContext(#[from] KubePodContextError),
    #[error(transparent)]
    Secret(#[from] KubeSecretError),
}

impl From<KubernetesError> for TdError {
    fn from(error: KubernetesError) -> Self {
        match error {
            KubernetesError::Client(inner) => TdError::new(inner),
            KubernetesError::Cluster(inner) => TdError::new(inner),
            KubernetesError::ConfigMap(inner) => TdError::new(inner),
            KubernetesError::Job(inner) => TdError::new(inner),
            KubernetesError::Kubeconfig(inner) => TdError::new(inner),
            KubernetesError::Messaging(inner) => TdError::new(inner),
            KubernetesError::Namespace(inner) => TdError::new(inner),
            KubernetesError::Pod(inner) => TdError::new(inner),
            KubernetesError::PodContext(inner) => TdError::new(inner),
            KubernetesError::Secret(inner) => TdError::new(inner),
        }
    }
}
