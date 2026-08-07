//
// Copyright 2026 Tabsdata Inc.
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
pub mod cronjob;
pub mod deployment;
pub mod job;
pub mod kubeconfig;
pub mod messaging;
pub mod namespace;
pub mod pod;
pub mod podcontext;
pub mod rbac;
pub mod secret;
pub mod service;

use td_error::TdError;
use thiserror::Error;

use crate::base::error::naming::NamingError;

pub use client::KubeClientError;
pub use cluster::KubeClusterError;
pub use configmap::KubeConfigMapError;
pub use cronjob::KubeCronJobError;
pub use deployment::KubeDeploymentError;
pub use job::KubeJobError;
pub use kubeconfig::KubeconfigError;
pub use messaging::KubeMessagingError;
pub use namespace::KubeNamespaceError;
pub use pod::KubePodError;
pub use podcontext::KubePodContextError;
pub use rbac::KubeRbacError;
pub use secret::KubeSecretError;
pub use service::KubeServiceError;

#[derive(Debug, Error)]
pub enum KubernetesError {
    #[error(transparent)]
    Client(#[from] KubeClientError),
    #[error(transparent)]
    Cluster(#[from] KubeClusterError),
    #[error(transparent)]
    ConfigMap(#[from] KubeConfigMapError),
    #[error(transparent)]
    CronJob(#[from] KubeCronJobError),
    #[error(transparent)]
    Deployment(#[from] KubeDeploymentError),
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
    Rbac(#[from] KubeRbacError),
    #[error(transparent)]
    Secret(#[from] KubeSecretError),
    #[error(transparent)]
    Service(#[from] KubeServiceError),
    #[error(transparent)]
    Naming(#[from] NamingError),
}

impl From<KubernetesError> for TdError {
    fn from(error: KubernetesError) -> Self {
        match error {
            KubernetesError::Client(inner) => TdError::new(inner),
            KubernetesError::Cluster(inner) => TdError::new(inner),
            KubernetesError::ConfigMap(inner) => TdError::new(inner),
            KubernetesError::CronJob(inner) => TdError::new(inner),
            KubernetesError::Deployment(inner) => TdError::new(inner),
            KubernetesError::Job(inner) => TdError::new(inner),
            KubernetesError::Kubeconfig(inner) => TdError::new(inner),
            KubernetesError::Messaging(inner) => TdError::new(inner),
            KubernetesError::Namespace(inner) => TdError::new(inner),
            KubernetesError::Pod(inner) => TdError::new(inner),
            KubernetesError::PodContext(inner) => TdError::new(inner),
            KubernetesError::Rbac(inner) => TdError::new(inner),
            KubernetesError::Secret(inner) => TdError::new(inner),
            KubernetesError::Service(inner) => TdError::new(inner),
            KubernetesError::Naming(inner) => TdError::new(inner),
        }
    }
}
