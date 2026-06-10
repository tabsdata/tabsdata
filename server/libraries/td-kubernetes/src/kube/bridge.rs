//
// Copyright 2026 Tabs Data Inc.
//

pub mod helpers;

use std::sync::Arc;

use async_trait::async_trait;
use serde::Serialize;
use td_common::crypto::install_crypto_provider;
use td_common::server::{
    BridgeProvider, EtcContent, EtcError, LogBridgeError, ServerCatalog, ServerCatalogHandle,
    WorkerLogStream, WorkerLogTarget,
};

use crate::base::error::kubernetes::KubernetesError;
use crate::client::session::pod::PodContext;
use crate::kube::bridge::helpers::log::stream_worker_logs;
use crate::kube::context::KubeContext;
use crate::runtime::reporting::build_pod_kube_client;

#[derive(Serialize)]
struct PythonVersions {
    versions: Vec<String>,
}

impl PythonVersions {
    fn supported() -> Self {
        Self {
            versions: vec!["3.12.12".to_owned(), "3.12.13".to_owned()],
        }
    }
}

pub struct KubernetesBridgeProvider {
    context: KubeContext,
}

impl KubernetesBridgeProvider {
    pub fn new(context: KubeContext) -> Self {
        Self { context }
    }

    pub async fn from_pod_environment() -> Result<Self, KubernetesError> {
        install_crypto_provider();
        let pod_context = PodContext::from_environment()?;
        let client = build_pod_kube_client().await?;
        Ok(Self::new(KubeContext::new(
            Arc::new(client),
            pod_context.namespace,
        )))
    }

    pub fn context(&self) -> &KubeContext {
        &self.context
    }
}

#[async_trait]
impl BridgeProvider for KubernetesBridgeProvider {
    async fn server_catalog(&self) -> Result<ServerCatalogHandle, EtcError> {
        Ok(ServerCatalogHandle::new(Arc::new(
            KubernetesServerCatalog::new(),
        )))
    }

    async fn worker_logs(
        &self,
        target: &WorkerLogTarget,
    ) -> Result<WorkerLogStream, LogBridgeError> {
        Ok(stream_worker_logs(&self.context, &target.worker_id, false))
    }

    async fn worker_logs_tail(
        &self,
        target: &WorkerLogTarget,
    ) -> Result<WorkerLogStream, LogBridgeError> {
        Ok(stream_worker_logs(&self.context, &target.worker_id, true))
    }
}

pub struct KubernetesServerCatalog;

impl KubernetesServerCatalog {
    pub fn new() -> Self {
        Self
    }
}

impl Default for KubernetesServerCatalog {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ServerCatalog for KubernetesServerCatalog {
    async fn read(&self, content: &EtcContent) -> Result<Option<Vec<u8>>, EtcError> {
        match content {
            EtcContent::AllowedPythonVersions_yaml
            | EtcContent::AvailablePythonVersions_yaml
            | EtcContent::ValidPythonVersions_yaml => {
                let bytes = serde_yaml::to_string(&PythonVersions::supported())
                    .map_err(|cause| EtcError::SerializationError {
                        content: *content,
                        cause: cause.to_string(),
                    })?
                    .into_bytes();
                Ok(Some(bytes))
            }
            EtcContent::ServerVersion_yaml | EtcContent::ServerBuildManifest_yaml => Ok(None),
        }
    }
}
