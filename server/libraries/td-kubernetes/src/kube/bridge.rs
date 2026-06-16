//
// Copyright 2026 Tabs Data Inc.
//

pub mod connection;
pub mod helpers;

use std::sync::Arc;

use async_trait::async_trait;
use serde::Serialize;
use td_bridge::catalog::{EtcContent, EtcError, ServerCatalog};
use td_bridge::connection::ConnectionStore;
use td_bridge::messaging::OrchestratorMessageQueue;
use td_bridge::worker::{LogBridgeError, WorkerHandler, WorkerLogStream, WorkerLogTarget};
use td_bridge::{BridgeProvider, Orchestrator};
use td_common::crypto::install_crypto_provider;

use crate::base::error::kubernetes::KubernetesError;
use crate::client::session::pod::PodContext;
use crate::kube::bridge::connection::KubernetesConnectionStore;
use crate::kube::bridge::helpers::log::stream_worker_logs;
use crate::kube::context::KubeContext;
use crate::runtime::gateway::messaging::messenger::queue::KubernetesOrchestratorMessageQueue;
use crate::runtime::gateway::messaging::messenger::template::PodTemplateIndex;
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
    server_catalog: KubernetesServerCatalog,
    worker_handler: KubernetesWorkerHandler,
    connection_store: KubernetesConnectionStore,
    message_queue: KubernetesOrchestratorMessageQueue,
}

impl KubernetesBridgeProvider {
    pub async fn new(context: KubeContext) -> Result<Self, KubernetesError> {
        let server_catalog = KubernetesServerCatalog::new();
        let worker_handler = KubernetesWorkerHandler {
            context: context.clone(),
        };
        let connection_store = KubernetesConnectionStore::new();
        let templates = PodTemplateIndex::load(&context).await?;
        let message_queue = KubernetesOrchestratorMessageQueue::new(context.clone(), templates);
        Ok(Self {
            context,
            server_catalog,
            worker_handler,
            connection_store,
            message_queue,
        })
    }

    pub async fn from_pod_environment() -> Result<Self, KubernetesError> {
        install_crypto_provider();
        let pod_context = PodContext::from_environment()?;
        let client = build_pod_kube_client().await?;
        Self::new(KubeContext::new(Arc::new(client), pod_context.namespace)).await
    }

    pub fn context(&self) -> &KubeContext {
        &self.context
    }
}

impl BridgeProvider for KubernetesBridgeProvider {
    fn orchestrator(&self) -> Orchestrator {
        Orchestrator::Kubernetes
    }

    fn server_catalog(&self) -> &dyn ServerCatalog {
        &self.server_catalog
    }

    fn worker_handler(&self) -> &dyn WorkerHandler {
        &self.worker_handler
    }

    fn connection_store(&self) -> &dyn ConnectionStore {
        &self.connection_store
    }

    fn message_queue(&self) -> &dyn OrchestratorMessageQueue {
        &self.message_queue
    }
}

/// Streams pod logs through the kube client.
pub struct KubernetesWorkerHandler {
    context: KubeContext,
}

#[async_trait]
impl WorkerHandler for KubernetesWorkerHandler {
    async fn logs(&self, target: &WorkerLogTarget) -> Result<WorkerLogStream, LogBridgeError> {
        Ok(stream_worker_logs(&self.context, &target.worker_id, false))
    }

    async fn logs_tail(&self, target: &WorkerLogTarget) -> Result<WorkerLogStream, LogBridgeError> {
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
