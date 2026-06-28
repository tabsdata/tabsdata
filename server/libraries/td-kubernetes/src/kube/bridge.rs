//
// Copyright 2026 Tabs Data Inc.
//

pub mod helpers;

use std::sync::Arc;

use td_bridge::catalog::ServerCatalog;
use td_bridge::connection::ConnectionStore;
use td_bridge::messaging::OrchestratorMessageQueue;
use td_bridge::worker::WorkerHandler;
use td_bridge::{BridgeProvider, Orchestrator};
use td_common::crypto::install_crypto_provider;

use crate::base::error::kubernetes::KubernetesError;
use crate::client::session::pod::PodContext;
use crate::kube::context::KubeContext;
use crate::runtime::gateway::catalog::KubernetesServerCatalog;
use crate::runtime::gateway::datasource::KubernetesConnectionStore;
use crate::runtime::gateway::messaging::KubernetesOrchestratorMessageQueue;
use crate::runtime::gateway::messaging::messenger::template::PodTemplateIndex;
use crate::runtime::gateway::workload::KubernetesWorkerHandler;
use crate::runtime::reporting::build_pod_kube_client;

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
        let worker_handler = KubernetesWorkerHandler::new(context.clone());
        let connection_store = KubernetesConnectionStore::new(context.clone());
        let pod_templates = PodTemplateIndex::load(&context).await?;
        let message_queue = KubernetesOrchestratorMessageQueue::new(context.clone(), pod_templates);
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
