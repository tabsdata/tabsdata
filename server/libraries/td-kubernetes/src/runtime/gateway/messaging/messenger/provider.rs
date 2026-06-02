//
// Copyright 2026 Tabs Data Inc.
//

use std::sync::Arc;

use async_trait::async_trait;
use td_common::server::{
    OrchestratorMessageQueueHandle, OrchestratorMessagingProvider, QueueError,
};

use crate::base::error::kubernetes::KubernetesError;
use crate::client::session::pod::PodContext;
use crate::kube::api::client::install_crypto_provider;
use crate::kube::context::KubeContext;
use crate::runtime::gateway::messaging::messenger::queue::KubernetesOrchestratorMessageQueue;
use crate::runtime::gateway::messaging::messenger::template::PodTemplateIndex;
use crate::runtime::reporting::build_pod_kube_client;

pub struct KubernetesOrchestratorMessagingProvider {
    context: KubeContext,
}

impl KubernetesOrchestratorMessagingProvider {
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
}

#[async_trait]
impl OrchestratorMessagingProvider for KubernetesOrchestratorMessagingProvider {
    async fn orchestrator_message_queue(
        &self,
    ) -> Result<OrchestratorMessageQueueHandle, QueueError> {
        let templates = PodTemplateIndex::load(&self.context)
            .await
            .map_err(|error| QueueError::BackendError {
                cause: error.to_string(),
            })?;
        let queue = KubernetesOrchestratorMessageQueue::new(self.context.clone(), templates);
        Ok(OrchestratorMessageQueueHandle::new(Arc::new(queue)))
    }
}
