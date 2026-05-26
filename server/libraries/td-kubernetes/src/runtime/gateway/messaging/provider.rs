//
// Copyright 2026 Tabs Data Inc.
//

use std::sync::Arc;

use async_trait::async_trait;
use kube::Client;
use td_common::server::{
    OrchestratorMessageQueueHandle, OrchestratorMessagingProvider, QueueError,
};

use crate::base::error::kubernetes::KubernetesError;
use crate::client::tdkpodcontext::TdkPodContext;
use crate::runtime::gateway::messaging::queue::KubernetesOrchestratorMessageQueue;
use crate::runtime::gateway::messaging::template::PodTemplateIndex;
use crate::runtime::reporting::build_incluster_kube_client;

pub struct KubernetesOrchestratorMessagingProvider {
    client: Arc<Client>,
    namespace: String,
}

impl KubernetesOrchestratorMessagingProvider {
    pub fn new(client: Arc<Client>, namespace: String) -> Self {
        Self { client, namespace }
    }

    pub async fn from_pod_environment() -> Result<Self, KubernetesError> {
        let pod_context = TdkPodContext::from_environment()?;
        let client = build_incluster_kube_client().await?;
        Ok(Self::new(Arc::new(client), pod_context.namespace))
    }
}

#[async_trait]
impl OrchestratorMessagingProvider for KubernetesOrchestratorMessagingProvider {
    async fn orchestrator_message_queue(
        &self,
    ) -> Result<OrchestratorMessageQueueHandle, QueueError> {
        let templates = PodTemplateIndex::load(&self.client, &self.namespace)
            .await
            .map_err(|error| QueueError::BackendError {
                cause: error.to_string(),
            })?;
        let queue = KubernetesOrchestratorMessageQueue::new(
            Arc::clone(&self.client),
            self.namespace.clone(),
            templates,
        );
        Ok(OrchestratorMessageQueueHandle::new(Arc::new(queue)))
    }
}
