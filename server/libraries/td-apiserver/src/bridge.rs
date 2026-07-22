//
// Copyright 2025 Tabsdata Inc.
//

//! The server's bridge-provider factory: builds the bridge for whichever
//! orchestrator the apiserver config names. It lives here rather than in
//! `td-bridge` because it constructs the Kubernetes provider from
//! `td-kubernetes`, which itself depends on `td-bridge` (so `td-bridge` cannot
//! depend back on it). This crate is the one place that sees every backend.

use async_trait::async_trait;
use std::error::Error;
use std::sync::Arc;
use td_bridge::{BridgeProvider, BridgeProviderFactory, Orchestrator, SupervisorBridgeProvider};
use td_kubernetes::kube::KubernetesBridgeProvider;

/// Builds the bridge for whichever orchestrator the apiserver config names.
pub(crate) struct ServerBridgeFactory;

#[async_trait]
impl BridgeProviderFactory for ServerBridgeFactory {
    async fn build(
        &self,
        orchestrator: Orchestrator,
    ) -> Result<Arc<dyn BridgeProvider>, Box<dyn Error + Send + Sync>> {
        match orchestrator {
            Orchestrator::Supervisor => Ok(Arc::new(SupervisorBridgeProvider::new().await?)),
            Orchestrator::Kubernetes => Ok(Arc::new(
                KubernetesBridgeProvider::from_pod_environment().await?,
            )),
        }
    }
}
