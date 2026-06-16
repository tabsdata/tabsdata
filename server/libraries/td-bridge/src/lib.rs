//
// Copyright 2026 Tabs Data Inc.
//

//! The orchestrator bridge: the traits the server uses to talk to whatever
//! runs it (local supervisor, Kubernetes, …). [`BridgeProvider`] vends a
//! store per concern — server catalog, [`worker`] store, collection
//! [`connection`] store. Implementations live with their backend (e.g.
//! `td-kubernetes`); [`SupervisorBridgeProvider`] is the local default.

pub mod catalog;
pub mod connection;
pub mod messaging;
#[cfg(feature = "test-utils")]
pub mod test;
pub mod worker;

mod supervisor;

pub use supervisor::SupervisorBridgeProvider;

use crate::catalog::ServerCatalog;
use crate::connection::ConnectionStore;
use crate::messaging::OrchestratorMessageQueue;
use crate::worker::WorkerHandler;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::ops::Deref;
use std::sync::Arc;
use strum::{AsRefStr, Display, EnumString};

/// Providers build their stores once at construction (fallible setup happens
/// there); the accessors borrow the held instance — a store cannot be minted
/// per call. To own a store beyond a call, clone the [`BridgeProviderHandle`].
/// Which orchestrator runs this server — the selector the config names and
/// the factory builds the [`BridgeProvider`] from.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, EnumString, AsRefStr, Display,
)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
#[derive(Default)]
pub enum Orchestrator {
    #[default]
    Supervisor,
    Kubernetes,
}

pub trait BridgeProvider: Send + Sync {
    /// Which orchestrator this provider talks to — the single source of truth
    /// for code that varies by orchestrator (e.g. worker message shape).
    fn orchestrator(&self) -> Orchestrator;

    /// Catalog the server reads interchange files through.
    fn server_catalog(&self) -> &dyn ServerCatalog;

    /// Store the worker services read worker logs through.
    fn worker_handler(&self) -> &dyn WorkerHandler;

    /// Store the connection services persist/read collection connections
    /// through. An unconfigured store (every call answers 503) is the
    /// "no backend" choice.
    fn connection_store(&self) -> &dyn ConnectionStore;

    /// Queue the scheduler puts worker requests on for the orchestrator to
    /// execute.
    fn message_queue(&self) -> &dyn OrchestratorMessageQueue;
}

#[derive(Clone)]
pub struct BridgeProviderHandle(Arc<dyn BridgeProvider>);

impl BridgeProviderHandle {
    pub fn new(provider: Arc<dyn BridgeProvider>) -> Self {
        Self(provider)
    }
}

impl Deref for BridgeProviderHandle {
    type Target = Arc<dyn BridgeProvider>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Builds the orchestrator's [`BridgeProvider`]. Implemented by the binary
/// crate (the only place that sees every backend); the apiserver receives it
/// and calls it once, after parsing which orchestrator the config names.
#[async_trait]
pub trait BridgeProviderFactory: Send + Sync {
    async fn build(
        &self,
        orchestrator: Orchestrator,
    ) -> Result<Arc<dyn BridgeProvider>, Box<dyn Error + Send + Sync>>;
}
