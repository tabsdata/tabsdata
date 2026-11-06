//
// Copyright 2026 Tabsdata Inc.
//

//! Test bridge: backs `BridgeProviderHandle::default()` in `with_defaults`
//! test contexts. Every backend defaults to an inert one (empty catalog, noop
//! stores, in-memory connection store); tests that exercise a backend plug a
//! real or mock one in via the
//! `with_*` setters. Never part of production wiring.

use crate::catalog::{EtcContent, EtcError, ServerCatalog};
use crate::connection::ConnectionStore;
use crate::connection::memory::InMemoryConnectionStore;
use crate::messaging::OrchestratorMessageQueue;
use crate::messaging::memory::InMemoryOrchestratorMessageQueue;
use crate::observer::{ObserverError, WorkloadObservation, WorkloadObserver};
use crate::secret::SecretStore;
use crate::secret::memory::InMemorySecretStore;
use crate::validator::{ValidatorError, WorkloadDefinition, WorkloadValidator};
use crate::worker::{LogBridgeError, WorkerHandler, WorkerLogStream, WorkerLogTarget};
use crate::{BridgeProvider, BridgeProviderHandle, Orchestrator};
use async_trait::async_trait;
use std::sync::Arc;

pub struct TestBridgeProvider {
    orchestrator: Orchestrator,
    server_catalog: EmptyServerCatalog,
    worker_handler: Box<dyn WorkerHandler>,
    connection_store: Box<dyn ConnectionStore>,
    secret_store: Box<dyn SecretStore>,
    message_queue: Box<dyn OrchestratorMessageQueue>,
    workload_observer: Option<Box<dyn WorkloadObserver>>,
    workload_validator: Option<Box<dyn WorkloadValidator>>,
}

impl TestBridgeProvider {
    pub fn with_connection_store(mut self, store: impl ConnectionStore + 'static) -> Self {
        self.connection_store = Box::new(store);
        self
    }

    pub fn with_secret_store(mut self, store: impl SecretStore + 'static) -> Self {
        self.secret_store = Box::new(store);
        self
    }

    pub fn with_worker_handler(mut self, store: impl WorkerHandler + 'static) -> Self {
        self.worker_handler = Box::new(store);
        self
    }

    pub fn with_message_queue(mut self, queue: impl OrchestratorMessageQueue + 'static) -> Self {
        self.message_queue = Box::new(queue);
        self
    }

    pub fn with_orchestrator(mut self, orchestrator: Orchestrator) -> Self {
        self.orchestrator = orchestrator;
        self
    }

    pub fn with_workload_observer(mut self, observer: impl WorkloadObserver + 'static) -> Self {
        self.workload_observer = Some(Box::new(observer));
        self
    }

    pub fn with_workload_validator(mut self, validator: impl WorkloadValidator + 'static) -> Self {
        self.workload_validator = Some(Box::new(validator));
        self
    }
}

pub struct RefusingWorkloadValidator;

impl WorkloadValidator for RefusingWorkloadValidator {
    fn validate(&self, _: &WorkloadDefinition<'_>) -> Result<(), ValidatorError> {
        Err(ValidatorError::Unrunnable {
            worker: "test".to_string(),
            cause: "refused by the test validator".to_string(),
        })
    }
}

/// Observer that answers every `observe` with the same fixed observation.
#[derive(Default)]
pub struct StaticWorkloadObserver(pub WorkloadObservation);

#[async_trait]
impl WorkloadObserver for StaticWorkloadObserver {
    async fn observe(&self) -> Result<WorkloadObservation, ObserverError> {
        Ok(self.0.clone())
    }
}

impl Default for TestBridgeProvider {
    fn default() -> Self {
        Self {
            orchestrator: Orchestrator::default(),
            server_catalog: EmptyServerCatalog,
            worker_handler: Box::new(NoopWorkerHandler),
            connection_store: Box::new(InMemoryConnectionStore::default()),
            secret_store: Box::new(InMemorySecretStore::default()),
            message_queue: Box::new(InMemoryOrchestratorMessageQueue::default()),
            workload_observer: None,
            workload_validator: None,
        }
    }
}

impl BridgeProvider for TestBridgeProvider {
    fn orchestrator(&self) -> Orchestrator {
        self.orchestrator
    }

    fn server_catalog(&self) -> &dyn ServerCatalog {
        &self.server_catalog
    }

    fn worker_handler(&self) -> &dyn WorkerHandler {
        self.worker_handler.as_ref()
    }

    fn connection_store(&self) -> &dyn ConnectionStore {
        self.connection_store.as_ref()
    }

    fn secret_store(&self) -> &dyn SecretStore {
        self.secret_store.as_ref()
    }

    fn message_queue(&self) -> &dyn OrchestratorMessageQueue {
        self.message_queue.as_ref()
    }

    fn workload_observer(&self) -> Option<&dyn WorkloadObserver> {
        self.workload_observer.as_deref()
    }

    fn workload_validator(&self) -> Option<&dyn WorkloadValidator> {
        self.workload_validator.as_deref()
    }
}

impl Default for BridgeProviderHandle {
    fn default() -> Self {
        Self::new(Arc::new(TestBridgeProvider::default()))
    }
}

/// Catalog with nothing in it.
struct EmptyServerCatalog;

#[async_trait]
impl ServerCatalog for EmptyServerCatalog {
    async fn read(&self, _: &EtcContent) -> Result<Option<Vec<u8>>, EtcError> {
        Ok(None)
    }
}

/// No worker log backend — every operation fails as unconfigured.
struct NoopWorkerHandler;

#[async_trait]
impl WorkerHandler for NoopWorkerHandler {
    async fn logs(&self, _: &WorkerLogTarget) -> Result<WorkerLogStream, LogBridgeError> {
        Err(unconfigured())
    }

    async fn logs_tail(&self, _: &WorkerLogTarget) -> Result<WorkerLogStream, LogBridgeError> {
        Err(unconfigured())
    }
}

fn unconfigured() -> LogBridgeError {
    LogBridgeError::Backend {
        cause: "no worker log backend configured".to_string(),
    }
}
