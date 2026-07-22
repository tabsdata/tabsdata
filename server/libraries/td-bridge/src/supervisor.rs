//
// Copyright 2026 Tabsdata Inc.
//

//! The supervisor-orchestrator [`BridgeProvider`]: a composition of the
//! local impls — local workspace logs, the etc-folder catalog, the
//! file-based message queue, and the repository-file connection store.

use crate::catalog::ServerCatalog;
use crate::catalog::etc_folder::EtcFolderStore;
use crate::connection::ConnectionStore;
use crate::connection::file::FileConnectionStore;
use crate::messaging::OrchestratorMessageQueue;
use crate::messaging::file_queue::FileSupervisorMessageQueue;
use crate::worker::WorkerHandler;
use crate::worker::workspace::WorkspaceWorkerHandler;
use crate::{BridgeProvider, Orchestrator};
use td_error::TdError;

pub struct SupervisorBridgeProvider {
    server_catalog: EtcFolderStore,
    worker_handler: WorkspaceWorkerHandler,
    connection_store: FileConnectionStore,
    message_queue: FileSupervisorMessageQueue,
}

impl SupervisorBridgeProvider {
    pub async fn new() -> Result<Self, TdError> {
        Ok(Self {
            server_catalog: EtcFolderStore::new().await?,
            worker_handler: WorkspaceWorkerHandler,
            connection_store: FileConnectionStore::new(),
            message_queue: FileSupervisorMessageQueue::new().await?,
        })
    }
}

impl BridgeProvider for SupervisorBridgeProvider {
    fn orchestrator(&self) -> Orchestrator {
        Orchestrator::Supervisor
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
