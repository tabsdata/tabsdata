//
// Copyright 2026 Tabs Data Inc.
//

use async_trait::async_trait;
use td_bridge::connection::ConnectionStore;
use td_error::TdError;
use td_objects::dxo::connection::{ConnectionRead, ConnectionUpsert};
use td_objects::types::basic::{ConnectionId, ProjectId};

#[derive(Default)]
pub struct KubernetesConnectionStore;

impl KubernetesConnectionStore {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl ConnectionStore for KubernetesConnectionStore {
    async fn upsert(
        &self,
        _project_id: &ProjectId,
        _connection_id: &ConnectionId,
        _connection: &ConnectionUpsert,
    ) -> Result<ConnectionRead, TdError> {
        todo!("kubernetes connection bridge")
    }

    async fn fetch(
        &self,
        _project_id: &ProjectId,
        _connection_id: &ConnectionId,
    ) -> Result<ConnectionRead, TdError> {
        todo!("kubernetes connection bridge")
    }

    async fn delete(
        &self,
        _project_id: &ProjectId,
        _connection_id: &ConnectionId,
    ) -> Result<(), TdError> {
        todo!("kubernetes connection bridge")
    }
}
