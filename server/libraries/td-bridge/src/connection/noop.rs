//
// Copyright 2026 Tabs Data Inc.
//

use super::ConnectionStore;
use async_trait::async_trait;
use http::StatusCode;
use td_error::{TdError, api_error};
use td_objects::dxo::connection::{ConnectionRead, ConnectionUpsert};
use td_objects::types::basic::{ConnectionId, ProjectId};

/// No backend configured — every operation fails as unconfigured.
pub struct NoopConnectionStore;

#[async_trait]
impl ConnectionStore for NoopConnectionStore {
    async fn upsert(
        &self,
        _: &ProjectId,
        _: &ConnectionId,
        _: &ConnectionUpsert,
    ) -> Result<ConnectionRead, TdError> {
        Err(unconfigured())
    }

    async fn fetch(&self, _: &ProjectId, _: &ConnectionId) -> Result<ConnectionRead, TdError> {
        Err(unconfigured())
    }

    async fn delete(&self, _: &ProjectId, _: &ConnectionId) -> Result<(), TdError> {
        Err(unconfigured())
    }
}

fn unconfigured() -> TdError {
    api_error!(
        status = StatusCode::SERVICE_UNAVAILABLE,
        "the connection store is not configured"
    )
}
