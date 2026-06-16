//
// Copyright 2026 Tabs Data Inc.
//

//! In-memory connection store for tests — the [`TestBridgeProvider`]'s
//! default, so service tests that create source/destination collections get
//! a working store without wiring one.
//!
//! [`TestBridgeProvider`]: crate::test::TestBridgeProvider

use super::{ConnectionStore, merge_kept, read_of};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Mutex;
use td_error::{ApiError, TdError, api_error};
use td_objects::dxo::connection::{ConnectionRead, ConnectionUpsert};
use td_objects::types::basic::{ConnectionId, ProjectId};

#[derive(Default)]
pub struct InMemoryConnectionStore {
    entries: Mutex<HashMap<(ProjectId, ConnectionId), ConnectionUpsert>>,
}

#[async_trait]
impl ConnectionStore for InMemoryConnectionStore {
    async fn upsert(
        &self,
        project_id: &ProjectId,
        connection_id: &ConnectionId,
        connection: &ConnectionUpsert,
    ) -> Result<ConnectionRead, TdError> {
        let mut entries = self.entries.lock().unwrap();
        let name = connection
            .connection
            .as_ref()
            .and_then(|d| d.get("name"))
            .and_then(|n| n.as_str());
        if let Some(name) = name
            && entries.iter().any(|((p, id), stored)| {
                p == project_id
                    && id != connection_id
                    && stored
                        .connection
                        .as_ref()
                        .and_then(|d| d.get("name"))
                        .and_then(|n| n.as_str())
                        == Some(name)
            })
        {
            return Err(api_error!(
                ApiError::InputError,
                "the connection name '{name}' belongs to another collection's connection"
            ));
        }
        let merged = merge_kept(connection, entries.get(&(*project_id, *connection_id)))?;
        let read = read_of(&merged);
        entries.insert((*project_id, *connection_id), merged);
        Ok(read)
    }

    async fn fetch(
        &self,
        project_id: &ProjectId,
        connection_id: &ConnectionId,
    ) -> Result<ConnectionRead, TdError> {
        match self
            .entries
            .lock()
            .unwrap()
            .get(&(*project_id, *connection_id))
        {
            Some(entry) => Ok(read_of(entry)),
            None => Err(api_error!(
                ApiError::NotFound,
                "the collection has no connection configured"
            )),
        }
    }

    async fn delete(
        &self,
        project_id: &ProjectId,
        connection_id: &ConnectionId,
    ) -> Result<(), TdError> {
        self.entries
            .lock()
            .unwrap()
            .remove(&(*project_id, *connection_id));
        Ok(())
    }
}
