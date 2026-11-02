//
// Copyright 2026 Tabsdata Inc.
//

//! In-memory connection store for tests — the [`TestBridgeProvider`]'s
//! default, so service tests that create source/destination collections get
//! a working store without wiring one.
//!
//! [`TestBridgeProvider`]: crate::test::TestBridgeProvider

use super::{ConnectionStore, hydrated_of, merge_kept, read_of};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Mutex;
use td_error::{ApiError, TdError, api_error};
use td_objects::dxo::connection::{ConnectionHydrated, ConnectionRead, ConnectionUpsert};
use td_objects::types::basic::{CollectionId, CollectionName, ConnectionId, ProjectId};

#[derive(Default)]
pub struct InMemoryConnectionStore {
    entries: Mutex<HashMap<(ProjectId, ConnectionId), ConnectionUpsert>>,
}

#[async_trait]
impl ConnectionStore for InMemoryConnectionStore {
    async fn upsert(
        &self,
        project_id: &ProjectId,
        _collection_id: &CollectionId,
        _collection: &CollectionName,
        connection_id: &ConnectionId,
        connection: &ConnectionUpsert,
    ) -> Result<ConnectionRead, TdError> {
        let mut entries = self.entries.lock().unwrap();
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

    async fn fetch_hydrated(
        &self,
        project_id: &ProjectId,
        connection_id: &ConnectionId,
    ) -> Result<ConnectionHydrated, TdError> {
        match self
            .entries
            .lock()
            .unwrap()
            .get(&(*project_id, *connection_id))
        {
            Some(entry) => Ok(hydrated_of(entry)),
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
