//
// Copyright 2026 Tabsdata Inc.
//

//! The connection store holds nothing for a deleted collection, whatever
//! connection write was in flight when it was deleted.

use crate::collection::services::connection::test_support::{
    collection_param, create_request, linked_connection, upsert_request, upsert_service,
};
use async_trait::async_trait;
use std::sync::{Arc, Mutex};
use td_bridge::connection::ConnectionStore;
use td_bridge::connection::memory::InMemoryConnectionStore;
use td_database::pool::TdPool;
use td_database::pool::scoped::ScopedExt;
use td_error::TdError;
use td_objects::dxo::connection::{ConnectionHydrated, ConnectionRead, ConnectionUpsert};
use td_objects::dxo::crudl::RequestContext;
use td_objects::types::basic::{
    AccessTokenId, CollectionId, CollectionName, ConnectionId, GroupName, ProjectId, UserId,
};
use td_queries::test_utils::SeedEntry;
use tokio::sync::oneshot;

type Pause = (oneshot::Sender<()>, oneshot::Receiver<()>);

/// The in-memory store, with the next upsert held until released.
#[derive(Clone, Default)]
struct PausingStore {
    inner: Arc<InMemoryConnectionStore>,
    pause: Arc<Mutex<Option<Pause>>>,
}

impl PausingStore {
    fn pause_next_upsert(&self) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (reached, reached_rx) = oneshot::channel();
        let (release, release_rx) = oneshot::channel();
        *self.pause.lock().unwrap() = Some((reached, release_rx));
        (reached_rx, release)
    }
}

#[async_trait]
impl ConnectionStore for PausingStore {
    async fn upsert(
        &self,
        project_id: &ProjectId,
        connection_id: &ConnectionId,
        collection: &CollectionName,
        collection_id: &CollectionId,
        connection: &ConnectionUpsert,
    ) -> Result<ConnectionRead, TdError> {
        let pause = self.pause.lock().unwrap().take();
        if let Some((reached, release)) = pause {
            let _ = reached.send(());
            let _ = release.await;
        }
        self.inner
            .upsert(
                project_id,
                connection_id,
                collection,
                collection_id,
                connection,
            )
            .await
    }

    async fn fetch(
        &self,
        project_id: &ProjectId,
        connection_id: &ConnectionId,
    ) -> Result<ConnectionRead, TdError> {
        self.inner.fetch(project_id, connection_id).await
    }

    async fn fetch_hydrated(
        &self,
        project_id: &ProjectId,
        connection_id: &ConnectionId,
    ) -> Result<ConnectionHydrated, TdError> {
        self.inner.fetch_hydrated(project_id, connection_id).await
    }

    async fn delete(
        &self,
        project_id: &ProjectId,
        connection_id: &ConnectionId,
    ) -> Result<(), TdError> {
        self.inner.delete(project_id, connection_id).await
    }
}

/// An update of source collection `src`'s connection has resolved the
/// collection and is writing the store when `src` is deleted, which drops the
/// store content after its commit. When the update's write lands, the store
/// must not hold `src`'s connection and secrets again (on Kubernetes, a
/// configmap and secret nothing will ever remove).
#[td_test::test(db)]
async fn test_a_connection_update_racing_a_collection_delete_leaves_no_secrets(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let store = PausingStore::default();
    upsert_service(&db, store.clone())
        .create_collection(create_request(p.id, "src", Some(GroupName::source())))
        .await?;
    let connection = linked_connection(&db, p.id, "src")
        .await
        .expect("a source collection has a connection");

    let (reached, release) = store.pause_next_upsert();
    let updater = upsert_service(&db, store.clone());
    let updating =
        tokio::spawn(async move { updater.connection_upsert(upsert_request(p.id, "src")).await });
    reached.await.expect("the update never reached the store");

    upsert_service(&db, store.clone())
        .delete_collection(
            RequestContext::with(AccessTokenId::default(), UserId::admin())
                .delete(collection_param("src"))
                .in_project(p.id),
        )
        .await?;
    let _ = release.send(());
    let _ = updating.await.expect("update task");

    let left = store.fetch(&p.id, &connection.base.id).await;
    assert!(
        left.is_err(),
        "the deleted collection's connection is back in the store: {left:?}"
    );
    Ok(())
}
