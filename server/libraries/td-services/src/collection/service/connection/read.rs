//
// Copyright 2026 Tabsdata Inc.
//

//! Read a source/destination collection's connection (dehydrated connection +
//! secret/var names, never values). ManageProject-gated ("CRUD sources and
//! destinations" in the permission model): the dehydrated JSON can carry
//! plain (non-Secret/Var) literals — URIs, hosts, anything the user didn't
//! mark as a secret — so it is more sensitive than collection metadata.
//!
//! Two phases, so no DB connection is held across the store's network call:
//! resolve + guard (own connection), then the store fetch.

use super::layer::{ensure_connectable, require_connection_id};
use std::sync::Arc;
use ta_services::factory::{FieldAccessor, ServiceFactory, service_factory};
use ta_services::service::TdService;
use td_authz::AuthzContext;
use td_authz::rbac::{ManageProject, Project, Rbac, Scope};
use td_bridge::BridgeProviderHandle;
use td_database::pool::TdPool;
use td_database::pool::scoped::Scoped;
use td_error::TdError;
use td_objects::dxo::collection::ActiveCollectionDB;
use td_objects::dxo::connection::ConnectionRead;
use td_objects::dxo::crudl::{ReadRequest, RequestContext};
use td_objects::rest_urls::CollectionParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{CollectionIdName, ConnectionId, ProjectId};
use td_queries::By;
use td_tower::ctx_service::{CtxResponse, RawOneshot, ReqCtx};
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;
use td_tower::service_provider::TdBoxService;
use tower::service_fn;
use tower::util::BoxCloneSyncService;

#[service_factory(
    name = ConnectionFetchService,
    request = ReadRequest<CollectionParam>,
    response = ConnectionId,
    connection = ConnectionProvider,
    scope = "Project",
    context = AuthzContext,
)]
fn fetch() {
    layers!(
        from_fn(With::<ReadRequest<CollectionParam>>::extract::<RequestContext>),
        from_fn(Scope::<Project>::set),
        from_fn(Rbac::<Project, ManageProject>::check),
        from_fn(With::<ReadRequest<CollectionParam>>::extract_name::<CollectionParam>),
        from_fn(With::<CollectionParam>::extract::<CollectionIdName>),
        from_fn(By::<CollectionIdName>::fetch_one::<ActiveCollectionDB>),
        from_fn(ensure_connectable),
        from_fn(require_connection_id),
    )
}

/// The routed read service: resolve + guard, then the store fetch with no DB
/// connection held.
pub struct ConnectionReadService {
    fetch: ConnectionFetchService,
    bridge: Arc<BridgeProviderHandle>,
}

impl ConnectionReadService {
    pub fn new(
        db: TdPool,
        authz_context: Arc<AuthzContext>,
        bridge: Arc<BridgeProviderHandle>,
    ) -> Self {
        Self {
            fetch: ConnectionFetchService::new(db, authz_context),
            bridge,
        }
    }

    #[cfg(feature = "test-utils")]
    pub fn with_defaults(db: TdPool) -> Self {
        Self::new(
            db,
            Arc::new(AuthzContext::default()),
            Arc::new(BridgeProviderHandle::default()),
        )
    }
}

impl<C> ServiceFactory<C> for ConnectionReadService
where
    TdPool: FieldAccessor<C>,
    Arc<AuthzContext>: FieldAccessor<C>,
    Arc<BridgeProviderHandle>: FieldAccessor<C>,
{
    type Service = Self;

    fn build(ctx: &C) -> Self {
        Self::new(
            TdPool::get_field(ctx),
            Arc::<AuthzContext>::get_field(ctx),
            Arc::<BridgeProviderHandle>::get_field(ctx),
        )
    }
}

impl TdService for ConnectionReadService {
    type Request = Scoped<ReadRequest<CollectionParam>, ProjectId>;
    type Response = ConnectionRead;
    type Error = TdError;

    fn service(&self) -> TdBoxService<Self::Request, Self::Response, Self::Error> {
        let fetch = self.fetch.service();
        let bridge = self.bridge.clone();
        BoxCloneSyncService::new(service_fn(move |request: Self::Request| {
            let fetch = fetch.clone();
            let bridge = bridge.clone();
            async move {
                let project = request.project;
                let connection_id = fetch.raw_oneshot(request).await?;
                let mut read = bridge
                    .connection_store()
                    .fetch(&project, &connection_id)
                    .await?;
                read.id = Some(connection_id);
                Ok(CtxResponse::new(read, ReqCtx::default()))
            }
        }))
    }

    #[cfg(feature = "test_tower_metadata")]
    async fn metadata(&self) -> td_tower::metadata::Metadata {
        self.fetch.metadata().await
    }
}

// The orchestrator's metadata is its fetch pipeline (the store phase is
// hand-rolled around it). The behavior tests live in `upsert.rs`, which
// reads back everything it writes.
#[cfg(all(test, feature = "test_tower_metadata"))]
mod tests {
    use super::*;

    #[td_test::test(db)]
    async fn test_tower_metadata_connection_read(db: TdPool) {
        use td_tower::metadata::type_of_val;

        ConnectionReadService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<Scoped<ReadRequest<CollectionParam>, ProjectId>, ConnectionId>(&[
                type_of_val(&With::<ReadRequest<CollectionParam>>::extract::<RequestContext>),
                type_of_val(&Scope::<Project>::set),
                type_of_val(&Rbac::<Project, ManageProject>::check),
                type_of_val(&With::<ReadRequest<CollectionParam>>::extract_name::<CollectionParam>),
                type_of_val(&With::<CollectionParam>::extract::<CollectionIdName>),
                type_of_val(&By::<CollectionIdName>::fetch_one::<ActiveCollectionDB>),
                type_of_val(&ensure_connectable),
                type_of_val(&require_connection_id),
            ]);
    }
}
