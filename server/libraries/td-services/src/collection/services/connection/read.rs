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

use super::{ensure_connectable, require_connection_id};
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{ManageProject, Project};
use td_bridge::BridgeProviderHandle;
use td_database::access::{DbPool, InProject};
use td_error::TdError;
use td_objects::dxo::collection::ActiveCollectionDB;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::connection::ConnectionRead;
use td_objects::dxo::crudl::{ReadRequest, WithinProject};
use td_queries::LiveEntry;

#[service_factory]
async fn connection_read(
    authz: Ctx<AuthzContext>,
    bridge: Ctx<BridgeProviderHandle>,
    db: DbPool<InProject>,
    request: WithinProject<ReadRequest<CollectionParam>>,
) -> Result<ConnectionRead, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, param) = request.into_parts();

    // Resolve + guard on a short connection of its own.
    let connection_id = {
        let mut conn = db.read().await?;
        let grant = authz
            .require::<Project, ManageProject>(&mut conn, &ctx, &project)
            .await?;
        let collection: ActiveCollectionDB =
            conn.get(grant).live().fetch_one(&param.collection).await?;
        ensure_connectable(&collection)?;
        require_connection_id(&collection)?
    };

    // The store fetch, with no DB connection held.
    let mut read = bridge
        .connection_store()
        .fetch(&project, &connection_id)
        .await?;
    read.id = Some(connection_id);
    Ok(read)
}

// The orchestrator's metadata is its fetch pipeline (the store phase is
// hand-rolled around it). The behavior tests live in `upsert.rs`, which
// reads back everything it writes.
