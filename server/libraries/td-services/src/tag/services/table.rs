//
// Copyright 2026 Tabsdata Inc.
//

use crate::collection::resolve_collection_id;
use crate::tag::user::{delete_user_tag, entity_tags, set_user_tag};
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{DevelopProject, Project, ViewProject};
use td_database::access::{AccessMode, GatedConnection, InProject, RO, RW};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::and;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::crudl::{CreateRequest, DeleteRequest, ReadRequest, WithinProject};
use td_objects::dxo::table::{TableDBWithNames, TableParam};
use td_objects::dxo::tag::{Tag, TagParam, TagSet};
use td_objects::types::basic::{TagEntityId, TagEntityType};
use td_queries::Queries;

async fn table<M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, InProject, M>,
    collection: &CollectionParam,
    table: &TableParam,
) -> Result<TagEntityId, TdError> {
    let table: TableDBWithNames = conn
        .scd2()
        .fetch_one(and!(&collection_id, &table.table))
        .await?;
    TagEntityId::try_from(table.base.base.table_id)
}

#[service_factory]
async fn list_table_tags(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO, InProject>,
    request: WithinProject<ReadRequest<(CollectionParam, TableParam)>>,
) -> Result<Vec<Tag>, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, (collection, table_param)) = request.into_parts();
    let grant = authz
        .require::<Project, ViewProject>(gated_conn, &ctx, &project)
        .await?;
    let conn = gated_conn.get(grant);
    let entity_id = table(conn, &collection, &table_param).await?;
    entity_tags(conn, TagEntityType::Table, entity_id).await
}

#[service_factory]
async fn set_table_tag(
    authz: Ctx<AuthzContext>,
    gated_trx: &mut GatedConnection<RW, InProject>,
    request: WithinProject<CreateRequest<TagSet, (CollectionParam, TableParam)>>,
) -> Result<Tag, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, (collection, table_param), set) = request.into_parts();
    let grant = authz
        .require::<Project, DevelopProject>(gated_trx, &ctx, &project)
        .await?;
    let at = gated_trx.locked_at();
    let conn = gated_trx.get(grant);
    let entity_id = table(conn, &collection, &table_param).await?;
    set_user_tag(conn, &ctx, TagEntityType::Table, entity_id, set, at).await
}

#[service_factory]
async fn delete_table_tag(
    authz: Ctx<AuthzContext>,
    gated_trx: &mut GatedConnection<RW, InProject>,
    request: WithinProject<DeleteRequest<(CollectionParam, TableParam, TagParam)>>,
) -> Result<(), TdError> {
    let (project, request) = request.into_parts();
    let (ctx, (collection, table_param, tag)) = request.into_parts();
    let grant = authz
        .require::<Project, DevelopProject>(gated_trx, &ctx, &project)
        .await?;
    let at = gated_trx.locked_at();
    let conn = gated_trx.get(grant);
    let entity_id = table(conn, &collection, &table_param).await?;
    delete_user_tag(conn, TagEntityType::Table, entity_id, tag.key, at).await
}
