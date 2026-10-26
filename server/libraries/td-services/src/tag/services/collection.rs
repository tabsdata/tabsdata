//
// Copyright 2026 Tabsdata Inc.
//

use crate::tag::user::{delete_user_tag, entity_tags, set_user_tag};
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{DevelopProject, Project, ViewProject};
use td_database::access::{AccessMode, GatedConnection, InProject, RO, RW};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::collection::{CollectionDB, CollectionParam};
use td_objects::dxo::crudl::{CreateRequest, DeleteRequest, ReadRequest, WithinProject};
use td_objects::dxo::tag::{Tag, TagParam, TagSet};
use td_objects::types::basic::{TagEntityId, TagEntityType};
use td_queries::Queries;

async fn collection<M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, InProject, M>,
    collection: &CollectionParam,
) -> Result<TagEntityId, TdError> {
    let collection: ActiveCollectionDB = conn.fetch_one(&collection.collection).await?;
    TagEntityId::try_from(collection.id)
}

#[service_factory]
async fn list_collection_tags(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO, InProject>,
    request: WithinProject<ReadRequest<CollectionParam>>,
) -> Result<Vec<Tag>, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, collection_param) = request.into_parts();
    let grant = authz
        .require::<Project, ViewProject>(gated_conn, &ctx, &project)
        .await?;
    let conn = gated_conn.get(grant);
    let entity_id = collection(conn, &collection_param).await?;
    entity_tags(conn, TagEntityType::Collection, entity_id).await
}

#[service_factory]
async fn set_collection_tag(
    authz: Ctx<AuthzContext>,
    gated_trx: &mut GatedConnection<RW, InProject>,
    request: WithinProject<CreateRequest<TagSet, CollectionParam>>,
) -> Result<Tag, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, collection_param, set) = request.into_parts();
    let grant = authz
        .require::<Project, DevelopProject>(gated_trx, &ctx, &project)
        .await?;
    let at = gated_trx.locked_at();
    let conn = gated_trx.get(grant);
    let entity_id = collection(conn, &collection_param).await?;
    set_user_tag(conn, &ctx, TagEntityType::Collection, entity_id, set, at).await
}

#[service_factory]
async fn delete_collection_tag(
    authz: Ctx<AuthzContext>,
    gated_trx: &mut GatedConnection<RW, InProject>,
    request: WithinProject<DeleteRequest<(CollectionParam, TagParam)>>,
) -> Result<(), TdError> {
    let (project, request) = request.into_parts();
    let (ctx, (collection_param, tag)) = request.into_parts();
    let grant = authz
        .require::<Project, DevelopProject>(gated_trx, &ctx, &project)
        .await?;
    let at = gated_trx.locked_at();
    let conn = gated_trx.get(grant);
    let entity_id = collection(conn, &collection_param).await?;
    delete_user_tag(conn, TagEntityType::Collection, entity_id, tag.key, at).await
}
