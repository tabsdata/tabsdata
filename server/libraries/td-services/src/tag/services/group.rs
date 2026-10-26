//
// Copyright 2026 Tabsdata Inc.
//

use crate::tag::user::{delete_user_tag, entity_tags, set_user_tag};
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{ManageProject, Project, ViewProject};
use td_database::access::{AccessMode, GatedConnection, InProject, RO, RW};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::crudl::{CreateRequest, DeleteRequest, ReadRequest, WithinProject};
use td_objects::dxo::group::{GroupDB, GroupParam};
use td_objects::dxo::tag::{Tag, TagParam, TagSet};
use td_objects::types::basic::{TagEntityId, TagEntityType};
use td_queries::Queries;

async fn group<M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, InProject, M>,
    group: &GroupParam,
) -> Result<TagEntityId, TdError> {
    let group: GroupDBWithNames = conn.fetch_one(&group.group).await?;
    TagEntityId::try_from(group.base.base.id)
}

#[service_factory]
async fn list_group_tags(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO, InProject>,
    request: WithinProject<ReadRequest<GroupParam>>,
) -> Result<Vec<Tag>, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, group_param) = request.into_parts();
    let grant = authz
        .require::<Project, ViewProject>(gated_conn, &ctx, &project)
        .await?;
    let conn = gated_conn.get(grant);
    let entity_id = group(conn, &group_param).await?;
    entity_tags(conn, TagEntityType::Group, entity_id).await
}

#[service_factory]
async fn set_group_tag(
    authz: Ctx<AuthzContext>,
    gated_trx: &mut GatedConnection<RW, InProject>,
    request: WithinProject<CreateRequest<TagSet, GroupParam>>,
) -> Result<Tag, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, group_param, set) = request.into_parts();
    let grant = authz
        .require::<Project, ManageProject>(gated_trx, &ctx, &project)
        .await?;
    let at = gated_trx.locked_at();
    let conn = gated_trx.get(grant);
    let entity_id = group(conn, &group_param).await?;
    set_user_tag(conn, &ctx, TagEntityType::Group, entity_id, set, at).await
}

#[service_factory]
async fn delete_group_tag(
    authz: Ctx<AuthzContext>,
    gated_trx: &mut GatedConnection<RW, InProject>,
    request: WithinProject<DeleteRequest<(GroupParam, TagParam)>>,
) -> Result<(), TdError> {
    let (project, request) = request.into_parts();
    let (ctx, (group_param, tag)) = request.into_parts();
    let grant = authz
        .require::<Project, ManageProject>(gated_trx, &ctx, &project)
        .await?;
    let at = gated_trx.locked_at();
    let conn = gated_trx.get(grant);
    let entity_id = group(conn, &group_param).await?;
    delete_user_tag(conn, TagEntityType::Group, entity_id, tag.key, at).await
}
