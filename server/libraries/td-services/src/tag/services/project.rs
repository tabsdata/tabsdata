//
// Copyright 2026 Tabsdata Inc.
//

use crate::tag::user::{delete_user_tag, set_user_tag};
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{ManageProject, Project};
use td_database::access::{GatedConnection, InProject, ProjectLock, RW};
use td_error::TdError;
use td_objects::dxo::crudl::{CreateRequest, DeleteRequest, WithinProject};
use td_objects::dxo::tag::{Tag, TagParam, TagSet};
use td_objects::types::basic::TagEntity;

#[service_factory]
async fn set_project_tag(
    authz: Ctx<AuthzContext>,
    gated_trx: &mut GatedConnection<RW<ProjectLock>, InProject>,
    request: WithinProject<CreateRequest<TagSet>>,
) -> Result<Tag, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, (), set) = request.into_parts();
    let grant = authz
        .require::<Project, ManageProject>(gated_trx, &ctx, &project)
        .await?;
    let at = gated_trx.locked_at();
    let conn = gated_trx.get(grant);
    set_user_tag(conn, &ctx, TagEntity::from(project), set, at).await
}

#[service_factory]
async fn delete_project_tag(
    authz: Ctx<AuthzContext>,
    gated_trx: &mut GatedConnection<RW<ProjectLock>, InProject>,
    request: WithinProject<DeleteRequest<TagParam>>,
) -> Result<(), TdError> {
    let (project, request) = request.into_parts();
    let (ctx, tag) = request.into_parts();
    let grant = authz
        .require::<Project, ManageProject>(gated_trx, &ctx, &project)
        .await?;
    let at = gated_trx.locked_at();
    let conn = gated_trx.get(grant);
    delete_user_tag(conn, TagEntity::from(project), tag.key, at).await
}
