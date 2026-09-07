//
// Copyright 2026 Tabsdata Inc.
//

use crate::extractors::Project;
use axum::Extension;
use axum::extract::{Path, State};
use axum_extra::extract::Query;
use ta_apiserver::status::error_status::ErrorStatus;
use ta_apiserver::status::extractors::Json;
use ta_apiserver::status::ok_status::{
    CreateStatus, DeleteStatus, GetStatus, ListStatus, NoContent, UpdateStatus,
};
use td_apiforge::apiserver_path;
use td_database::pool::scoped::ScopedExt;
use td_objects::dxo::crudl::{ListParams, RequestContext};
use td_objects::dxo::group::{Group, GroupCreate, GroupParam, GroupUpdate};
use td_services::Services;

const GROUPS_TAG: &str = "Groups";

#[apiserver_path(mount = project, method = post, path = "/projects/{project}/groups", tag = GROUPS_TAG, doc = "Create a group")]
pub async fn create_group(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Json(request): Json<GroupCreate>,
) -> Result<CreateStatus<Group>, ErrorStatus> {
    let request = context.create((), request).in_project(project_id);
    let response = state.create_group(request).await?;
    Ok(CreateStatus::CREATED(response))
}

#[apiserver_path(mount = project, method = delete, path = "/projects/{project}/groups/{group}", tag = GROUPS_TAG, doc = "Delete a group")]
pub async fn delete_group(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Path(group): Path<GroupParam>,
) -> Result<DeleteStatus<NoContent>, ErrorStatus> {
    let request = context.delete(group).in_project(project_id);
    let response = state.delete_group(request).await?;
    Ok(DeleteStatus::OK(response))
}

#[apiserver_path(mount = project, method = get, path = "/projects/{project}/groups", tag = GROUPS_TAG, doc = "List groups")]
pub async fn list_group(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Query(list_params): Query<ListParams>,
) -> Result<ListStatus<Group>, ErrorStatus> {
    let request = context.list((), list_params).in_project(project_id);
    let response = state.list_group(request).await?;
    Ok(ListStatus::OK(response))
}

#[apiserver_path(mount = project, method = get, path = "/projects/{project}/groups/{group}", tag = GROUPS_TAG, doc = "Read a group")]
pub async fn read_group(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Path(group): Path<GroupParam>,
) -> Result<GetStatus<Group>, ErrorStatus> {
    let request = context.read(group).in_project(project_id);
    let response = state.read_group(request).await?;
    Ok(GetStatus::OK(response))
}

#[apiserver_path(mount = project, method = post, path = "/projects/{project}/groups/{group}", tag = GROUPS_TAG, doc = "Update a group")]
pub async fn update_group(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Path(group): Path<GroupParam>,
    Json(request): Json<GroupUpdate>,
) -> Result<UpdateStatus<Group>, ErrorStatus> {
    let request = context.update(group, request).in_project(project_id);
    let response = state.update_group(request).await?;
    Ok(UpdateStatus::OK(response))
}
