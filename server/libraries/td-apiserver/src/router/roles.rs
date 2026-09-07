//
// Copyright 2025 Tabsdata Inc.
//

use axum::Extension;
use axum::extract::{Path, State};
use axum_extra::extract::Query;
use ta_apiserver::status::error_status::ErrorStatus;
use ta_apiserver::status::extractors::Json;
use ta_apiserver::status::ok_status::{
    CreateStatus, DeleteStatus, GetStatus, ListStatus, NoContent, UpdateStatus,
};
use td_apiforge::apiserver_path;
use td_objects::dxo::crudl::{ListParams, RequestContext};
use td_objects::dxo::role::{Role, RoleCreate, RoleParam, RoleUpdate};
use td_services::Services;

const ROLES_TAG: &str = "Roles";

#[apiserver_path(
    mount(rbac),
    method(post),
    path = "/roles",
    tag(ROLES_TAG),
    doc = "Create a role"
)]
pub async fn create_role(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Json(request): Json<RoleCreate>,
) -> Result<CreateStatus<Role>, ErrorStatus> {
    let request = context.create((), request);
    let response = state.create_role(request).await?;
    Ok(CreateStatus::CREATED(response))
}

#[apiserver_path(
    mount(rbac),
    method(delete),
    path = "/roles/{role}",
    tag(ROLES_TAG),
    doc = "Delete a role"
)]
pub async fn delete_role(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Path(role): Path<RoleParam>,
) -> Result<DeleteStatus<NoContent>, ErrorStatus> {
    let request = context.delete(role);
    let response = state.delete_role(request).await?;
    Ok(DeleteStatus::OK(response))
}

#[apiserver_path(
    mount(rbac),
    method(get),
    path = "/roles",
    tag(ROLES_TAG),
    doc = "List roles"
)]
pub async fn list_role(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Query(list_params): Query<ListParams>,
) -> Result<ListStatus<Role>, ErrorStatus> {
    let request = context.list((), list_params);
    let response = state.list_role(request).await?;
    Ok(ListStatus::OK(response))
}

#[apiserver_path(
    mount(rbac),
    method(get),
    path = "/roles/{role}",
    tag(ROLES_TAG),
    doc = "Read a role"
)]
pub async fn read_role(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Path(role): Path<RoleParam>,
) -> Result<GetStatus<Role>, ErrorStatus> {
    let request = context.read(role);
    let response = state.read_role(request).await?;
    Ok(GetStatus::OK(response))
}

#[apiserver_path(
    mount(rbac),
    method(post),
    path = "/roles/{role}",
    tag(ROLES_TAG),
    doc = "Update a role"
)]
pub async fn update_role(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Path(role): Path<RoleParam>,
    Json(request): Json<RoleUpdate>,
) -> Result<UpdateStatus<Role>, ErrorStatus> {
    let request = context.update(role, request);
    let response = state.update_role(request).await?;
    Ok(UpdateStatus::OK(response))
}
