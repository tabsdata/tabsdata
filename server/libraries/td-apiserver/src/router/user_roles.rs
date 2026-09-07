//
// Copyright 2025 Tabsdata Inc.
//

use axum::Extension;
use axum::extract::{Path, State};
use axum_extra::extract::Query;
use ta_apiserver::status::error_status::ErrorStatus;
use ta_apiserver::status::extractors::Json;
use ta_apiserver::status::ok_status::{
    CreateStatus, DeleteStatus, GetStatus, ListStatus, NoContent,
};
use td_apiforge::apiserver_path;
use td_objects::dxo::crudl::{ListParams, RequestContext};
use td_objects::dxo::role::RoleParam;
use td_objects::dxo::user::UserParam;
use td_objects::dxo::user_role::{UserRole, UserRoleCreate};
use td_services::Services;

const USER_ROLES_TAG: &str = "User Roles";

#[apiserver_path(mount = rbac, method = post, path = "/roles/{role}/users", tag = USER_ROLES_TAG, doc = "Add a role for a user")]
pub async fn create_user_role(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Path(role): Path<RoleParam>,
    Json(request): Json<UserRoleCreate>,
) -> Result<CreateStatus<UserRole>, ErrorStatus> {
    let request = context.create(role, request);
    let response = state.create_user_role(request).await?;
    Ok(CreateStatus::CREATED(response))
}

#[apiserver_path(mount = rbac, method = delete, path = "/roles/{role}/users/{user}", tag = USER_ROLES_TAG, doc = "Delete a user from a role")]
pub async fn delete_user_role(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Path(role): Path<RoleParam>,
    Path(user): Path<UserParam>,
) -> Result<DeleteStatus<NoContent>, ErrorStatus> {
    let request = context.delete((role, user));
    let response = state.delete_user_role(request).await?;
    Ok(DeleteStatus::OK(response))
}

#[apiserver_path(mount = rbac, method = get, path = "/roles/{role}/users", tag = USER_ROLES_TAG, doc = "List users for a role")]
pub async fn list_user_roles(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Query(list_params): Query<ListParams>,
    Path(role): Path<RoleParam>,
) -> Result<ListStatus<UserRole>, ErrorStatus> {
    let request = context.list(role, list_params);
    let response = state.list_user_role(request).await?;
    Ok(ListStatus::OK(response))
}

#[apiserver_path(mount = rbac, method = get, path = "/roles/{role}/users/{user}", tag = USER_ROLES_TAG, doc = "Read a user role")]
pub async fn read_user_role(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Path(role): Path<RoleParam>,
    Path(user): Path<UserParam>,
) -> Result<GetStatus<UserRole>, ErrorStatus> {
    let request = context.read((role, user));
    let response = state.read_user_role(request).await?;
    Ok(GetStatus::OK(response))
}
