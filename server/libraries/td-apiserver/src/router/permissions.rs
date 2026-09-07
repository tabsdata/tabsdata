//
// Copyright 2025 Tabsdata Inc.
//

use axum::Extension;
use axum::extract::{Path, State};
use axum_extra::extract::Query;
use ta_apiserver::status::error_status::ErrorStatus;
use ta_apiserver::status::extractors::Json;
use ta_apiserver::status::ok_status::{CreateStatus, DeleteStatus, ListStatus, NoContent};
use td_apiforge::apiserver_path;
use td_objects::dxo::crudl::{ListParams, RequestContext};
use td_objects::dxo::permission::{Permission, PermissionCreate, PermissionParam};
use td_objects::dxo::role::RoleParam;
use td_services::Services;

const PERMISSIONS_TAG: &str = "Permissions";

#[apiserver_path(mount = rbac, method = post, path = "/roles/{role}/permissions", tag = PERMISSIONS_TAG, doc = "Create a permission")]
pub async fn create_permission(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Path(role): Path<RoleParam>,
    Json(request): Json<PermissionCreate>,
) -> Result<CreateStatus<Permission>, ErrorStatus> {
    let request = context.create(role, request);
    let response = state.create_permission(request).await?;
    Ok(CreateStatus::CREATED(response))
}

#[apiserver_path(mount = rbac, method = delete, path = "/roles/{role}/permissions/{permission}", tag = PERMISSIONS_TAG, doc = "Delete a permission")]
pub async fn delete_permission(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Path(role): Path<RoleParam>,
    Path(permission): Path<PermissionParam>,
) -> Result<DeleteStatus<NoContent>, ErrorStatus> {
    let request = context.delete((role, permission));
    let response = state.delete_permission(request).await?;
    Ok(DeleteStatus::OK(response))
}

#[apiserver_path(mount = rbac, method = get, path = "/roles/{role}/permissions", tag = PERMISSIONS_TAG, doc = "List permissions")]
pub async fn list_permissions(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Query(list_params): Query<ListParams>,
    Path(role): Path<RoleParam>,
) -> Result<ListStatus<Permission>, ErrorStatus> {
    let request = context.list(role, list_params);
    let response = state.list_permission(request).await?;
    Ok(ListStatus::OK(response))
}
