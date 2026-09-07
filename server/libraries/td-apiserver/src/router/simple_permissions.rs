//
// Copyright 2026 Tabsdata Inc.
//

//! Simple Security Mode permission facade router. Flat per-user
//! permission grant/revoke/list endpoints that translate to RBAC
//! role-membership operations. Mounted alongside the RBAC `/roles/.../permissions`
//! surface — both views operate on the same tables.

use axum::Extension;
use axum::extract::State;
use axum_extra::extract::Query;
use ta_apiserver::status::error_status::ErrorStatus;
use ta_apiserver::status::extractors::Json;
use ta_apiserver::status::ok_status::{CreateStatus, DeleteStatus, ListStatus, NoContent};
use td_apiforge::apiserver_path;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::project::ProjectParam;
use td_objects::dxo::simple_permission::{
    ListByProjectRequest, ListByUserRequest, SimplePermission, SimplePermissionGrant,
    SimpleProjectAccess,
};
use td_objects::dxo::user::UserParam;
use td_services::Services;

const SIMPLE_PERMISSIONS_TAG: &str = "Permissions (Simple)";

#[apiserver_path(mount = simple, method = post, path = "/permission-grants", tag = SIMPLE_PERMISSIONS_TAG, doc = "Grant a flat permission to a user (Simple Security Mode)")]
pub async fn grant(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Json(request): Json<SimplePermissionGrant>,
) -> Result<CreateStatus<NoContent>, ErrorStatus> {
    let request = context.create((), request);
    let response = state.grant_simple_permission(request).await?;
    Ok(CreateStatus::CREATED(response))
}

#[apiserver_path(mount = simple, method = delete, path = "/permission-grants", tag = SIMPLE_PERMISSIONS_TAG, doc = "Revoke a flat permission from a user (Simple Security Mode)")]
pub async fn revoke(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Json(request): Json<SimplePermissionGrant>,
) -> Result<DeleteStatus<NoContent>, ErrorStatus> {
    let request = context.delete(request);
    let response = state.revoke_simple_permission(request).await?;
    Ok(DeleteStatus::OK(response))
}

// Two list shapes (different response types), so two routes.
#[apiserver_path(mount = simple,
    method = get,
    path = "/permission-grants/by-user",
    tag = SIMPLE_PERMISSIONS_TAG,
    doc = "List a user's effective flat permissions (Simple Security Mode)")]
pub async fn list_by_user(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Query(user): Query<UserParam>,
) -> Result<ListStatus<SimplePermission>, ErrorStatus> {
    let request = ListByUserRequest::new(context, user);
    let response = state.list_simple_permission_by_user(request).await?;
    Ok(ListStatus::OK(response))
}

#[apiserver_path(mount = simple,
    method = get,
    path = "/permission-grants/by-project",
    tag = SIMPLE_PERMISSIONS_TAG,
    doc = "List everyone with authority on a project (Simple Security Mode)")]
pub async fn list_by_project(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Query(project): Query<ProjectParam>,
) -> Result<ListStatus<SimpleProjectAccess>, ErrorStatus> {
    let request = ListByProjectRequest::new(context, project);
    let response = state.list_simple_permission_by_project(request).await?;
    Ok(ListStatus::OK(response))
}
