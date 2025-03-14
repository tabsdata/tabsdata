//
// Copyright 2025 Tabs Data Inc.
//

use crate::bin::apisrv::api_server::PermissionsState;
use crate::bin::apisrv::roles::ROLES_TAG;
use crate::logic::apisrv::status::error_status::GetErrorStatus;
use crate::logic::apisrv::status::DeleteStatus;
use crate::router;
use axum::extract::{Path, State};
use axum::Extension;
use td_apiforge::api_server_path;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::{RolePermissionParam, DELETE_PERMISSION};
use tower::ServiceExt;

router! {
    state => { PermissionsState },
    routes => { delete_permission }
}

#[api_server_path(method = delete, path = DELETE_PERMISSION, tag = ROLES_TAG)]
#[doc = "Delete a permission"]
pub async fn delete_permission(
    State(state): State<PermissionsState>,
    Extension(context): Extension<RequestContext>,
    Path(param): Path<RolePermissionParam>,
) -> Result<DeleteStatus, GetErrorStatus> {
    let request = context.delete(param);
    let response = state.delete_permission().await.oneshot(request).await?;
    Ok(DeleteStatus::OK(response.into()))
}
