//
// Copyright 2025 Tabs Data Inc.
//

use crate::bin::apiserver::roles::ROLES_TAG;
use crate::bin::apiserver::PermissionsState;
use crate::logic::apiserver::status::error_status::GetErrorStatus;
use crate::logic::apiserver::status::DeleteStatus;
use crate::router;
use axum::extract::{Path, State};
use axum::Extension;
use td_apiforge::apiserver_path;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::{RolePermissionParam, DELETE_PERMISSION};
use tower::ServiceExt;

router! {
    state => { PermissionsState },
    routes => { delete_permission }
}

#[apiserver_path(method = delete, path = DELETE_PERMISSION, tag = ROLES_TAG)]
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
