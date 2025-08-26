//
// Copyright 2025 Tabs Data Inc.
//

use crate::router;
use crate::router::roles::AUTHZ_TAG;
use crate::router::state::Permissions;
use crate::status::error_status::ErrorStatus;
use crate::status::ok_status::{DeleteStatus, NoContent};
use axum::Extension;
use axum::extract::{Path, State};
use td_apiforge::apiserver_path;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::{DELETE_PERMISSION, RolePermissionParam};
use tower::ServiceExt;

router! {
    state => { Permissions },
    routes => { delete_permission }
}

#[apiserver_path(method = delete, path = DELETE_PERMISSION, tag = AUTHZ_TAG)]
#[doc = "Delete a permission"]
pub async fn delete_permission(
    State(state): State<Permissions>,
    Extension(context): Extension<RequestContext>,
    Path(param): Path<RolePermissionParam>,
) -> Result<DeleteStatus<NoContent>, ErrorStatus> {
    let request = context.delete(param);
    let response = state.delete_permission().await.oneshot(request).await?;
    Ok(DeleteStatus::OK(response))
}
