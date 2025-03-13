//
// Copyright 2025 Tabs Data Inc.
//

use crate::bin::apisrv::api_server::RolesState;
use crate::bin::apisrv::roles::ROLES_TAG;
use crate::logic::apisrv::status::error_status::GetErrorStatus;
use crate::logic::apisrv::status::DeleteStatus;
use crate::router;
use axum::extract::{Path, State};
use axum::routing::delete;
use axum::Extension;
use td_apiforge::api_server_path;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::DELETE_ROLE;
use td_objects::types::role::RoleParam;
use tower::ServiceExt;

router! {
    state => { RolesState },
    paths => {{
        DELETE_ROLE => delete(delete_role),
    }}
}

#[api_server_path(method = delete, path = DELETE_ROLE, tag = ROLES_TAG)]
#[doc = "Delete a role"]
pub async fn delete_role(
    State(state): State<RolesState>,
    Extension(context): Extension<RequestContext>,
    Path(role_path): Path<RoleParam>,
) -> Result<DeleteStatus, GetErrorStatus> {
    let request = context.delete(role_path);
    let response = state.delete_role().await.oneshot(request).await?;
    Ok(DeleteStatus::OK(response.into()))
}
