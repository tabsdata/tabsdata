//
// Copyright 2025 Tabs Data Inc.
//

use crate::router;
use crate::router::roles::AUTHZ_TAG;
use crate::router::state::Roles;
use crate::status::error_status::GetErrorStatus;
use crate::status::DeleteStatus;
use axum::extract::{Path, State};
use axum::Extension;
use td_apiforge::apiserver_path;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::{RoleParam, DELETE_ROLE};
use tower::ServiceExt;

router! {
    state => { Roles },
    routes => { delete_role }
}

#[apiserver_path(method = delete, path = DELETE_ROLE, tag = AUTHZ_TAG)]
#[doc = "Delete a role"]
pub async fn delete_role(
    State(state): State<Roles>,
    Extension(context): Extension<RequestContext>,
    Path(role_path): Path<RoleParam>,
) -> Result<DeleteStatus, GetErrorStatus> {
    let request = context.delete(role_path);
    let response = state.delete_role().await.oneshot(request).await?;
    Ok(DeleteStatus::OK(response.into()))
}
