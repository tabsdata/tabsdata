//
// Copyright 2025 Tabs Data Inc.
//

use crate::bin::apiserver::roles::ROLES_TAG;
use crate::bin::apiserver::UserRolesState;
use crate::logic::apiserver::status::error_status::GetErrorStatus;
use crate::logic::apiserver::status::DeleteStatus;
use crate::router;
use axum::extract::{Path, State};
use axum::Extension;
use td_apiforge::apiserver_path;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::{UserRoleParam, DELETE_USER_ROLE};
use tower::ServiceExt;

router! {
    state => { UserRolesState },
    routes => { delete_user_role }
}

#[apiserver_path(method = delete, path = DELETE_USER_ROLE, tag = ROLES_TAG)]
#[doc = "Delete a user from a role"]
pub async fn delete_user_role(
    State(state): State<UserRolesState>,
    Extension(context): Extension<RequestContext>,
    Path(user_role_param): Path<UserRoleParam>,
) -> Result<DeleteStatus, GetErrorStatus> {
    let request = context.delete(user_role_param);
    let response = state.delete_user_role().await.oneshot(request).await?;
    Ok(DeleteStatus::OK(response.into()))
}
