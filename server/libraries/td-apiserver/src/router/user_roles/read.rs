//
// Copyright 2025 Tabs Data Inc.
//

use crate::router;
use crate::router::roles::AUTHZ_TAG;
use crate::router::state::UserRoles;
use crate::status::error_status::ErrorStatus;
use crate::status::ok_status::GetStatus;
use axum::Extension;
use axum::extract::{Path, State};
use td_apiforge::apiserver_path;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::{GET_USER_ROLE, UserRoleParam};
use td_objects::types::role::UserRole;
use tower::ServiceExt;

router! {
    state => { UserRoles },
    routes => { read_user_role }
}

#[apiserver_path(method = get, path = GET_USER_ROLE, tag = AUTHZ_TAG)]
#[doc = "Read a user role"]
pub async fn read_user_role(
    State(state): State<UserRoles>,
    Extension(context): Extension<RequestContext>,
    Path(user_role_param): Path<UserRoleParam>,
) -> Result<GetStatus<UserRole>, ErrorStatus> {
    let request = context.read(user_role_param);
    let response = state.read_user_roles().await.oneshot(request).await?;
    Ok(GetStatus::OK(response))
}
