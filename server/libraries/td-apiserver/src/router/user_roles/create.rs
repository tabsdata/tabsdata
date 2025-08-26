//
// Copyright 2025 Tabs Data Inc.
//

use crate::router;
use crate::router::roles::AUTHZ_TAG;
use crate::router::state::UserRoles;
use crate::status::error_status::ErrorStatus;
use crate::status::extractors::Json;
use crate::status::ok_status::CreateStatus;
use axum::Extension;
use axum::extract::{Path, State};
use td_apiforge::apiserver_path;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::{CREATE_USER_ROLE, RoleParam};
use td_objects::types::role::{UserRole, UserRoleCreate};
use tower::ServiceExt;

router! {
    state => { UserRoles },
    routes => { create_user_role }
}

#[apiserver_path(method = post, path = CREATE_USER_ROLE, tag = AUTHZ_TAG)]
#[doc = "Add a role for a user"]
pub async fn create_user_role(
    State(state): State<UserRoles>,
    Extension(context): Extension<RequestContext>,
    Path(role_param): Path<RoleParam>,
    Json(request): Json<UserRoleCreate>,
) -> Result<CreateStatus<UserRole>, ErrorStatus> {
    let request = context.create(role_param, request);
    let response = state.create_user_role().await.oneshot(request).await?;
    Ok(CreateStatus::CREATED(response))
}
