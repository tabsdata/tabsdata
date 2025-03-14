//
// Copyright 2025 Tabs Data Inc.
//

use crate::bin::apisrv::api_server::UserRolesState;
use crate::bin::apisrv::roles::ROLES_TAG;
use crate::logic::apisrv::status::error_status::GetErrorStatus;
use crate::router;
use axum::extract::{Path, State};
use axum::Extension;
use derive_builder::Builder;
use getset::Getters;
use serde::Serialize;
use td_apiforge::{api_server_path, get_status};
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::{UserRoleParam, GET_USER_ROLE};
use td_objects::types::role::UserRole;
use td_tower::ctx_service::{CtxMap, CtxResponse, CtxResponseBuilder};
use tower::ServiceExt;

router! {
    state => { UserRolesState },
    routes => { read_user_role }
}

get_status!(UserRole);

#[api_server_path(method = get, path = GET_USER_ROLE, tag = ROLES_TAG)]
#[doc = "Read a user role"]
pub async fn read_user_role(
    State(state): State<UserRolesState>,
    Extension(context): Extension<RequestContext>,
    Path(user_role_param): Path<UserRoleParam>,
) -> Result<GetStatus, GetErrorStatus> {
    let request = context.read(user_role_param);
    let response = state.read_user_roles().await.oneshot(request).await?;
    Ok(GetStatus::OK(response.into()))
}
