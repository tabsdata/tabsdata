//
// Copyright 2025 Tabs Data Inc.
//

use crate::router;
use crate::router::roles::ROLES_TAG;
use crate::router::state::UserRoles;
use crate::status::error_status::CreateErrorStatus;
use crate::status::extractors::Json;
use axum::extract::{Path, State};
use axum::Extension;
use derive_builder::Builder;
use getset::Getters;
use serde::Serialize;
use td_apiforge::{apiserver_path, create_status};
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::{RoleParam, CREATE_USER_ROLE};
use td_objects::types::role::{UserRole, UserRoleCreate};
use td_tower::ctx_service::{CtxMap, CtxResponse, CtxResponseBuilder};
use tower::ServiceExt;

router! {
    state => { UserRoles },
    routes => { create_user_role }
}

create_status!(UserRole);

#[apiserver_path(method = post, path = CREATE_USER_ROLE, tag = ROLES_TAG)]
#[doc = "Add a role for a user"]
pub async fn create_user_role(
    State(state): State<UserRoles>,
    Extension(context): Extension<RequestContext>,
    Path(role_param): Path<RoleParam>,
    Json(request): Json<UserRoleCreate>,
) -> Result<CreateStatus, CreateErrorStatus> {
    let request = context.create(role_param, request);
    let response = state.create_user_role().await.oneshot(request).await?;
    Ok(CreateStatus::CREATED(response.into()))
}
