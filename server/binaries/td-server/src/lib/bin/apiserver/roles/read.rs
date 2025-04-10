//
// Copyright 2025 Tabs Data Inc.
//

use crate::bin::apiserver::roles::ROLES_TAG;
use crate::bin::apiserver::RolesState;
use crate::logic::apiserver::status::error_status::GetErrorStatus;
use crate::router;
use axum::extract::{Path, State};
use axum::Extension;
use derive_builder::Builder;
use getset::Getters;
use serde::Serialize;
use td_apiforge::{apiserver_path, get_status};
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::{RoleParam, GET_ROLE};
use td_objects::types::role::Role;
use td_tower::ctx_service::{CtxMap, CtxResponse, CtxResponseBuilder};
use tower::ServiceExt;

router! {
    state => { RolesState },
    routes => { read_role }
}

get_status!(Role);

#[apiserver_path(method = get, path = GET_ROLE, tag = ROLES_TAG)]
#[doc = "Read a role"]
pub async fn read_role(
    State(state): State<RolesState>,
    Extension(context): Extension<RequestContext>,
    Path(role_param): Path<RoleParam>,
) -> Result<GetStatus, GetErrorStatus> {
    let request = context.read(role_param);
    let response = state.read_role().await.oneshot(request).await?;
    Ok(GetStatus::OK(response.into()))
}
