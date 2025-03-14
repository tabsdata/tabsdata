//
// Copyright 2025 Tabs Data Inc.
//

use crate::bin::apisrv::api_server::RolesState;
use crate::bin::apisrv::roles::ROLES_TAG;
use crate::logic::apisrv::status::error_status::GetErrorStatus;
use crate::router;
use axum::extract::{Path, State};
use axum::routing::get;
use axum::Extension;
use derive_builder::Builder;
use getset::Getters;
use serde::Serialize;
use td_apiforge::{api_server_path, get_status};
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::{RoleParam, GET_ROLE};
use td_objects::types::role::Role;
use td_tower::ctx_service::{CtxMap, CtxResponse, CtxResponseBuilder};
use tower::ServiceExt;

router! {
    state => { RolesState },
    paths => {{
        GET_ROLE => get(read_role),
    }}
}

get_status!(Role);

#[api_server_path(method = get, path = GET_ROLE, tag = ROLES_TAG)]
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
