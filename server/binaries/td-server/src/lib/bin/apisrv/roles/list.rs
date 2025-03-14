//
// Copyright 2025 Tabs Data Inc.
//

use crate::bin::apisrv::api_server::RolesState;
use crate::bin::apisrv::roles::ROLES_TAG;
use crate::logic::apisrv::status::error_status::GetErrorStatus;
use crate::router;
use axum::extract::{Query, State};
use axum::Extension;
use derive_builder::Builder;
use getset::Getters;
use serde::Deserialize;
use serde::Serialize;
use td_apiforge::{api_server_path, list_status};
use td_objects::crudl::{ListParams, RequestContext};
use td_objects::crudl::{ListResponse, ListResponseBuilder};
use td_objects::rest_urls::LIST_ROLES;
use td_objects::types::role::Role;
use td_tower::ctx_service::{CtxMap, CtxResponse, CtxResponseBuilder};
use tower::ServiceExt;

router! {
    state => { RolesState },
    routes => { list_role }
}

list_status!(Role);

#[api_server_path(method = get, path = LIST_ROLES, tag = ROLES_TAG)]
#[doc = "List roles"]
pub async fn list_role(
    State(state): State<RolesState>,
    Extension(context): Extension<RequestContext>,
    Query(query_params): Query<ListParams>,
) -> Result<ListStatus, GetErrorStatus> {
    let request = context.list((), query_params);
    let response = state.list_role().await.oneshot(request).await?;
    Ok(ListStatus::OK(response.into()))
}
