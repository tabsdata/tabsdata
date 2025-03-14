//
// Copyright 2025 Tabs Data Inc.
//

use crate::bin::apisrv::api_server::PermissionsState;
use crate::bin::apisrv::roles::ROLES_TAG;
use crate::logic::apisrv::status::error_status::GetErrorStatus;
use crate::router;
use axum::extract::{Path, Query, State};
use axum::routing::get;
use axum::Extension;
use derive_builder::Builder;
use getset::Getters;
use serde::Deserialize;
use serde::Serialize;
use td_apiforge::{api_server_path, list_status};
use td_objects::crudl::{ListParams, RequestContext};
use td_objects::crudl::{ListResponse, ListResponseBuilder};
use td_objects::rest_urls::{RoleParam, LIST_PERMISSIONS};
use td_objects::types::permission::Permission;
use td_tower::ctx_service::{CtxMap, CtxResponse, CtxResponseBuilder};
use tower::ServiceExt;

router! {
    state => { PermissionsState },
    paths => {{
        LIST_PERMISSIONS => get(list_permission),
    }}
}

list_status!(Permission);

#[api_server_path(method = get, path = LIST_PERMISSIONS, tag = ROLES_TAG)]
#[doc = "List permissions"]
pub async fn list_permission(
    State(state): State<PermissionsState>,
    Extension(context): Extension<RequestContext>,
    Query(query_params): Query<ListParams>,
    Path(path_params): Path<RoleParam>,
) -> Result<ListStatus, GetErrorStatus> {
    let request = context.list(path_params, query_params);
    let response = state.list_permission().await.oneshot(request).await?;
    Ok(ListStatus::OK(response.into()))
}
