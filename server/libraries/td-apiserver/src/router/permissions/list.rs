//
// Copyright 2025 Tabs Data Inc.
//

use crate::router;
use crate::router::roles::AUTHZ_TAG;
use crate::router::state::Permissions;
use crate::status::error_status::GetErrorStatus;
use axum::extract::{Path, State};
use axum::Extension;
use axum_extra::extract::Query;
use derive_builder::Builder;
use getset::Getters;
use serde::Deserialize;
use serde::Serialize;
use td_apiforge::{apiserver_path, list_status};
use td_objects::crudl::{ListParams, RequestContext};
use td_objects::crudl::{ListResponse, ListResponseBuilder};
use td_objects::rest_urls::{RoleParam, LIST_PERMISSIONS};
use td_objects::types::permission::Permission;
use td_tower::ctx_service::{CtxMap, CtxResponse, CtxResponseBuilder};
use tower::ServiceExt;

router! {
    state => { Permissions },
    routes => { list_permission }
}

list_status!(Permission);

#[apiserver_path(method = get, path = LIST_PERMISSIONS, tag = AUTHZ_TAG)]
#[doc = "List permissions"]
pub async fn list_permission(
    State(state): State<Permissions>,
    Extension(context): Extension<RequestContext>,
    Query(query_params): Query<ListParams>,
    Path(path_params): Path<RoleParam>,
) -> Result<ListStatus, GetErrorStatus> {
    let request = context.list(path_params, query_params);
    let response = state.list_permission().await.oneshot(request).await?;
    Ok(ListStatus::OK(response.into()))
}
