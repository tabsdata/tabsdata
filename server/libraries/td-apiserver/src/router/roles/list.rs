//
// Copyright 2025 Tabs Data Inc.
//

use crate::router;
use crate::router::roles::AUTHZ_TAG;
use crate::router::state::Roles;
use crate::status::error_status::GetErrorStatus;
use axum::extract::State;
use axum::Extension;
use axum_extra::extract::Query;
use derive_builder::Builder;
use getset::Getters;
use serde::Deserialize;
use serde::Serialize;
use td_apiforge::{apiserver_path, list_status};
use td_objects::crudl::{ListParams, RequestContext};
use td_objects::crudl::{ListResponse, ListResponseBuilder};
use td_objects::rest_urls::LIST_ROLES;
use td_objects::types::role::Role;
use td_tower::ctx_service::{CtxMap, CtxResponse, CtxResponseBuilder};
use tower::ServiceExt;

router! {
    state => { Roles },
    routes => { list_role }
}

list_status!(Role);

#[apiserver_path(method = get, path = LIST_ROLES, tag = AUTHZ_TAG)]
#[doc = "List roles"]
pub async fn list_role(
    State(state): State<Roles>,
    Extension(context): Extension<RequestContext>,
    Query(query_params): Query<ListParams>,
) -> Result<ListStatus, GetErrorStatus> {
    let request = context.list((), query_params);
    let response = state.list_role().await.oneshot(request).await?;
    Ok(ListStatus::OK(response.into()))
}
