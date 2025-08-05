//
// Copyright 2025 Tabs Data Inc.
//

use crate::router;
use crate::router::roles::AUTHZ_TAG;
use crate::router::state::Permissions;
use crate::status::error_status::ErrorStatus;
use crate::status::ok_status::ListStatus;
use axum::extract::{Path, State};
use axum::Extension;
use axum_extra::extract::Query;
use td_apiforge::apiserver_path;
use td_objects::crudl::{ListParams, RequestContext};
use td_objects::rest_urls::{RoleParam, LIST_PERMISSIONS};
use td_objects::types::permission::Permission;
use tower::ServiceExt;

router! {
    state => { Permissions },
    routes => { list_permission }
}

#[apiserver_path(method = get, path = LIST_PERMISSIONS, tag = AUTHZ_TAG)]
#[doc = "List permissions"]
pub async fn list_permission(
    State(state): State<Permissions>,
    Extension(context): Extension<RequestContext>,
    Query(query_params): Query<ListParams>,
    Path(path_params): Path<RoleParam>,
) -> Result<ListStatus<Permission>, ErrorStatus> {
    let request = context.list(path_params, query_params);
    let response = state.list_permission().await.oneshot(request).await?;
    Ok(ListStatus::OK(response))
}
