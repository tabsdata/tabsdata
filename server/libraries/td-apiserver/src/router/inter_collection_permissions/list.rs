//
// Copyright 2025. Tabs Data Inc.
//

//
// Copyright 2025 Tabs Data Inc.
//

use crate::router;
use crate::router::roles::AUTHZ_TAG;
use crate::router::state::InterCollectionPermissions;
use crate::status::error_status::GetErrorStatus;
use axum::extract::{Path, Query, State};
use axum::Extension;
use derive_builder::Builder;
use getset::Getters;
use serde::Deserialize;
use serde::Serialize;
use td_apiforge::{apiserver_path, list_status};
use td_objects::crudl::{ListParams, RequestContext};
use td_objects::crudl::{ListResponse, ListResponseBuilder};
use td_objects::rest_urls::{CollectionParam, LIST_INTER_COLLECTION_PERMISSIONS};
use td_objects::types::permission::InterCollectionPermission;
use td_tower::ctx_service::{CtxMap, CtxResponse, CtxResponseBuilder};
use tower::ServiceExt;

router! {
    state => { InterCollectionPermissions },
    routes => { list_permission }
}

list_status!(InterCollectionPermission);

#[apiserver_path(method = get, path = LIST_INTER_COLLECTION_PERMISSIONS, tag = AUTHZ_TAG)]
#[doc = "List permissions"]
pub async fn list_permission(
    State(state): State<InterCollectionPermissions>,
    Extension(context): Extension<RequestContext>,
    Query(query_params): Query<ListParams>,
    Path(path_params): Path<CollectionParam>,
) -> Result<ListStatus, GetErrorStatus> {
    let request = context.list(path_params, query_params);
    let response = state.list_permission().await.oneshot(request).await?;
    Ok(ListStatus::OK(response.into()))
}
