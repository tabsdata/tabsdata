//
// Copyright 2025 Tabs Data Inc.
//

use crate::router;
use crate::router::roles::AUTHZ_TAG;
use crate::router::state::InterCollectionPermissions;
use crate::status::error_status::ErrorStatus;
use crate::status::ok_status::ListStatus;
use axum::Extension;
use axum::extract::{Path, State};
use axum_extra::extract::Query;
use td_apiforge::apiserver_path;
use td_objects::crudl::{ListParams, RequestContext};
use td_objects::rest_urls::{CollectionParam, LIST_INTER_COLLECTION_PERMISSIONS};
use td_objects::types::permission::InterCollectionPermission;
use tower::ServiceExt;

router! {
    state => { InterCollectionPermissions },
    routes => { list_inter_collection_permission }
}

#[apiserver_path(method = get, path = LIST_INTER_COLLECTION_PERMISSIONS, tag = AUTHZ_TAG)]
#[doc = "List permissions"]
pub async fn list_inter_collection_permission(
    State(state): State<InterCollectionPermissions>,
    Extension(context): Extension<RequestContext>,
    Query(query_params): Query<ListParams>,
    Path(path_params): Path<CollectionParam>,
) -> Result<ListStatus<InterCollectionPermission>, ErrorStatus> {
    let request = context.list(path_params, query_params);
    let response = state.list_permission().await.oneshot(request).await?;
    Ok(ListStatus::OK(response))
}
