//
// Copyright 2025. Tabs Data Inc.
//
//
// Copyright 2025 Tabs Data Inc.
//

use crate::router;
use crate::router::roles::AUTHZ_TAG;
use crate::router::state::InterCollectionPermissions;
use crate::status::error_status::ErrorStatus;
use crate::status::ok_status::{DeleteStatus, NoContent};
use axum::extract::{Path, State};
use axum::Extension;
use td_apiforge::apiserver_path;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::{InterCollectionPermissionParam, DELETE_INTER_COLLECTION_PERMISSION};
use tower::ServiceExt;

router! {
    state => { InterCollectionPermissions },
    routes => { delete_inter_collection_permission }
}

#[apiserver_path(method = delete, path = DELETE_INTER_COLLECTION_PERMISSION, tag = AUTHZ_TAG)]
#[doc = "Delete an inter collection permission"]
pub async fn delete_inter_collection_permission(
    State(state): State<InterCollectionPermissions>,
    Extension(context): Extension<RequestContext>,
    Path(param): Path<InterCollectionPermissionParam>,
) -> Result<DeleteStatus<NoContent>, ErrorStatus> {
    let request = context.delete(param);
    let response = state.delete_permission().await.oneshot(request).await?;
    Ok(DeleteStatus::OK(response))
}
