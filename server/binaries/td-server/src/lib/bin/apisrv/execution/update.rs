//
//   Copyright 2024 Tabs Data Inc.
//

use crate::bin::apisrv::api_server::DatasetsState;
use crate::bin::apisrv::execution::DataVersionUriParams;
use crate::logic::apisrv::status::error_status::UpdateErrorStatus;
use crate::logic::apisrv::status::extractors::Json;
use crate::logic::apisrv::status::status_macros::EmptyUpdateStatus;
use crate::router;
use axum::extract::{Path, State};
use axum::routing::post;
use axum::Extension;
use td_common::execution_status::DataVersionUpdateRequest;
use td_objects::crudl::RequestContext;
use tower::ServiceExt;

pub const DATA_VERSION: &str = "/data_version/{data_version_id}";

router! {
    state => { DatasetsState },
    paths => {{
        DATA_VERSION => post(update_execution_status),
    }}
}

// This is a private endpoint, we don't want to document it
#[doc = "Update the execution status for a dataset"]
pub async fn update_execution_status(
    State(dataset_state): State<DatasetsState>,
    Extension(context): Extension<RequestContext>,
    Path(data_version_uri_params): Path<DataVersionUriParams>,
    Json(request): Json<DataVersionUpdateRequest>,
) -> Result<EmptyUpdateStatus, UpdateErrorStatus> {
    let request = context.update(data_version_uri_params, request);
    dataset_state
        .update_execution_status()
        .await
        .oneshot(request)
        .await?;
    Ok(EmptyUpdateStatus::NO_CONTENT)
}
