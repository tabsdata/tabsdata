//
//   Copyright 2024 Tabs Data Inc.
//

use crate::bin::apisrv::api_server::DatasetsState;
use crate::bin::apisrv::execution::DataVersionUriParams;
use crate::logic::apisrv::status::error_status::UpdateErrorStatus;
use crate::logic::apisrv::status::extractors::Json;
use crate::logic::apisrv::status::EmptyUpdateStatus;
use crate::router;
use axum::extract::{Path, State};
use axum::Extension;
use td_apiforge::{api_server_path, api_server_tag};
use td_common::execution_status::DataVersionUpdateRequest;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::UPDATE_DATA_VERSION;
use tower::ServiceExt;

router! {
    state => { DatasetsState },
    routes => { update_execution_status }
}

api_server_tag!(name = "Internal", description = "Internal API");

#[api_server_path(method = post, path = UPDATE_DATA_VERSION, tag = INTERNAL_TAG)]
#[doc = "Update the execution status for a dataset"]
pub async fn update_execution_status(
    State(dataset_state): State<DatasetsState>,
    Extension(context): Extension<RequestContext>,
    Path(data_version_uri_params): Path<DataVersionUriParams>,
    Json(request): Json<DataVersionUpdateRequest>,
) -> Result<EmptyUpdateStatus, UpdateErrorStatus> {
    let request = context.update(data_version_uri_params, request);
    let response = dataset_state
        .update_execution_status()
        .await
        .oneshot(request)
        .await?;
    Ok(EmptyUpdateStatus::OK(response.into()))
}
