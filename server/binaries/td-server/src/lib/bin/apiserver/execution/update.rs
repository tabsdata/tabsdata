//
//   Copyright 2024 Tabs Data Inc.
//

use crate::bin::apiserver::execution::DataVersionUriParams;
use crate::bin::apiserver::DatasetsState;
use crate::logic::apiserver::status::error_status::UpdateErrorStatus;
use crate::logic::apiserver::status::extractors::Json;
use crate::logic::apiserver::status::EmptyUpdateStatus;
use crate::router;
use axum::extract::{Path, State};
use axum::Extension;
use td_apiforge::{apiserver_path, apiserver_tag};
use td_common::execution_status::DataVersionUpdateRequest;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::UPDATE_DATA_VERSION;
use tower::ServiceExt;

router! {
    state => { DatasetsState },
    routes => { update_execution_status }
}

apiserver_tag!(name = "Internal", description = "Internal API");

#[apiserver_path(method = post, path = UPDATE_DATA_VERSION, tag = INTERNAL_TAG)]
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
