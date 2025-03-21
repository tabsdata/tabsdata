//
//  Copyright 2024 Tabs Data Inc.
//

use crate::bin::apiserver::functions::FUNCTIONS_TAG;
use crate::bin::apiserver::DatasetsState;
use crate::logic::apiserver::status::error_status::UpdateErrorStatus;
use crate::logic::apiserver::status::EmptyUpdateStatus;
use crate::router;
use axum::extract::{Path, Request, State};
use axum::Extension;
use td_apiforge::{apiserver_path, apiserver_schema};
use td_objects::crudl::RequestContext;
use td_objects::datasets::dto::UploadFunction;
use td_objects::rest_urls::{FunctionIdParam, FUNCTION_UPLOAD};
use tower::ServiceExt;

// TODO(TD-281) add Datasets logic, clean unused code serving as example
router! {
    state => { DatasetsState },
    routes => { upload_function }
}

/// This struct is just used to document FileUpload in the OpenAPI schema.
/// It allows for a single file upload, of any kind, in binary format.
#[allow(dead_code)]
#[apiserver_schema]
pub struct FileUpload(Vec<u8>);

#[apiserver_path(method = post, path = FUNCTION_UPLOAD, tag = FUNCTIONS_TAG)]
#[doc = "Upload a function bundle (completing a function create or update)"]
pub async fn upload_function(
    State(dataset_state): State<DatasetsState>,
    Extension(_context): Extension<RequestContext>,
    Path(function_id_param): Path<FunctionIdParam>,
    request: Request,
) -> Result<EmptyUpdateStatus, UpdateErrorStatus> {
    let request = UploadFunction::new(function_id_param, request);
    let response = dataset_state
        .upload_function()
        .await
        .oneshot(request)
        .await?;
    Ok(EmptyUpdateStatus::OK(response.into()))
}
