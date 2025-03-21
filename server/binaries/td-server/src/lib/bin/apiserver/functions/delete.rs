//
// Copyright 2024 Tabs Data Inc.
//

//! Dataset API Service for API Server.

#![allow(clippy::upper_case_acronyms)]

use crate::bin::apiserver::functions::FUNCTIONS_TAG;
use crate::bin::apiserver::DatasetsState;
use crate::logic::apiserver::status::error_status::DeleteErrorStatus;
use crate::logic::apiserver::status::DeleteStatus;
use crate::router;
use axum::extract::Path;
use td_apiforge::apiserver_path;
use td_objects::rest_urls::{FunctionParam, FUNCTION_DELETE};

// TODO(TD-281) add Datasets logic, clean unused code serving as example
router! {
    state => { DatasetsState },
    routes => { delete_dataset }
}

#[apiserver_path(method = delete, path = FUNCTION_DELETE, tag = FUNCTIONS_TAG)]
#[doc = "Delete a function (NOT IMPLEMENTED YET)"]
pub async fn delete_dataset(
    Path(_params): Path<FunctionParam>,
) -> Result<DeleteStatus, DeleteErrorStatus> {
    todo!("Not implemented yet");
    // Ok(DeleteStatus::NO_CONTENT)
}
