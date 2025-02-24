//
// Copyright 2024 Tabs Data Inc.
//

//! Dataset API Service for API Server.

#![allow(clippy::upper_case_acronyms)]

use axum::extract::Path;
use axum::routing::delete;

use td_utoipa::api_server_path;

use crate::bin::apisrv::api_server::DatasetsState;
use crate::bin::apisrv::functions::FUNCTIONS_TAG;
use crate::logic::apisrv::status::error_status::DeleteErrorStatus;
use crate::logic::apisrv::status::status_macros::DeleteStatus;
use crate::router;
use td_objects::rest_urls::{FunctionParam, FUNCTION_DELETE};

// TODO(TD-281) add Datasets logic, clean unused code serving as example
router! {
    state => { DatasetsState },
    paths => {{
        FUNCTION_DELETE => delete(delete_dataset),
    }}
}

#[api_server_path(method = delete, path = FUNCTION_DELETE, tag = FUNCTIONS_TAG)]
#[doc = "Delete a function (NOT IMPLEMENTED YET)"]
pub async fn delete_dataset(
    Path(_params): Path<FunctionParam>,
) -> Result<DeleteStatus, DeleteErrorStatus> {
    todo!("Not implemented yet");
    // Ok(DeleteStatus::NO_CONTENT)
}
