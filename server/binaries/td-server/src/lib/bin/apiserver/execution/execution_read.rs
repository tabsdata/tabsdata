//
// Copyright 2025 Tabs Data Inc.
//

use crate::bin::apiserver::execution::EXECUTION_TAG;
use crate::bin::apiserver::DatasetsState;
use crate::logic::apiserver::status::error_status::CreateErrorStatus;
use crate::router;
use axum::extract::{Path, State};
use axum::Extension;
use derive_builder::Builder;
use getset::Getters;
use serde::Serialize;
use td_apiforge::{apiserver_path, get_status};
use td_objects::crudl::RequestContext;
use td_objects::datasets::dto::ExecutionPlanRead;
use td_objects::rest_urls::{ExecutionPlanIdParam, EXECUTION_PLAN_GET};
use td_tower::ctx_service::{CtxMap, CtxResponse, CtxResponseBuilder};
use tower::ServiceExt;

router! {
    state => { DatasetsState },
    routes => { read_execution_plan }
}

get_status!(ExecutionPlanRead);

#[apiserver_path(method = get, path = EXECUTION_PLAN_GET, tag = EXECUTION_TAG)]
#[doc = "Reads an execution plan"]
pub async fn read_execution_plan(
    State(dataset_state): State<DatasetsState>,
    Extension(context): Extension<RequestContext>,
    Path(function_param): Path<ExecutionPlanIdParam>,
) -> Result<GetStatus, CreateErrorStatus> {
    let request = context.read(function_param);
    let response = dataset_state
        .read_execution_plan()
        .await
        .oneshot(request)
        .await?;
    Ok(GetStatus::OK(response.into()))
}
