//
//   Copyright 2024 Tabs Data Inc.
//

use crate::bin::apisrv::api_server::DatasetsState;
use crate::bin::apisrv::execution::EXECUTION_TAG;
use crate::logic::apisrv::status::error_status::CreateErrorStatus;
use crate::logic::apisrv::status::extractors::Json;
use crate::{create_status, router};
use axum::extract::{Path, State};
use axum::routing::post;
use axum::Extension;
use td_objects::crudl::RequestContext;
use td_objects::datasets::dto::{ExecutionPlanRead, ExecutionPlanWrite};
use td_objects::rest_urls::{FunctionParam, FUNCTION_EXECUTE};
use td_utoipa::api_server_path;
use tower::ServiceExt;

router! {
    state => { DatasetsState },
    paths => {{
        FUNCTION_EXECUTE => post(execute),
    }}
}

create_status!(ExecutionPlanRead);

#[api_server_path(method = post, path = FUNCTION_EXECUTE, tag = EXECUTION_TAG)]
#[doc = "Executes a function"]
pub async fn execute(
    State(dataset_state): State<DatasetsState>,
    Extension(context): Extension<RequestContext>,
    Path(function_param): Path<FunctionParam>,
    Json(request): Json<ExecutionPlanWrite>,
) -> Result<CreateStatus, CreateErrorStatus> {
    let request = context.create(function_param, request);
    let response = dataset_state
        .create_execution_plan()
        .await
        .oneshot(request)
        .await?;
    Ok(CreateStatus::CREATED(response))
}
