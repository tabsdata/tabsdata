//
// Copyright 2025 Tabsdata Inc.
//

use crate::apiserver::BASE_URL;
use axum::Extension;
use axum::extract::{Path, State};
use axum_extra::extract::Query;
use ta_apiserver::status::error_status::ErrorStatus;
use ta_apiserver::status::extractors::Json;
use ta_apiserver::status::ok_status::{CreateStatus, GetStatus, NoContent, UpdateStatus};
use td_apiforge::apiserver_path;
use td_common::execution_status::CallbackResponse;
use td_objects::Route;
use td_objects::dxo::crudl::{AtTimeParam, RequestContext};
use td_objects::dxo::execution::{ExecutionParam, ExecutionResponse, InternalExecutionRequest};
use td_objects::dxo::file::DownloadResponse;
use td_objects::dxo::function::FunctionVersionParam;
use td_objects::dxo::physical_function_run::{FunctionRunParam, PhysicalFunctionRun};
use td_objects::dxo::worker::{CallbackRequest, WorkerParam};
use td_objects::dxo::yaml::WorkerInput;
use td_services::Services;
use td_services::internal_api::InternalRoutes;

const INTERNAL_TAG: &str = "Internal";

#[apiserver_path(
    internal,
    version(v1),
    auth(internal),
    method(post),
    path = "/internal/function-runs/{function_run}/workers/{worker}",
    tag(INTERNAL_TAG),
    doc = "Callback endpoint for function executions"
)]
pub async fn function_run_callback(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Path(function_run_id): Path<FunctionRunParam>,
    Path(worker_id): Path<WorkerParam>,
    Json(request): Json<CallbackRequest>,
) -> Result<UpdateStatus<CallbackResponse>, ErrorStatus> {
    let request = context.update((function_run_id, worker_id), request);
    let response = state.execution_callback(request).await?;
    Ok(UpdateStatus::OK(response))
}

#[apiserver_path(
    internal,
    version(v1),
    auth(internal),
    method(post),
    path = "/internal/workers/{worker}",
    tag(INTERNAL_TAG),
    doc = "Callback endpoint for workers"
)]
pub async fn worker_callback(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Path(worker_id): Path<WorkerParam>,
    Json(request): Json<CallbackRequest>,
) -> Result<UpdateStatus<CallbackResponse>, ErrorStatus> {
    let request = context.update(worker_id, request);
    let response = state.worker_callback(request).await?;
    Ok(UpdateStatus::OK(response))
}

#[apiserver_path(
    internal,
    version(v1),
    auth(internal),
    method(get),
    path = "/internal/workers/{worker}/message",
    tag(INTERNAL_TAG),
    doc = "Resolve and return a worker's message (kubernetes pull path)"
)]
pub async fn worker_message(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Path(worker_id): Path<WorkerParam>,
) -> Result<GetStatus<WorkerInput>, ErrorStatus> {
    let request = context.read(worker_id);
    let response = state.resolve_worker_message(request).await?;
    Ok(GetStatus::OK(response))
}

#[apiserver_path(
    internal,
    version(v1),
    auth(internal),
    method(post),
    path = "/internal/function-versions/{function_version}/execute",
    tag(INTERNAL_TAG),
    doc = "Executes a function version (must be the current active version)"
)]
pub async fn function_version_execute(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Path(function_version_id): Path<FunctionVersionParam>,
    Json(request): Json<InternalExecutionRequest>,
) -> Result<CreateStatus<ExecutionResponse>, ErrorStatus> {
    let request = context.create(function_version_id, request);
    let response = state.execute_function_version(request).await?;
    Ok(CreateStatus::CREATED(response))
}

#[apiserver_path(
    internal,
    version(v1),
    auth(internal),
    method(get),
    path = "/internal/function-versions/{function_version}/execution-status",
    tag(INTERNAL_TAG),
    doc = "Execution status for a given function version and execution plan"
)]
pub async fn function_version_execution_status(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Path(function_version_id): Path<FunctionVersionParam>,
    Json(request): Json<ExecutionParam>,
) -> Result<GetStatus<PhysicalFunctionRun>, ErrorStatus> {
    let request = context.read((function_version_id, request));
    let response = state.function_version_execution_read(request).await?;
    Ok(GetStatus::OK(response))
}

#[apiserver_path(
    internal,
    version(v1),
    auth(internal),
    method(get),
    path = "/internal/function-versions/{function_version}/download-state",
    tag(INTERNAL_TAG),
    content_type = "application/vnd.apache.parquet",
    doc = "Download function state"
)]
pub async fn download_state(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Path(function_version_id): Path<FunctionVersionParam>,
    Query(at): Query<AtTimeParam>,
) -> Result<DownloadResponse, ErrorStatus> {
    let request = context.read((function_version_id, at));
    let response = state.table_state_download(request).await?.data;
    Ok(DownloadResponse::parquet(response))
}

/// The route templates exactly as served, from the generated `*_PATH` consts.
pub fn internal_routes() -> InternalRoutes {
    InternalRoutes {
        base: BASE_URL,
        worker_callback: Route::new(WORKER_CALLBACK_PATH),
        worker_message: Route::new(WORKER_MESSAGE_PATH),
        function_run_callback: Route::new(FUNCTION_RUN_CALLBACK_PATH),
        function_version_execute: Route::new(FUNCTION_VERSION_EXECUTE_PATH),
        function_version_execution_status: Route::new(FUNCTION_VERSION_EXECUTION_STATUS_PATH),
        download_function_state: Route::new(DOWNLOAD_STATE_PATH),
    }
}
