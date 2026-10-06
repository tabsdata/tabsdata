//
// Copyright 2026 Tabsdata Inc.
//

//! Function-run routes. The global listing is top-level (run ids are globally
//! unique); the single-run read is addressed the way the user thinks of it —
//! the execution of a named function in a named collection.

use crate::extractors::Project;
use axum::Extension;
use axum::extract::{Path, State};
use axum_extra::extract::Query;
use ta_apiserver::status::error_status::ErrorStatus;
use ta_apiserver::status::ok_status::{GetStatus, ListStatus};
use td_apiforge::apiserver_path;
use td_database::pool::scoped::ScopedExt;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::crudl::{ListParams, RequestContext};
use td_objects::dxo::execution::ExecutionParam;
use td_objects::dxo::file::DownloadResponse;
use td_objects::dxo::function::FunctionParam;
use td_objects::dxo::logical::FunctionRun;
use td_objects::dxo::physical_function_run::{
    FunctionRunParam, InputIdxParam, PhysicalFunctionRun,
};
use td_services::Services;

const FUNCTION_RUNS_TAG: &str = "Function Runs";

// Single listing endpoint. Returns LFR identity rows merged with the
// currently-bound PFR's per-attempt fields (`physical_function_run_id`,
// `started_on`, `ended_on`, …). No separate logical-only endpoint.
#[apiserver_path(
    version(v1),
    auth(session, service_token),
    method(get),
    path = "/function-runs",
    tag(FUNCTION_RUNS_TAG),
    doc = "List function runs (logical identity + currently-bound physical)"
)]
pub async fn list_function_runs(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Query(list_params): Query<ListParams>,
) -> Result<ListStatus<FunctionRun>, ErrorStatus> {
    let request = context.list((), list_params);
    let response = state.function_run_list(request).await?;
    Ok(ListStatus::OK(response))
}

#[apiserver_path(
    version(v1),
    scope(project),
    auth(session, service_token),
    method(get),
    path = "/projects/{project}/collections/{collection}/functions/{function}/executions/{execution}",
    tag(FUNCTION_RUNS_TAG),
    doc = "Read function run"
)]
pub async fn read_run(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Path(collection): Path<CollectionParam>,
    Path(function): Path<FunctionParam>,
    Path(execution): Path<ExecutionParam>,
) -> Result<GetStatus<PhysicalFunctionRun>, ErrorStatus> {
    let request = context
        .read((collection, function, execution))
        .in_project(project_id);
    let response = state.function_run_read(request).await?;
    Ok(GetStatus::OK(response))
}

#[apiserver_path(
    version(v1),
    scope(project),
    auth(session, service_token),
    method(get),
    path = "/projects/{project}/collections/{collection}/function-runs/{function_run}/inputs/{input_idx}/raw-data",
    tag(FUNCTION_RUNS_TAG),
    content_type = "application/vnd.apache.parquet",
    doc = "Download one stamped raw-data file a publisher run ingested"
)]
pub async fn raw_data(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Path(collection): Path<CollectionParam>,
    Path(function_run): Path<FunctionRunParam>,
    Path(input_idx): Path<InputIdxParam>,
) -> Result<DownloadResponse, ErrorStatus> {
    let request = context
        .read((collection, function_run, input_idx))
        .in_project(project_id);
    let stream = state.function_run_raw_data(request).await?.data;
    Ok(DownloadResponse::parquet(stream))
}
