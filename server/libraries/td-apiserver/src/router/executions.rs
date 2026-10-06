//
// Copyright 2025 Tabsdata Inc.
//

//! Execution routes. Top-level (no `{project}` segment): execution ids are
//! globally unique, so the services resolve the owning project from the row and
//! gate authorization on it. Lists span the caller's visible projects, narrowed
//! by `?project=`. The two `list_by_*` routes hang off the version that
//! produced the executions rather than off `/executions`.

use axum::Extension;
use axum::extract::{Path, State};
use axum_extra::extract::Query;
use ta_apiserver::status::error_status::ErrorStatus;
use ta_apiserver::status::ok_status::{GetStatus, ListStatus, NoContent, UpdateStatus};
use td_apiforge::apiserver_path;
use td_objects::dxo::crudl::{ListParams, RequestContext};
use td_objects::dxo::execution::{
    AllowDisabledParam, Execution, ExecutionDetails, ExecutionParam, ExecutionResponse,
};
use td_objects::dxo::function::FunctionVersionParam;
use td_objects::dxo::logical::TableDataVersionParam;
use td_services::Services;

const EXECUTION_TAG: &str = "Execution";

#[apiserver_path(
    version(v1),
    auth(session, service_token),
    method(post),
    path = "/executions/{execution}/cancel",
    tag(EXECUTION_TAG),
    doc = "Cancel all transactions in the given execution"
)]
pub async fn cancel_execution(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Path(execution): Path<ExecutionParam>,
) -> Result<UpdateStatus<NoContent>, ErrorStatus> {
    let request = context.update(execution, ());
    let response = state.execution_cancel(request).await?;
    Ok(UpdateStatus::OK(response))
}

#[apiserver_path(
    version(v1),
    auth(session, service_token),
    method(get),
    path = "/executions/{execution}/details",
    tag(EXECUTION_TAG),
    doc = "Details of an execution"
)]
pub async fn details(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Path(execution): Path<ExecutionParam>,
) -> Result<GetStatus<ExecutionDetails>, ErrorStatus> {
    let request = context.read(execution);
    let response = state.execution_details(request).await?;
    Ok(GetStatus::OK(response))
}

#[apiserver_path(
    version(v1),
    auth(session, service_token),
    method(get),
    path = "/executions",
    tag(EXECUTION_TAG),
    doc = "List executions"
)]
pub async fn lists(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Query(list_params): Query<ListParams>,
) -> Result<ListStatus<Execution>, ErrorStatus> {
    let request = context.list((), list_params);
    let response = state.execution_list(request).await?;
    Ok(ListStatus::OK(response))
}

#[apiserver_path(
    version(v1),
    auth(session, service_token),
    method(get),
    path = "/function-versions/{function_version}/executions",
    tag(EXECUTION_TAG),
    doc = "List executions by function version"
)]
pub async fn list_by_function_version(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Path(function_version_id): Path<FunctionVersionParam>,
    Query(list_params): Query<ListParams>,
) -> Result<ListStatus<Execution>, ErrorStatus> {
    let request = context.list(function_version_id, list_params);
    let response = state.execution_list_by_function_version(request).await?;
    Ok(ListStatus::OK(response))
}

#[apiserver_path(
    version(v1),
    auth(session, service_token),
    method(get),
    path = "/table-data-versions/{table_data_version}/executions",
    tag(EXECUTION_TAG),
    doc = "List executions by table data version"
)]
pub async fn list_by_table_data_version(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Path(table_data_version): Path<TableDataVersionParam>,
    Query(list_params): Query<ListParams>,
) -> Result<ListStatus<Execution>, ErrorStatus> {
    let request = context.list(table_data_version, list_params);
    let response = state.execution_list_by_table_data_version(request).await?;
    Ok(ListStatus::OK(response))
}

#[apiserver_path(
    version(v1),
    auth(session, service_token),
    method(get),
    path = "/executions/{execution}",
    tag(EXECUTION_TAG),
    doc = "Read an execution"
)]
pub async fn read_execution(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Path(execution): Path<ExecutionParam>,
    Query(with_disabled): Query<AllowDisabledParam>,
) -> Result<GetStatus<ExecutionResponse>, ErrorStatus> {
    let request = context.read((execution, with_disabled.with_disabled));
    let response = state.execution_read(request).await?;
    Ok(GetStatus::OK(response))
}

#[apiserver_path(
    version(v1),
    auth(session, service_token),
    method(post),
    path = "/executions/{execution}/recover",
    tag(EXECUTION_TAG),
    doc = "Recover all transactions in the given execution"
)]
pub async fn recover_execution(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Path(execution): Path<ExecutionParam>,
) -> Result<UpdateStatus<NoContent>, ErrorStatus> {
    let request = context.update(execution, ());
    let response = state.execution_recover(request).await?;
    Ok(UpdateStatus::OK(response))
}
