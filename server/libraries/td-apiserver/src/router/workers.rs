//
// Copyright 2025 Tabsdata Inc.
//

//! Worker-message routes. Top-level: worker ids are globally unique, so the
//! services resolve the owning project from the row.

use axum::Extension;
use axum::extract::{Path, State};
use axum_extra::extract::Query;
use ta_apiserver::status::error_status::ErrorStatus;
use ta_apiserver::status::ok_status::ListStatus;
use td_apiforge::apiserver_path;
use td_objects::dxo::crudl::{ListParams, RequestContext};
use td_objects::dxo::file::DownloadResponse;
use td_objects::dxo::worker::{LogsExtensionParam, LogsRetryParam, Worker, WorkerParam};
use td_services::Services;

const WORKERS_TAG: &str = "Workers";

#[apiserver_path(
    version(v1),
    auth(session, service_token),
    method(get),
    path = "/workers",
    tag(WORKERS_TAG),
    doc = "List worker messages"
)]
pub async fn list_workers(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Query(list_params): Query<ListParams>,
) -> Result<ListStatus<Worker>, ErrorStatus> {
    let request = context.list((), list_params);
    let response = state.worker_list(request).await?;
    Ok(ListStatus::OK(response))
}

#[apiserver_path(
    version(v1),
    auth(session, service_token),
    method(get),
    path = "/workers/{worker}/logs",
    tag(WORKERS_TAG),
    content_type = "application/octet-stream",
    doc = "Read worker message logs"
)]
pub async fn logs(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Path(worker): Path<WorkerParam>,
    Query(extension): Query<LogsExtensionParam>,
    Query(retry): Query<LogsRetryParam>,
) -> Result<DownloadResponse, ErrorStatus> {
    let request = context.read((worker, extension, retry));
    let response = state.worker_log(request).await?.data;
    Ok(DownloadResponse::octet_stream(response))
}

#[apiserver_path(
    version(v1),
    auth(session, service_token),
    method(get),
    path = "/workers/{worker}/logs/tail",
    tag(WORKERS_TAG),
    content_type = "application/octet-stream",
    doc = "Tail worker message logs"
)]
pub async fn logs_tail(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Path(worker): Path<WorkerParam>,
    Query(extension): Query<LogsExtensionParam>,
    Query(retry): Query<LogsRetryParam>,
) -> Result<DownloadResponse, ErrorStatus> {
    let request = context.read((worker, extension, retry));
    let response = state.worker_log_tail(request).await?.data;
    Ok(DownloadResponse::octet_stream(response))
}
