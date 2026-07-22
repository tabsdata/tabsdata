//
// Copyright 2025 Tabsdata Inc.
//

use td_apiforge::router_ext;

#[router_ext(WorkersRouter)]
mod routes {
    use crate::extractors::{Project, WildcardProject};
    use axum::Extension;
    use axum::extract::{Path, State};
    use axum_extra::extract::Query;
    use std::sync::Arc;
    use ta_apiserver::status::error_status::ErrorStatus;
    use ta_apiserver::status::ok_status::ListStatus;
    use ta_services::service::TdService;
    use td_apiforge::apiserver_path;
    use td_database::pool::scoped::InProject;
    use td_objects::dxo::crudl::{ListParams, RequestContext};
    use td_objects::dxo::file::DownloadResponse;
    use td_objects::dxo::worker::Worker;
    use td_objects::rest_urls::{
        WORKER_LOGS, WORKER_LOGS_TAIL, WORKERS_LIST, WorkerLogsParams, WorkerLogsQueryParams,
        WorkerParam,
    };
    use td_services::worker::services::WorkerServices;
    use td_tower::ctx_service::RawOneshot;
    use tower::ServiceExt;

    const WORKERS_TAG: &str = "Workers";

    #[apiserver_path(method = get, path = WORKERS_LIST, tag = WORKERS_TAG)]
    #[doc = "List worker messages"]
    pub async fn list(
        State(messages): State<Arc<WorkerServices>>,
        Extension(context): Extension<RequestContext>,
        WildcardProject(selector): WildcardProject,
        Query(query_params): Query<ListParams>,
    ) -> Result<ListStatus<Worker>, ErrorStatus> {
        let request = context.list((), query_params).in_project(selector);
        let response = messages.list.service().oneshot(request).await?;
        Ok(ListStatus::OK(response))
    }

    #[apiserver_path(
        method = get,
        path = WORKER_LOGS,
        tag = WORKERS_TAG,
        content_type = "application/octet-stream",
    )]
    #[doc = "Read worker message logs"]
    pub async fn logs(
        State(messages): State<Arc<WorkerServices>>,
        Extension(context): Extension<RequestContext>,
        Project(project_id): Project,
        Path(path_params): Path<WorkerParam>,
        Query(query_params): Query<WorkerLogsQueryParams>,
    ) -> Result<DownloadResponse, ErrorStatus> {
        let params = WorkerLogsParams::new(path_params, query_params);
        let request = context.read(params).in_project(project_id);
        let response = messages.logs.service().raw_oneshot(request).await?;
        Ok(DownloadResponse::octet_stream(response))
    }

    #[apiserver_path(
        method = get,
        path = WORKER_LOGS_TAIL,
        tag = WORKERS_TAG,
        content_type = "application/octet-stream",
    )]
    #[doc = "Tail worker message logs"]
    pub async fn logs_tail(
        State(messages): State<Arc<WorkerServices>>,
        Extension(context): Extension<RequestContext>,
        Project(project_id): Project,
        Path(path_params): Path<WorkerParam>,
        Query(query_params): Query<WorkerLogsQueryParams>,
    ) -> Result<DownloadResponse, ErrorStatus> {
        let params = WorkerLogsParams::new(path_params, query_params);
        let request = context.read(params).in_project(project_id);
        let response = messages.logs_tail.service().raw_oneshot(request).await?;
        Ok(DownloadResponse::octet_stream(response))
    }
}
