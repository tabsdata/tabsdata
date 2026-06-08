//
// Copyright 2025 Tabs Data Inc.
//

use td_apiforge::router_ext;

#[router_ext(WorkersRouter)]
mod routes {
    use axum::Extension;
    use axum::extract::{Path, State};
    use axum_extra::extract::Query;
    use std::sync::Arc;
    use ta_apiserver::status::error_status::ErrorStatus;
    use ta_apiserver::status::ok_status::ListStatus;
    use ta_services::service::TdService;
    use td_apiforge::apiserver_path;
    use td_objects::dxo::crudl::{ListParams, RequestContext};
    use td_objects::dxo::file::DownloadResponse;
    use td_objects::dxo::worker::Worker;
    use td_objects::rest_urls::{
        WORKER_LOGS, WORKERS_LIST, WorkerLogsParams, WorkerLogsQueryParams, WorkerParam,
    };
    use td_objects::types::basic::ProjectId;
    use td_services::worker::services::WorkerServices;
    use td_tower::ctx_service::RawOneshot;
    use td_tower::project_scope::InProject;
    use tower::ServiceExt;

    const WORKERS_TAG: &str = "Workers";

    #[apiserver_path(method = get, path = WORKERS_LIST, tag = WORKERS_TAG)]
    #[doc = "List worker messages"]
    pub async fn list(
        State(messages): State<Arc<WorkerServices>>,
        Extension(context): Extension<RequestContext>,
        Extension(project_id): Extension<ProjectId>,
        Query(query_params): Query<ListParams>,
    ) -> Result<ListStatus<Worker>, ErrorStatus> {
        let request = context.list((), query_params).in_project(project_id);
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
        Extension(project_id): Extension<ProjectId>,
        Path(path_params): Path<WorkerParam>,
        Query(query_params): Query<WorkerLogsQueryParams>,
    ) -> Result<DownloadResponse, ErrorStatus> {
        let params = WorkerLogsParams::new(path_params, query_params);
        let request = context.read(params).in_project(project_id);
        let response = messages.logs.service().raw_oneshot(request).await?;
        Ok(DownloadResponse::octet_stream(response))
    }
}
