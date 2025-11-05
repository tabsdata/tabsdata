//
// Copyright 2025 Tabs Data Inc.
//

use td_apiforge::router_ext;

#[router_ext(WorkersRouter)]
mod routes {
    use axum::Extension;
    use axum::extract::{Path, State};
    use axum::response::IntoResponse;
    use axum_extra::extract::Query;
    #[allow(unused_imports)]
    use serde_json::json;
    use std::sync::Arc;
    use ta_apiserver::status::error_status::ErrorStatus;
    use ta_apiserver::status::ok_status::ListStatus;
    use ta_services::service::TdService;
    use td_apiforge::apiserver_path;
    use td_objects::dxo::crudl::{ListParams, RequestContext};
    use td_objects::dxo::worker::defs::Worker;
    use td_objects::rest_urls::{
        WORKER_LOGS, WORKERS_LIST, WorkerLogsParams, WorkerLogsQueryParams, WorkerParam,
    };
    use td_objects::stream::BoxedSyncStream;
    use td_services::worker::services::WorkerServices;
    use td_tower::ctx_service::RawOneshot;
    use tower::ServiceExt;
    use utoipa::IntoResponses;

    const WORKERS_TAG: &str = "Workers";

    #[apiserver_path(method = get, path = WORKERS_LIST, tag = WORKERS_TAG)]
    #[doc = "List worker messages"]
    pub async fn list(
        State(messages): State<Arc<WorkerServices>>,
        Extension(context): Extension<RequestContext>,
        Query(query_params): Query<ListParams>,
    ) -> Result<ListStatus<Worker>, ErrorStatus> {
        let request = context.list((), query_params);
        let response = messages.list.service().await.oneshot(request).await?;
        Ok(ListStatus::OK(response))
    }

    /// This struct is just used to document ParquetFile in the OpenAPI schema.
    /// The server is just returning a stream of bytes, so we need to specify the content type.
    #[allow(dead_code)]
    #[derive(utoipa::ToSchema, IntoResponses)]
    #[response(
        status = 200,
        description = "OK",
        example = json!([]),
        content_type = "application/octet-stream"
    )]
    pub struct LogsFile(BoxedSyncStream);

    impl IntoResponse for LogsFile {
        fn into_response(self) -> axum::response::Response {
            self.0.into_response()
        }
    }

    #[apiserver_path(method = get, path = WORKER_LOGS, tag = WORKERS_TAG)]
    #[doc = "Read worker message logs"]
    pub async fn logs(
        State(messages): State<Arc<WorkerServices>>,
        Extension(context): Extension<RequestContext>,
        Path(path_params): Path<WorkerParam>,
        Query(query_params): Query<WorkerLogsQueryParams>,
    ) -> Result<LogsFile, ErrorStatus> {
        let params = WorkerLogsParams::new(path_params, query_params);
        let request = context.read(params);
        let response = messages.logs.service().await.raw_oneshot(request).await?;
        Ok(LogsFile(response))
    }
}
