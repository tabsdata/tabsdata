//
// Copyright 2025 Tabs Data Inc.
//

use td_apiforge::router_ext;

#[router_ext(InternalRouter)]
mod routes {
    use axum::Extension;
    use axum::extract::{Path, State};
    use axum_extra::extract::Query;
    use std::sync::Arc;
    use ta_apiserver::status::error_status::ErrorStatus;
    use ta_apiserver::status::extractors::Json;
    use ta_apiserver::status::ok_status::{CreateStatus, GetStatus, NoContent, UpdateStatus};
    use ta_services::service::TdService;
    use td_apiforge::apiserver_path;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::execution::{ExecutionRequestInternal, ExecutionResponseInternal};
    use td_objects::dxo::file::DownloadResponse;
    use td_objects::dxo::physical_function_run::PhysicalFunctionRun;
    use td_objects::dxo::worker::CallbackRequest;
    use td_objects::rest_urls::AtTimeParam;
    use td_objects::rest_urls::internal::{
        DOWNLOAD_FUNCTION_STATE, FUNCTION_VERSION_EXECUTE, FUNCTION_VERSION_EXECUTION_STATUS,
        FunctionVersionAtId, FunctionVersionExecution, PhysicalFunctionRunWorkerIdParam,
        UPDATE_FUNCTION_RUN_WORKER, UPDATE_WORKER, WorkerIdParam,
    };
    use td_objects::rest_urls::{ExecutionParam, FunctionVersionIdParam};
    use td_services::execution::services::ExecutionServices;
    use td_services::function_run::services::FunctionRunServices;
    use td_services::table::services::TableServices;
    use td_services::worker::services::WorkerServices;
    use td_tower::ctx_service::RawOneshot;
    use tower::ServiceExt;

    const INTERNAL_TAG: &str = "Internal";

    #[apiserver_path(method = post, path = UPDATE_FUNCTION_RUN_WORKER, tag = INTERNAL_TAG)]
    #[doc = "Callback endpoint for function executions"]
    pub async fn function_run_callback(
        State(state): State<Arc<ExecutionServices>>,
        Extension(context): Extension<RequestContext>,
        Path(param): Path<PhysicalFunctionRunWorkerIdParam>,
        Json(request): Json<CallbackRequest>,
    ) -> Result<UpdateStatus<NoContent>, ErrorStatus> {
        let request = context.update(param, request);
        let response = state.callback.service().oneshot(request).await?;
        Ok(UpdateStatus::OK(response))
    }

    #[apiserver_path(method = post, path = UPDATE_WORKER, tag = INTERNAL_TAG)]
    #[doc = "Callback endpoint for workers"]
    pub async fn worker_callback(
        State(state): State<Arc<WorkerServices>>,
        Extension(context): Extension<RequestContext>,
        Path(param): Path<WorkerIdParam>,
        Json(request): Json<CallbackRequest>,
    ) -> Result<UpdateStatus<NoContent>, ErrorStatus> {
        let request = context.update(param, request);
        let response = state.callback.service().oneshot(request).await?;
        Ok(UpdateStatus::OK(response))
    }

    #[apiserver_path(method = post, path = FUNCTION_VERSION_EXECUTE, tag = INTERNAL_TAG)]
    #[doc = "Executes a function version (must be the current active version)"]
    pub async fn function_version_execute(
        State(state): State<Arc<ExecutionServices>>,
        Extension(context): Extension<RequestContext>,
        Path(function_param): Path<FunctionVersionIdParam>,
        Json(request): Json<ExecutionRequestInternal>,
    ) -> Result<CreateStatus<ExecutionResponseInternal>, ErrorStatus> {
        let request = context.create(function_param, request);
        let response = state.execute_version.service().oneshot(request).await?;
        Ok(CreateStatus::CREATED(response))
    }

    #[apiserver_path(method = get, path = FUNCTION_VERSION_EXECUTION_STATUS, tag = INTERNAL_TAG)]
    #[doc = "Execution status for a given function version and execution plan"]
    pub async fn function_version_execution_status(
        State(state): State<Arc<FunctionRunServices>>,
        Extension(context): Extension<RequestContext>,
        Path(param): Path<FunctionVersionIdParam>,
        Json(request): Json<ExecutionParam>,
    ) -> Result<GetStatus<PhysicalFunctionRun>, ErrorStatus> {
        let request = FunctionVersionExecution {
            function_version_id: param.function_version_id,
            execution: request.execution,
        };
        let request = context.read(request);
        let response = state
            .read_version_execution
            .service()
            .oneshot(request)
            .await?;
        Ok(GetStatus::OK(response))
    }

    #[apiserver_path(
        method = get,
        path = DOWNLOAD_FUNCTION_STATE,
        tag = INTERNAL_TAG,
        content_type = "application/vnd.apache.parquet",
    )]
    #[doc = "Download function state"]
    pub async fn download_state(
        State(state): State<Arc<TableServices>>,
        Extension(context): Extension<RequestContext>,
        Path(function_param): Path<FunctionVersionIdParam>,
        Query(at_param): Query<AtTimeParam>,
    ) -> Result<DownloadResponse, ErrorStatus> {
        let param = FunctionVersionAtId {
            function_version_id: function_param.function_version_id,
            at: at_param.at,
        };
        let request = context.read(param);
        let response = state.download_state.service().raw_oneshot(request).await?;
        Ok(DownloadResponse::parquet(response))
    }
}
