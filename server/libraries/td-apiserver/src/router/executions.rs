//
// Copyright 2025 Tabs Data Inc.
//

use td_apiforge::router_ext;

#[router_ext(ExecutionsRouter)]
mod routes {
    use axum::Extension;
    use axum::extract::{Path, State};
    use axum_extra::extract::Query;
    use std::sync::Arc;
    use ta_apiserver::status::error_status::ErrorStatus;
    use ta_apiserver::status::extractors::Json;
    use ta_apiserver::status::ok_status::{
        CreateStatus, GetStatus, ListStatus, NoContent, UpdateStatus,
    };
    use ta_services::service::TdService;
    use td_apiforge::apiserver_path;
    use td_objects::dxo::crudl::{ListParams, RequestContext};
    use td_objects::dxo::execution::defs::{
        Execution, ExecutionDetails, ExecutionRequest, ExecutionResponse,
    };
    use td_objects::rest_urls::{
        EXECUTION_CANCEL, EXECUTION_DETAILS, EXECUTION_LIST, EXECUTION_READ, EXECUTION_RECOVER,
        ExecutionParam, FUNCTION_EXECUTE, FunctionParam,
    };
    use td_services::execution::services::ExecutionServices;
    use tower::ServiceExt;

    const EXECUTION_TAG: &str = "Execution";

    #[apiserver_path(method = post, path = EXECUTION_CANCEL, tag = EXECUTION_TAG)]
    #[doc = "Cancel all transactions in the given execution"]
    pub async fn cancel(
        State(executions): State<Arc<ExecutionServices>>,
        Extension(context): Extension<RequestContext>,
        Path(param): Path<ExecutionParam>,
    ) -> Result<UpdateStatus<NoContent>, ErrorStatus> {
        let request = context.update(param, ());
        let response = executions.cancel.service().await.oneshot(request).await?;
        Ok(UpdateStatus::OK(response))
    }

    #[apiserver_path(method = get, path = EXECUTION_DETAILS, tag = EXECUTION_TAG)]
    #[doc = "Details of an execution"]
    pub async fn details(
        State(executions): State<Arc<ExecutionServices>>,
        Extension(context): Extension<RequestContext>,
        Path(param): Path<ExecutionParam>,
    ) -> Result<GetStatus<ExecutionDetails>, ErrorStatus> {
        let request = context.read(param);
        let response = executions.details.service().await.oneshot(request).await?;
        Ok(GetStatus::OK(response))
    }

    #[apiserver_path(method = post, path = FUNCTION_EXECUTE, tag = EXECUTION_TAG)]
    #[doc = "Executes a function"]
    pub async fn execute(
        State(executions): State<Arc<ExecutionServices>>,
        Extension(context): Extension<RequestContext>,
        Path(function_param): Path<FunctionParam>,
        Json(request): Json<ExecutionRequest>,
    ) -> Result<CreateStatus<ExecutionResponse>, ErrorStatus> {
        let request = context.create(function_param, request);
        let response = executions.execute.service().await.oneshot(request).await?;
        Ok(CreateStatus::CREATED(response))
    }

    #[apiserver_path(method = get, path = EXECUTION_LIST, tag = EXECUTION_TAG)]
    #[doc = "List executions"]
    pub async fn lists(
        State(executions): State<Arc<ExecutionServices>>,
        Extension(context): Extension<RequestContext>,
        Query(query_params): Query<ListParams>,
    ) -> Result<ListStatus<Execution>, ErrorStatus> {
        let request = context.list((), query_params);
        let response = executions.list.service().await.oneshot(request).await?;
        Ok(ListStatus::OK(response))
    }

    #[apiserver_path(method = get, path = EXECUTION_READ, tag = EXECUTION_TAG)]
    #[doc = "Read an execution"]
    pub async fn read(
        State(executions): State<Arc<ExecutionServices>>,
        Extension(context): Extension<RequestContext>,
        Path(param): Path<ExecutionParam>,
    ) -> Result<GetStatus<ExecutionResponse>, ErrorStatus> {
        let request = context.read(param);
        let response = executions.read.service().await.oneshot(request).await?;
        Ok(GetStatus::OK(response))
    }

    #[apiserver_path(method = post, path = EXECUTION_RECOVER, tag = EXECUTION_TAG)]
    #[doc = "Recover all transactions in the given execution"]
    pub async fn recover(
        State(executions): State<Arc<ExecutionServices>>,
        Extension(context): Extension<RequestContext>,
        Path(param): Path<ExecutionParam>,
    ) -> Result<UpdateStatus<NoContent>, ErrorStatus> {
        let request = context.update(param, ());
        let response = executions.recover.service().await.oneshot(request).await?;
        Ok(UpdateStatus::OK(response))
    }
}
