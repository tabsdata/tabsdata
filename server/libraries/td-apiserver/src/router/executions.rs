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
    use td_objects::dxo::execution::{
        Execution, ExecutionDetails, ExecutionRequest, ExecutionResponse,
    };
    use td_objects::rest_urls::{
        AllowDisabledParam, EXECUTION_CANCEL, EXECUTION_DETAILS, EXECUTION_LIST,
        EXECUTION_LIST_BY_FUNCTION_VERSION, EXECUTION_LIST_BY_TABLE_DATA_VERSION, EXECUTION_READ,
        EXECUTION_RECOVER, ExecutionParam, FUNCTION_EXECUTE, FunctionParam, FunctionVersionIdParam,
        PhysicalTableDataVersionParam,
    };
    use td_objects::types::basic::ProjectId;
    use td_services::execution::services::ExecutionServices;
    use tower::ServiceExt;

    const EXECUTION_TAG: &str = "Execution";

    #[apiserver_path(method = post, path = EXECUTION_CANCEL, tag = EXECUTION_TAG)]
    #[doc = "Cancel all transactions in the given execution"]
    pub async fn cancel(
        State(executions): State<Arc<ExecutionServices>>,
        Extension(context): Extension<RequestContext>,
        Extension(project_id): Extension<ProjectId>,
        Path(param): Path<ExecutionParam>,
    ) -> Result<UpdateStatus<NoContent>, ErrorStatus> {
        let request = context.update(param, ()).in_project(project_id);
        let response = executions.cancel.service().oneshot(request).await?;
        Ok(UpdateStatus::OK(response))
    }

    #[apiserver_path(method = get, path = EXECUTION_DETAILS, tag = EXECUTION_TAG)]
    #[doc = "Details of an execution"]
    pub async fn details(
        State(executions): State<Arc<ExecutionServices>>,
        Extension(context): Extension<RequestContext>,
        Extension(project_id): Extension<ProjectId>,
        Path(param): Path<ExecutionParam>,
    ) -> Result<GetStatus<ExecutionDetails>, ErrorStatus> {
        let request = context.read(param).in_project(project_id);
        let response = executions.details.service().oneshot(request).await?;
        Ok(GetStatus::OK(response))
    }

    #[apiserver_path(method = post, path = FUNCTION_EXECUTE, tag = EXECUTION_TAG)]
    #[doc = "Executes a function"]
    pub async fn execute(
        State(executions): State<Arc<ExecutionServices>>,
        Extension(context): Extension<RequestContext>,
        Extension(project_id): Extension<ProjectId>,
        Path(function_param): Path<FunctionParam>,
        Json(request): Json<ExecutionRequest>,
    ) -> Result<CreateStatus<ExecutionResponse>, ErrorStatus> {
        let request = context.create(function_param, request).in_project(project_id);
        let response = executions.execute.service().oneshot(request).await?;
        Ok(CreateStatus::CREATED(response))
    }

    #[apiserver_path(method = get, path = EXECUTION_LIST, tag = EXECUTION_TAG)]
    #[doc = "List executions"]
    pub async fn lists(
        State(executions): State<Arc<ExecutionServices>>,
        Extension(context): Extension<RequestContext>,
        Extension(project_id): Extension<ProjectId>,
        Query(query_params): Query<ListParams>,
    ) -> Result<ListStatus<Execution>, ErrorStatus> {
        let request = context.list((), query_params).in_project(project_id);
        let response = executions.list.service().oneshot(request).await?;
        Ok(ListStatus::OK(response))
    }

    #[apiserver_path(method = get, path = EXECUTION_LIST_BY_FUNCTION_VERSION, tag = EXECUTION_TAG)]
    #[doc = "List executions by function version"]
    pub async fn list_by_function_version(
        State(executions): State<Arc<ExecutionServices>>,
        Extension(context): Extension<RequestContext>,
        Extension(project_id): Extension<ProjectId>,
        Path(param): Path<FunctionVersionIdParam>,
        Query(query_params): Query<ListParams>,
    ) -> Result<ListStatus<Execution>, ErrorStatus> {
        let request = context.list(param, query_params).in_project(project_id);
        let response = executions
            .list_by_function_version
            .service()
            .oneshot(request)
            .await?;
        Ok(ListStatus::OK(response))
    }

    #[apiserver_path(method = get, path = EXECUTION_LIST_BY_TABLE_DATA_VERSION, tag = EXECUTION_TAG)]
    #[doc = "List executions by table data version"]
    pub async fn list_by_physical_table_data_version(
        State(executions): State<Arc<ExecutionServices>>,
        Extension(context): Extension<RequestContext>,
        Extension(project_id): Extension<ProjectId>,
        Path(param): Path<PhysicalTableDataVersionParam>,
        Query(query_params): Query<ListParams>,
    ) -> Result<ListStatus<Execution>, ErrorStatus> {
        let request = context.list(param, query_params).in_project(project_id);
        let response = executions
            .list_by_physical_table_data_version
            .service()
            .oneshot(request)
            .await?;
        Ok(ListStatus::OK(response))
    }

    #[apiserver_path(method = get, path = EXECUTION_READ, tag = EXECUTION_TAG)]
    #[doc = "Read an execution"]
    pub async fn read(
        State(executions): State<Arc<ExecutionServices>>,
        Extension(context): Extension<RequestContext>,
        Extension(project_id): Extension<ProjectId>,
        Path(param): Path<ExecutionParam>,
        Query(allow_disabled): Query<AllowDisabledParam>,
    ) -> Result<GetStatus<ExecutionResponse>, ErrorStatus> {
        let request = context.read(param).in_project(project_id);
        let service = if *allow_disabled.with_disabled {
            executions.read_with_disabled.service()
        } else {
            executions.read.service()
        };
        let response = service.oneshot(request).await?;
        Ok(GetStatus::OK(response))
    }

    #[apiserver_path(method = post, path = EXECUTION_RECOVER, tag = EXECUTION_TAG)]
    #[doc = "Recover all transactions in the given execution"]
    pub async fn recover(
        State(executions): State<Arc<ExecutionServices>>,
        Extension(context): Extension<RequestContext>,
        Extension(project_id): Extension<ProjectId>,
        Path(param): Path<ExecutionParam>,
    ) -> Result<UpdateStatus<NoContent>, ErrorStatus> {
        let request = context.update(param, ()).in_project(project_id);
        let response = executions.recover.service().oneshot(request).await?;
        Ok(UpdateStatus::OK(response))
    }
}
