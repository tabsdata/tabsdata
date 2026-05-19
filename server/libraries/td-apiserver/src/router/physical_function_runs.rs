//
// Copyright 2025 Tabs Data Inc.
//

use td_apiforge::router_ext;

#[router_ext(PhysicalFunctionRunsRouter)]
mod routes {
    use axum::Extension;
    use axum::extract::{Path, State};
    use axum_extra::extract::Query;
    use std::sync::Arc;
    use ta_apiserver::status::error_status::ErrorStatus;
    use ta_apiserver::status::ok_status::{GetStatus, ListStatus};
    use ta_services::service::TdService;
    use td_apiforge::apiserver_path;
    use td_objects::dxo::crudl::{ListParams, RequestContext};
    use td_objects::dxo::logical::LogicalFunctionRun;
    use td_objects::dxo::physical_function_run::PhysicalFunctionRun;
    use td_objects::rest_urls::{
        FUNCTION_RUN_GET, FUNCTION_RUN_LIST, LOGICAL_FUNCTION_RUN_LIST, PhysicalFunctionRunParam,
    };
    use td_services::function_run::services::PhysicalFunctionRunServices;
    use tower::ServiceExt;

    const FUNCTION_RUNS_TAG: &str = "Function Runs";

    #[apiserver_path(method = get, path = FUNCTION_RUN_LIST, tag = FUNCTION_RUNS_TAG)]
    #[doc = "List function runs (per-attempt audit)"]
    pub async fn list(
        State(physical_function_runs): State<Arc<PhysicalFunctionRunServices>>,
        Extension(context): Extension<RequestContext>,
        Query(query_params): Query<ListParams>,
    ) -> Result<ListStatus<PhysicalFunctionRun>, ErrorStatus> {
        let request = context.list((), query_params);
        let response = physical_function_runs
            .list
            .service()
            .oneshot(request)
            .await?;
        Ok(ListStatus::OK(response))
    }

    #[apiserver_path(method = get, path = LOGICAL_FUNCTION_RUN_LIST, tag = FUNCTION_RUNS_TAG)]
    #[doc = "List logical function runs (canonical flow-control status)"]
    pub async fn list_logical(
        State(physical_function_runs): State<Arc<PhysicalFunctionRunServices>>,
        Extension(context): Extension<RequestContext>,
        Query(query_params): Query<ListParams>,
    ) -> Result<ListStatus<LogicalFunctionRun>, ErrorStatus> {
        let request = context.list((), query_params);
        let response = physical_function_runs
            .list_logical
            .service()
            .oneshot(request)
            .await?;
        Ok(ListStatus::OK(response))
    }

    #[apiserver_path(method = get, path = FUNCTION_RUN_GET, tag = FUNCTION_RUNS_TAG)]
    #[doc = "Read function run"]
    pub async fn read_run(
        State(state): State<Arc<PhysicalFunctionRunServices>>,
        Extension(context): Extension<RequestContext>,
        Path(param): Path<PhysicalFunctionRunParam>,
    ) -> Result<GetStatus<PhysicalFunctionRun>, ErrorStatus> {
        let request = context.read(param);
        let response = state.read.service().oneshot(request).await?;
        Ok(GetStatus::OK(response))
    }
}
