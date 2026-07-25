//
// Copyright 2026 Tabsdata Inc.
//

use td_apiforge::router_ext;

#[router_ext(FunctionRunListRouter)]
mod list_routes {
    use axum::Extension;
    use axum::extract::State;
    use axum_extra::extract::Query;
    use std::sync::Arc;
    use ta_apiserver::status::error_status::ErrorStatus;
    use ta_apiserver::status::ok_status::ListStatus;
    use ta_services::service::TdService;
    use td_apiforge::apiserver_path;
    use td_objects::dxo::crudl::{ListParams, RequestContext};
    use td_objects::dxo::logical::FunctionRun;
    use td_objects::rest_urls::FUNCTION_RUN_LIST;
    use td_services::function_run::services::FunctionRunServices;
    use tower::ServiceExt;

    const FUNCTION_RUNS_TAG: &str = "Function Runs";

    // Single listing endpoint. Returns LFR identity rows merged with the
    // currently-bound PFR's per-attempt fields (`physical_function_run_id`,
    // `started_on`, `ended_on`, …). No separate logical-only endpoint.
    #[apiserver_path(method = get, path = FUNCTION_RUN_LIST, tag = FUNCTION_RUNS_TAG)]
    #[doc = "List function runs (logical identity + currently-bound physical)"]
    pub async fn list(
        State(physical_function_runs): State<Arc<FunctionRunServices>>,
        Extension(context): Extension<RequestContext>,
        Query(query_params): Query<ListParams>,
    ) -> Result<ListStatus<FunctionRun>, ErrorStatus> {
        let request = context.list((), query_params);
        let response = physical_function_runs
            .list
            .service()
            .oneshot(request)
            .await?;
        Ok(ListStatus::OK(response))
    }
}

#[router_ext(FunctionRunsRouter)]
mod routes {
    use crate::extractors::Project;
    use axum::Extension;
    use axum::extract::{Path, State};
    use std::sync::Arc;
    use ta_apiserver::status::error_status::ErrorStatus;
    use ta_apiserver::status::ok_status::GetStatus;
    use ta_services::service::TdService;
    use td_apiforge::apiserver_path;
    use td_database::pool::scoped::InProject;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::physical_function_run::PhysicalFunctionRun;
    use td_objects::rest_urls::{FUNCTION_RUN_GET, PhysicalFunctionRunParam};
    use td_services::function_run::services::FunctionRunServices;
    use tower::ServiceExt;

    const FUNCTION_RUNS_TAG: &str = "Function Runs";

    #[apiserver_path(method = get, path = FUNCTION_RUN_GET, tag = FUNCTION_RUNS_TAG)]
    #[doc = "Read function run"]
    pub async fn read_run(
        State(state): State<Arc<FunctionRunServices>>,
        Extension(context): Extension<RequestContext>,
        Project(project_id): Project,
        Path(param): Path<PhysicalFunctionRunParam>,
    ) -> Result<GetStatus<PhysicalFunctionRun>, ErrorStatus> {
        let request = context.read(param).in_project(project_id);
        let response = state.read.service().oneshot(request).await?;
        Ok(GetStatus::OK(response))
    }
}
