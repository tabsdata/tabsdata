//
// Copyright 2026 Tabs Data Inc.
//

use td_apiforge::router_ext;

#[router_ext(FunctionRunsRouter)]
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
    use td_objects::dxo::logical::FunctionRun;
    use td_objects::dxo::physical_function_run::PhysicalFunctionRun;
    use td_objects::rest_urls::{FUNCTION_RUN_GET, FUNCTION_RUN_LIST, PhysicalFunctionRunParam};
    use td_objects::types::basic::ProjectId;
    use td_services::function_run::services::FunctionRunServices;
    use td_tower::project_scope::InProject;
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
        Extension(project_id): Extension<ProjectId>,
        Query(query_params): Query<ListParams>,
    ) -> Result<ListStatus<FunctionRun>, ErrorStatus> {
        let request = context.list((), query_params).in_project(project_id);
        let response = physical_function_runs
            .list
            .service()
            .oneshot(request)
            .await?;
        Ok(ListStatus::OK(response))
    }

    #[apiserver_path(method = get, path = FUNCTION_RUN_GET, tag = FUNCTION_RUNS_TAG)]
    #[doc = "Read function run"]
    pub async fn read_run(
        State(state): State<Arc<FunctionRunServices>>,
        Extension(context): Extension<RequestContext>,
        Extension(project_id): Extension<ProjectId>,
        Path(param): Path<PhysicalFunctionRunParam>,
    ) -> Result<GetStatus<PhysicalFunctionRun>, ErrorStatus> {
        let request = context.read(param).in_project(project_id);
        let response = state.read.service().oneshot(request).await?;
        Ok(GetStatus::OK(response))
    }
}
