//
// Copyright 2025 Tabs Data Inc.
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
    use td_objects::crudl::{ListParams, RequestContext};
    use td_objects::rest_urls::{FUNCTION_RUN_GET, FUNCTION_RUN_LIST, FunctionRunParam};
    use td_objects::types::execution::FunctionRun;
    use td_services::function_run::services::FunctionRunServices;
    use tower::ServiceExt;

    const FUNCTION_RUNS_TAG: &str = "Function Runs";

    #[apiserver_path(method = get, path = FUNCTION_RUN_LIST, tag = FUNCTION_RUNS_TAG)]
    #[doc = "List function runs"]
    pub async fn list(
        State(function_runs): State<Arc<FunctionRunServices>>,
        Extension(context): Extension<RequestContext>,
        Query(query_params): Query<ListParams>,
    ) -> Result<ListStatus<FunctionRun>, ErrorStatus> {
        let request = context.list((), query_params);
        let response = function_runs
            .list()
            .service()
            .await
            .oneshot(request)
            .await?;
        Ok(ListStatus::OK(response))
    }

    #[apiserver_path(method = get, path = FUNCTION_RUN_GET, tag = FUNCTION_RUNS_TAG)]
    #[doc = "Read function run"]
    pub async fn read_run(
        State(state): State<Arc<FunctionRunServices>>,
        Extension(context): Extension<RequestContext>,
        Path(param): Path<FunctionRunParam>,
    ) -> Result<GetStatus<FunctionRun>, ErrorStatus> {
        let request = context.read(param);
        let response = state.read().service().await.oneshot(request).await?;
        Ok(GetStatus::OK(response))
    }
}
