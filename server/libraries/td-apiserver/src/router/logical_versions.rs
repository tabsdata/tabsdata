//
// Copyright 2026 Tabs Data Inc.
//

use td_apiforge::router_ext;

#[router_ext(LogicalVersionsRouter)]
mod routes {
    use axum::Extension;
    use axum::extract::{Path, State};
    use std::sync::Arc;
    use ta_apiserver::status::error_status::ErrorStatus;
    use ta_apiserver::status::ok_status::CreateStatus;
    use ta_services::service::TdService;
    use td_apiforge::apiserver_path;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::logical::LogicalFunctionRunReplayResponse;
    use td_objects::rest_urls::{LOGICAL_FUNCTION_RUN_REPLAY, LogicalFunctionRunParam};
    use td_objects::types::basic::ProjectId;
    use td_services::logical::services::LogicalServices;
    use tower::ServiceExt;

    const LOGICAL_VERSIONS_TAG: &str = "Logical Versions";

    #[apiserver_path(method = post, path = LOGICAL_FUNCTION_RUN_REPLAY, tag = LOGICAL_VERSIONS_TAG)]
    #[doc = "Re-execute a finished LFR — mint a new concrete PhysicalFunctionRun against the latest patch"]
    pub async fn replay(
        State(state): State<Arc<LogicalServices>>,
        Extension(context): Extension<RequestContext>,
        Extension(project_id): Extension<ProjectId>,
        Path(param): Path<LogicalFunctionRunParam>,
    ) -> Result<CreateStatus<LogicalFunctionRunReplayResponse>, ErrorStatus> {
        let request = context.create(param, ()).in_project(project_id);
        let response = state.replay().service().oneshot(request).await?;
        Ok(CreateStatus::CREATED(response))
    }
}
