//
// Copyright 2026 Tabs Data Inc.
//

use td_apiforge::router_ext;

#[router_ext(ProjectsRouter)]
mod routes {
    use axum::Extension;
    use axum::extract::{Path, State};
    use axum_extra::extract::Query;
    use std::sync::Arc;
    use ta_apiserver::status::error_status::ErrorStatus;
    use ta_apiserver::status::extractors::Json;
    use ta_apiserver::status::ok_status::{
        CreateStatus, DeleteStatus, GetStatus, ListStatus, NoContent, UpdateStatus,
    };
    use ta_services::service::TdService;
    use td_apiforge::apiserver_path;
    use td_objects::dxo::crudl::{ListParams, RequestContext};
    use td_objects::dxo::project::{ProjectCreate, ProjectRead, ProjectUpdate};
    use td_objects::rest_urls::{
        CREATE_PROJECT, DELETE_PROJECT, GET_PROJECT, LIST_PROJECTS, ProjectParam, UPDATE_PROJECT,
    };
    use td_services::project::service::ProjectServices;
    use tower::ServiceExt;

    const PROJECTS_TAG: &str = "Projects";

    #[apiserver_path(method = get, path = LIST_PROJECTS, tag = PROJECTS_TAG)]
    #[doc = "Lists projects"]
    pub async fn list_projects(
        State(project_state): State<Arc<ProjectServices>>,
        Extension(context): Extension<RequestContext>,
        Query(query_params): Query<ListParams>,
    ) -> Result<ListStatus<ProjectRead>, ErrorStatus> {
        let request = context.list((), query_params);
        let response = project_state.list.service().oneshot(request).await?;
        Ok(ListStatus::OK(response))
    }

    #[apiserver_path(method = get, path = GET_PROJECT, tag = PROJECTS_TAG)]
    #[doc = "Get a project"]
    pub async fn get_project(
        State(project_state): State<Arc<ProjectServices>>,
        Extension(context): Extension<RequestContext>,
        Path(project_param): Path<ProjectParam>,
    ) -> Result<GetStatus<ProjectRead>, ErrorStatus> {
        let request = context.read(project_param);
        let response = project_state.read.service().oneshot(request).await?;
        Ok(GetStatus::OK(response))
    }

    #[apiserver_path(method = post, path = CREATE_PROJECT, tag = PROJECTS_TAG)]
    #[doc = "Create a project"]
    pub async fn create_project(
        State(project_state): State<Arc<ProjectServices>>,
        Extension(context): Extension<RequestContext>,
        Json(request): Json<ProjectCreate>,
    ) -> Result<CreateStatus<ProjectRead>, ErrorStatus> {
        let request = context.create((), request);
        let response = project_state.create.service().oneshot(request).await?;
        Ok(CreateStatus::CREATED(response))
    }

    #[apiserver_path(method = post, path = UPDATE_PROJECT, tag = PROJECTS_TAG)]
    #[doc = "Update a project"]
    pub async fn update_project(
        State(project_state): State<Arc<ProjectServices>>,
        Extension(context): Extension<RequestContext>,
        Path(project_param): Path<ProjectParam>,
        Json(request): Json<ProjectUpdate>,
    ) -> Result<UpdateStatus<ProjectRead>, ErrorStatus> {
        let request = context.update(project_param, request);
        let response = project_state.update.service().oneshot(request).await?;
        Ok(UpdateStatus::OK(response))
    }

    #[apiserver_path(method = delete, path = DELETE_PROJECT, tag = PROJECTS_TAG)]
    #[doc = "Delete a project"]
    pub async fn delete_project(
        State(project_state): State<Arc<ProjectServices>>,
        Extension(context): Extension<RequestContext>,
        Path(project_param): Path<ProjectParam>,
    ) -> Result<DeleteStatus<NoContent>, ErrorStatus> {
        let request = context.delete(project_param);
        let response = project_state.delete.service().oneshot(request).await?;
        Ok(DeleteStatus::OK(response))
    }
}
