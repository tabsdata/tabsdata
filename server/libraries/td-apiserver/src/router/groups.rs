//
// Copyright 2026 Tabs Data Inc.
//

use td_apiforge::router_ext;

#[router_ext(GroupsRouter)]
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
    use td_objects::dxo::group::{Group, GroupCreate, GroupUpdate};
    use td_objects::rest_urls::{
        CREATE_GROUP, DELETE_GROUP, GET_GROUP, GroupParam, LIST_GROUPS, UPDATE_GROUP,
    };
    use td_objects::types::basic::ProjectId;
    use td_services::group::services::GroupServices;
    use td_database::scoped::InProject;
    use tower::ServiceExt;

    const GROUPS_TAG: &str = "Groups";

    #[apiserver_path(method = post, path = CREATE_GROUP, tag = GROUPS_TAG)]
    #[doc = "Create a group"]
    pub async fn create_group(
        State(state): State<Arc<GroupServices>>,
        Extension(context): Extension<RequestContext>,
        Extension(project_id): Extension<ProjectId>,
        Json(request): Json<GroupCreate>,
    ) -> Result<CreateStatus<Group>, ErrorStatus> {
        let request = context.create((), request).in_project(project_id);
        let response = state.create.service().oneshot(request).await?;
        Ok(CreateStatus::CREATED(response))
    }

    #[apiserver_path(method = delete, path = DELETE_GROUP, tag = GROUPS_TAG)]
    #[doc = "Delete a group"]
    pub async fn delete_group(
        State(state): State<Arc<GroupServices>>,
        Extension(context): Extension<RequestContext>,
        Extension(project_id): Extension<ProjectId>,
        Path(group_path): Path<GroupParam>,
    ) -> Result<DeleteStatus<NoContent>, ErrorStatus> {
        let request = context.delete(group_path).in_project(project_id);
        let response = state.delete.service().oneshot(request).await?;
        Ok(DeleteStatus::OK(response))
    }

    #[apiserver_path(method = get, path = LIST_GROUPS, tag = GROUPS_TAG)]
    #[doc = "List groups"]
    pub async fn list_group(
        State(state): State<Arc<GroupServices>>,
        Extension(context): Extension<RequestContext>,
        Extension(project_id): Extension<ProjectId>,
        Query(query_params): Query<ListParams>,
    ) -> Result<ListStatus<Group>, ErrorStatus> {
        let request = context.list((), query_params).in_project(project_id);
        let response = state.list.service().oneshot(request).await?;
        Ok(ListStatus::OK(response))
    }

    #[apiserver_path(method = get, path = GET_GROUP, tag = GROUPS_TAG)]
    #[doc = "Read a group"]
    pub async fn read_group(
        State(state): State<Arc<GroupServices>>,
        Extension(context): Extension<RequestContext>,
        Extension(project_id): Extension<ProjectId>,
        Path(group_param): Path<GroupParam>,
    ) -> Result<GetStatus<Group>, ErrorStatus> {
        let request = context.read(group_param).in_project(project_id);
        let response = state.read.service().oneshot(request).await?;
        Ok(GetStatus::OK(response))
    }

    #[apiserver_path(method = post, path = UPDATE_GROUP, tag = GROUPS_TAG)]
    #[doc = "Update a group"]
    pub async fn update_group(
        State(state): State<Arc<GroupServices>>,
        Extension(context): Extension<RequestContext>,
        Extension(project_id): Extension<ProjectId>,
        Path(group_param): Path<GroupParam>,
        Json(request): Json<GroupUpdate>,
    ) -> Result<UpdateStatus<Group>, ErrorStatus> {
        let request = context.update(group_param, request).in_project(project_id);
        let response = state.update.service().oneshot(request).await?;
        Ok(UpdateStatus::OK(response))
    }
}
