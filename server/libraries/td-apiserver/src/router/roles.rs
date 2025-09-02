//
// Copyright 2025 Tabs Data Inc.
//

use td_apiforge::router_ext;

#[router_ext(RolesRouter)]
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
    use td_apiforge::apiserver_path;
    use td_objects::crudl::{ListParams, RequestContext};
    use td_objects::rest_urls::{
        CREATE_ROLE, DELETE_ROLE, GET_ROLE, LIST_ROLES, RoleParam, UPDATE_ROLE,
    };
    use td_objects::types::role::{Role, RoleCreate, RoleUpdate};
    use td_services::role::services::RoleServices;
    use td_tower::td_service::TdService;
    use tower::ServiceExt;

    const ROLES_TAG: &str = "Roles";

    #[apiserver_path(method = post, path = CREATE_ROLE, tag = ROLES_TAG)]
    #[doc = "Create a role"]
    pub async fn create_role(
        State(state): State<Arc<RoleServices>>,
        Extension(context): Extension<RequestContext>,
        Json(request): Json<RoleCreate>,
    ) -> Result<CreateStatus<Role>, ErrorStatus> {
        let request = context.create((), request);
        let response = state.create().service().await.oneshot(request).await?;
        Ok(CreateStatus::CREATED(response))
    }

    #[apiserver_path(method = delete, path = DELETE_ROLE, tag = ROLES_TAG)]
    #[doc = "Delete a role"]
    pub async fn delete_role(
        State(state): State<Arc<RoleServices>>,
        Extension(context): Extension<RequestContext>,
        Path(role_path): Path<RoleParam>,
    ) -> Result<DeleteStatus<NoContent>, ErrorStatus> {
        let request = context.delete(role_path);
        let response = state.delete().service().await.oneshot(request).await?;
        Ok(DeleteStatus::OK(response))
    }

    #[apiserver_path(method = get, path = LIST_ROLES, tag = ROLES_TAG)]
    #[doc = "List roles"]
    pub async fn list_role(
        State(state): State<Arc<RoleServices>>,
        Extension(context): Extension<RequestContext>,
        Query(query_params): Query<ListParams>,
    ) -> Result<ListStatus<Role>, ErrorStatus> {
        let request = context.list((), query_params);
        let response = state.list().service().await.oneshot(request).await?;
        Ok(ListStatus::OK(response))
    }

    #[apiserver_path(method = get, path = GET_ROLE, tag = ROLES_TAG)]
    #[doc = "Read a role"]
    pub async fn read_role(
        State(state): State<Arc<RoleServices>>,
        Extension(context): Extension<RequestContext>,
        Path(role_param): Path<RoleParam>,
    ) -> Result<GetStatus<Role>, ErrorStatus> {
        let request = context.read(role_param);
        let response = state.read().service().await.oneshot(request).await?;
        Ok(GetStatus::OK(response))
    }

    #[apiserver_path(method = post, path = UPDATE_ROLE, tag = ROLES_TAG)]
    #[doc = "Update a role"]
    pub async fn update_role(
        State(state): State<Arc<RoleServices>>,
        Extension(context): Extension<RequestContext>,
        Path(role_param): Path<RoleParam>,
        Json(request): Json<RoleUpdate>,
    ) -> Result<UpdateStatus<Role>, ErrorStatus> {
        let request = context.update(role_param, request);
        let response = state.update().service().await.oneshot(request).await?;
        Ok(UpdateStatus::OK(response))
    }
}
