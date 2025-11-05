//
// Copyright 2025 Tabs Data Inc.
//

use td_apiforge::router_ext;

#[router_ext(UserRolesRouter)]
mod routes {
    use axum::Extension;
    use axum::extract::{Path, State};
    use axum_extra::extract::Query;
    use std::sync::Arc;
    use ta_apiserver::status::error_status::ErrorStatus;
    use ta_apiserver::status::extractors::Json;
    use ta_apiserver::status::ok_status::{
        CreateStatus, DeleteStatus, GetStatus, ListStatus, NoContent,
    };
    use ta_services::service::TdService;
    use td_apiforge::apiserver_path;
    use td_objects::dxo::crudl::{ListParams, RequestContext};
    use td_objects::dxo::user_role::defs::{UserRole, UserRoleCreate};
    use td_objects::rest_urls::{
        CREATE_USER_ROLE, DELETE_USER_ROLE, GET_USER_ROLE, LIST_USER_ROLES, RoleParam,
        UserRoleParam,
    };
    use td_services::user_role::services::UserRoleServices;
    use tower::ServiceExt;

    const USER_ROLES_TAG: &str = "User Roles";

    #[apiserver_path(method = post, path = CREATE_USER_ROLE, tag = USER_ROLES_TAG)]
    #[doc = "Add a role for a user"]
    pub async fn create(
        State(state): State<Arc<UserRoleServices>>,
        Extension(context): Extension<RequestContext>,
        Path(role_param): Path<RoleParam>,
        Json(request): Json<UserRoleCreate>,
    ) -> Result<CreateStatus<UserRole>, ErrorStatus> {
        let request = context.create(role_param, request);
        let response = state.create.service().await.oneshot(request).await?;
        Ok(CreateStatus::CREATED(response))
    }

    #[apiserver_path(method = delete, path = DELETE_USER_ROLE, tag = USER_ROLES_TAG)]
    #[doc = "Delete a user from a role"]
    pub async fn delete(
        State(state): State<Arc<UserRoleServices>>,
        Extension(context): Extension<RequestContext>,
        Path(user_role_param): Path<UserRoleParam>,
    ) -> Result<DeleteStatus<NoContent>, ErrorStatus> {
        let request = context.delete(user_role_param);
        let response = state.delete.service().await.oneshot(request).await?;
        Ok(DeleteStatus::OK(response))
    }

    #[apiserver_path(method = get, path = LIST_USER_ROLES, tag = USER_ROLES_TAG)]
    #[doc = "List users for a role"]
    pub async fn list(
        State(state): State<Arc<UserRoleServices>>,
        Extension(context): Extension<RequestContext>,
        Query(query_params): Query<ListParams>,
        Path(role_param): Path<RoleParam>,
    ) -> Result<ListStatus<UserRole>, ErrorStatus> {
        let request = context.list(role_param, query_params);
        let response = state.list.service().await.oneshot(request).await?;
        Ok(ListStatus::OK(response))
    }

    #[apiserver_path(method = get, path = GET_USER_ROLE, tag = USER_ROLES_TAG)]
    #[doc = "Read a user role"]
    pub async fn read(
        State(state): State<Arc<UserRoleServices>>,
        Extension(context): Extension<RequestContext>,
        Path(user_role_param): Path<UserRoleParam>,
    ) -> Result<GetStatus<UserRole>, ErrorStatus> {
        let request = context.read(user_role_param);
        let response = state.read.service().await.oneshot(request).await?;
        Ok(GetStatus::OK(response))
    }
}
