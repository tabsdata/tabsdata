//
// Copyright 2025 Tabs Data Inc.
//

use td_apiforge::router_ext;

#[router_ext(PermissionsRouter)]
mod routes {
    use axum::Extension;
    use axum::extract::{Path, State};
    use axum_extra::extract::Query;
    use std::sync::Arc;
    use ta_apiserver::status::error_status::ErrorStatus;
    use ta_apiserver::status::extractors::Json;
    use ta_apiserver::status::ok_status::{CreateStatus, DeleteStatus, ListStatus, NoContent};
    use ta_services::service::TdService;
    use td_apiforge::apiserver_path;
    use td_objects::crudl::{ListParams, RequestContext};
    use td_objects::rest_urls::{
        CREATE_PERMISSION, DELETE_PERMISSION, LIST_PERMISSIONS, RoleParam, RolePermissionParam,
    };
    use td_objects::types::permission::{Permission, PermissionCreate};
    use td_services::permission::services::PermissionServices;
    use tower::ServiceExt;

    const PERMISSIONS_TAG: &str = "Permissions";

    #[apiserver_path(method = post, path = CREATE_PERMISSION, tag = PERMISSIONS_TAG)]
    #[doc = "Create a permission"]
    pub async fn create(
        State(state): State<Arc<PermissionServices>>,
        Extension(context): Extension<RequestContext>,
        Path(role_param): Path<RoleParam>,
        Json(request): Json<PermissionCreate>,
    ) -> Result<CreateStatus<Permission>, ErrorStatus> {
        let request = context.create(role_param, request);
        let response = state.create().service().await.oneshot(request).await?;
        Ok(CreateStatus::CREATED(response))
    }

    #[apiserver_path(method = delete, path = DELETE_PERMISSION, tag = PERMISSIONS_TAG)]
    #[doc = "Delete a permission"]
    pub async fn delete(
        State(state): State<Arc<PermissionServices>>,
        Extension(context): Extension<RequestContext>,
        Path(param): Path<RolePermissionParam>,
    ) -> Result<DeleteStatus<NoContent>, ErrorStatus> {
        let request = context.delete(param);
        let response = state.delete().service().await.oneshot(request).await?;
        Ok(DeleteStatus::OK(response))
    }

    #[apiserver_path(method = get, path = LIST_PERMISSIONS, tag = PERMISSIONS_TAG)]
    #[doc = "List permissions"]
    pub async fn list(
        State(state): State<Arc<PermissionServices>>,
        Extension(context): Extension<RequestContext>,
        Query(query_params): Query<ListParams>,
        Path(path_params): Path<RoleParam>,
    ) -> Result<ListStatus<Permission>, ErrorStatus> {
        let request = context.list(path_params, query_params);
        let response = state.list().service().await.oneshot(request).await?;
        Ok(ListStatus::OK(response))
    }
}
