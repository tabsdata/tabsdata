//
// Copyright 2026 Tabsdata Inc.
//

//! Simple Security Mode permission facade router. Flat per-user
//! permission grant/revoke/list endpoints that translate to RBAC
//! role-membership operations. Mounted alongside the RBAC `/roles/.../permissions`
//! surface — both views operate on the same tables.

use td_apiforge::router_ext;

#[router_ext(SimplePermissionsRouter)]
mod routes {
    use axum::Extension;
    use axum::extract::State;
    use axum_extra::extract::Query;
    use std::sync::Arc;
    use ta_apiserver::status::error_status::ErrorStatus;
    use ta_apiserver::status::extractors::Json;
    use ta_apiserver::status::ok_status::{CreateStatus, DeleteStatus, ListStatus, NoContent};
    use ta_services::service::TdService;
    use td_apiforge::apiserver_path;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::simple_permission::{
        ListByProjectRequest, ListByUserRequest, SimplePermission, SimplePermissionGrant,
        SimpleProjectAccess,
    };
    use td_objects::rest_urls::{
        GRANT_PERMISSION, LIST_PERMISSION_GRANTS_BY_PROJECT, LIST_PERMISSION_GRANTS_BY_USER,
        PermissionGrantProjectQuery, PermissionGrantUserQuery, REVOKE_PERMISSION,
    };
    use td_services::simple_permission::services::SimplePermissionServices;
    use tower::ServiceExt;

    const SIMPLE_PERMISSIONS_TAG: &str = "Permissions (Simple)";

    #[apiserver_path(method = post, path = GRANT_PERMISSION, tag = SIMPLE_PERMISSIONS_TAG)]
    #[doc = "Grant a flat permission to a user (Simple Security Mode)"]
    pub async fn grant(
        State(state): State<Arc<SimplePermissionServices>>,
        Extension(context): Extension<RequestContext>,
        Json(request): Json<SimplePermissionGrant>,
    ) -> Result<CreateStatus<NoContent>, ErrorStatus> {
        let request = context.create((), request);
        let response = state.grant.service().oneshot(request).await?;
        Ok(CreateStatus::CREATED(response))
    }

    #[apiserver_path(method = delete, path = REVOKE_PERMISSION, tag = SIMPLE_PERMISSIONS_TAG)]
    #[doc = "Revoke a flat permission from a user (Simple Security Mode)"]
    pub async fn revoke(
        State(state): State<Arc<SimplePermissionServices>>,
        Extension(context): Extension<RequestContext>,
        Json(request): Json<SimplePermissionGrant>,
    ) -> Result<DeleteStatus<NoContent>, ErrorStatus> {
        let request = context.delete(request);
        let response = state.revoke.service().oneshot(request).await?;
        Ok(DeleteStatus::OK(response))
    }

    #[apiserver_path(
        method = get,
        path = LIST_PERMISSION_GRANTS_BY_USER,
        tag = SIMPLE_PERMISSIONS_TAG
    )]
    #[doc = "List a user's effective flat permissions (Simple Security Mode)"]
    pub async fn list_by_user(
        State(state): State<Arc<SimplePermissionServices>>,
        Extension(context): Extension<RequestContext>,
        Query(query): Query<PermissionGrantUserQuery>,
    ) -> Result<ListStatus<SimplePermission>, ErrorStatus> {
        let request = ListByUserRequest::new(context, query);
        let response = state.list_by_user.service().oneshot(request).await?;
        Ok(ListStatus::OK(response))
    }

    #[apiserver_path(
        method = get,
        path = LIST_PERMISSION_GRANTS_BY_PROJECT,
        tag = SIMPLE_PERMISSIONS_TAG
    )]
    #[doc = "List everyone with authority on a project (Simple Security Mode)"]
    pub async fn list_by_project(
        State(state): State<Arc<SimplePermissionServices>>,
        Extension(context): Extension<RequestContext>,
        Query(query): Query<PermissionGrantProjectQuery>,
    ) -> Result<ListStatus<SimpleProjectAccess>, ErrorStatus> {
        let request = ListByProjectRequest::new(context, query);
        let response = state.list_by_project.service().oneshot(request).await?;
        Ok(ListStatus::OK(response))
    }
}
