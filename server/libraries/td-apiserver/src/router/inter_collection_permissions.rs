//
// Copyright 2025 Tabs Data Inc.
//

use td_apiforge::router_ext;

#[router_ext(InterCollectionPermissionsRouter)]
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
    use td_objects::dxo::crudl::{ListParams, RequestContext};
    use td_objects::dxo::inter_collection_permission::defs::{
        InterCollectionPermission, InterCollectionPermissionCreate,
    };
    use td_objects::rest_urls::{
        CREATE_INTER_COLLECTION_PERMISSION, CollectionParam, DELETE_INTER_COLLECTION_PERMISSION,
        InterCollectionPermissionParam, LIST_INTER_COLLECTION_PERMISSIONS,
    };
    use td_services::inter_coll_permission::services::InterCollectionPermissionServices;
    use tower::ServiceExt;

    const INTER_COLLECTION_PERMISSIONS_TAG: &str = "Inter Collection Permissions";

    #[apiserver_path(method = post, path = CREATE_INTER_COLLECTION_PERMISSION, tag = INTER_COLLECTION_PERMISSIONS_TAG)]
    #[doc = "Create an inter collection permission"]
    pub async fn create(
        State(state): State<Arc<InterCollectionPermissionServices>>,
        Extension(context): Extension<RequestContext>,
        Path(role_param): Path<CollectionParam>,
        Json(request): Json<InterCollectionPermissionCreate>,
    ) -> Result<CreateStatus<InterCollectionPermission>, ErrorStatus> {
        let request = context.create(role_param, request);
        let response = state.create.service().await.oneshot(request).await?;
        Ok(CreateStatus::CREATED(response))
    }

    #[apiserver_path(method = delete, path = DELETE_INTER_COLLECTION_PERMISSION, tag = INTER_COLLECTION_PERMISSIONS_TAG)]
    #[doc = "Delete an inter collection permission"]
    pub async fn delete(
        State(state): State<Arc<InterCollectionPermissionServices>>,
        Extension(context): Extension<RequestContext>,
        Path(param): Path<InterCollectionPermissionParam>,
    ) -> Result<DeleteStatus<NoContent>, ErrorStatus> {
        let request = context.delete(param);
        let response = state.delete.service().await.oneshot(request).await?;
        Ok(DeleteStatus::OK(response))
    }

    #[apiserver_path(method = get, path = LIST_INTER_COLLECTION_PERMISSIONS, tag = INTER_COLLECTION_PERMISSIONS_TAG)]
    #[doc = "List permissions"]
    pub async fn list(
        State(state): State<Arc<InterCollectionPermissionServices>>,
        Extension(context): Extension<RequestContext>,
        Query(query_params): Query<ListParams>,
        Path(path_params): Path<CollectionParam>,
    ) -> Result<ListStatus<InterCollectionPermission>, ErrorStatus> {
        let request = context.list(path_params, query_params);
        let response = state.list.service().await.oneshot(request).await?;
        Ok(ListStatus::OK(response))
    }
}
