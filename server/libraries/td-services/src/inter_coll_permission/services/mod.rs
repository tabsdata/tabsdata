//
// Copyright 2025. Tabs Data Inc.
//

use crate::inter_coll_permission::services::create::CreateInterCollectionPermissionService;
use crate::inter_coll_permission::services::delete::DeleteInterCollectionPermissionService;
use crate::inter_coll_permission::services::list::ListInterCollectionPermissionService;
use std::sync::Arc;
use td_authz::AuthzContext;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{CreateRequest, DeleteRequest, ListRequest, ListResponse};
use td_objects::rest_urls::{CollectionParam, InterCollectionPermissionParam};
use td_objects::sql::DaoQueries;
use td_objects::types::permission::{InterCollectionPermission, InterCollectionPermissionCreate};
use td_tower::service_provider::TdBoxService;

pub mod create;
pub mod delete;
pub mod list;

pub struct InterCollectionPermissionServices {
    create: CreateInterCollectionPermissionService,
    delete: DeleteInterCollectionPermissionService,
    list: ListInterCollectionPermissionService,
}

impl InterCollectionPermissionServices {
    pub fn new(db: DbPool, authz_context: Arc<AuthzContext>) -> Self {
        let queries = Arc::new(DaoQueries::default());
        Self {
            create: CreateInterCollectionPermissionService::new(
                db.clone(),
                queries.clone(),
                authz_context.clone(),
            ),
            delete: DeleteInterCollectionPermissionService::new(
                db.clone(),
                queries.clone(),
                authz_context.clone(),
            ),
            list: ListInterCollectionPermissionService::new(
                db.clone(),
                queries.clone(),
                authz_context.clone(),
            ),
        }
    }

    pub async fn create_permission(
        &self,
    ) -> TdBoxService<
        CreateRequest<CollectionParam, InterCollectionPermissionCreate>,
        InterCollectionPermission,
        TdError,
    > {
        self.create.service().await
    }

    pub async fn delete_permission(
        &self,
    ) -> TdBoxService<DeleteRequest<InterCollectionPermissionParam>, (), TdError> {
        self.delete.service().await
    }

    pub async fn list_permission(
        &self,
    ) -> TdBoxService<ListRequest<CollectionParam>, ListResponse<InterCollectionPermission>, TdError>
    {
        self.list.service().await
    }
}
