//
// Copyright 2025 Tabs Data Inc.
//

use std::sync::Arc;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::DeleteRequest;
use td_objects::rest_urls::RolePermissionParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::extractor::extract_req_name;
use td_objects::tower_service::from::{ExtractService, With};
use td_objects::tower_service::sql::{By, SqlDeleteService, SqlSelectIdOrNameService};
use td_objects::types::basic::{PermissionId, PermissionIdName};
use td_objects::types::permission::PermissionDB;
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{SrvCtxProvider, TransactionProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct DeletePermissionService {
    provider: ServiceProvider<DeleteRequest<RolePermissionParam>, (), TdError>,
}

impl DeletePermissionService {
    pub fn new(db: DbPool) -> Self {
        let queries = Arc::new(DaoQueries::default());
        Self {
            provider: Self::provider(db, queries),
        }
    }

    p! {
        provider(db: DbPool, queries: Arc<DaoQueries>) -> TdError {
            service_provider!(layers!(
                SrvCtxProvider::new(queries),
                from_fn(extract_req_name::<DeleteRequest<RolePermissionParam>, _>),

                // TODO check RoleParam exists
                from_fn(With::<RolePermissionParam>::extract::<PermissionIdName>),

                TransactionProvider::new(db),
                from_fn(By::<PermissionIdName>::select::<DaoQueries, PermissionDB>),
                from_fn(With::<PermissionDB>::extract::<PermissionId>),
                from_fn(By::<PermissionId>::delete::<DaoQueries, PermissionDB>),
            ))
        }
    }

    pub async fn service(&self) -> TdBoxService<DeleteRequest<RolePermissionParam>, (), TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_objects::crudl::RequestContext;
    use td_objects::test_utils::seed_permission::{get_permission, seed_permission};
    use td_objects::test_utils::seed_role::seed_role;
    use td_objects::types::basic::{
        AccessTokenId, Description, PermissionType, RoleId, RoleIdName, RoleName, UserId,
    };
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_delete_permission() {
        use td_tower::metadata::{type_of_val, Metadata};

        let db = td_database::test_utils::db().await.unwrap();
        let queries = Arc::new(DaoQueries::default());
        let provider = DeletePermissionService::provider(db, queries);
        let service = provider.make().await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();

        metadata.assert_service::<DeleteRequest<RolePermissionParam>, ()>(&[
            type_of_val(&extract_req_name::<DeleteRequest<RolePermissionParam>, _>),
            type_of_val(&With::<RolePermissionParam>::extract::<PermissionIdName>),
            type_of_val(&By::<PermissionIdName>::select::<DaoQueries, PermissionDB>),
            type_of_val(&With::<PermissionDB>::extract::<PermissionId>),
            type_of_val(&By::<PermissionId>::delete::<DaoQueries, PermissionDB>),
        ]);
    }

    #[tokio::test]
    async fn test_delete_permission() -> Result<(), TdError> {
        let db = td_database::test_utils::db().await?;

        let role = seed_role(
            &db,
            RoleName::try_from("king")?,
            Description::try_from("super user")?,
        )
        .await;
        let seeded = seed_permission(&db, PermissionType::try_from("sa")?, None, None, &role).await;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sec_admin(),
            true,
        )
        .delete(
            RolePermissionParam::builder()
                .role(RoleIdName::try_from("king")?)
                .permission(PermissionIdName::try_from(seeded.id().to_string())?)
                .build()?,
        );

        let service = DeletePermissionService::new(db.clone()).service().await;
        service.raw_oneshot(request).await?;

        let not_found = get_permission(&db, seeded.id()).await;
        assert!(not_found.is_err());
        Ok(())
    }
}
