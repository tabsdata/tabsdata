//
// Copyright 2025 Tabs Data Inc.
//

use std::sync::Arc;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::DeleteRequest;
use td_objects::rest_urls::RoleParam;
use td_objects::sql::roles::RoleQueries;
use td_objects::tower_service::extractor::extract_req_name;
use td_objects::tower_service::from::{ExtractService, With};
use td_objects::tower_service::sql::{By, SqlDeleteService, SqlSelectIdOrNameService};
use td_objects::types::basic::{RoleId, RoleIdName};
use td_objects::types::role::RoleDB;
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{SrvCtxProvider, TransactionProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct DeleteRoleService {
    provider: ServiceProvider<DeleteRequest<RoleParam>, (), TdError>,
}

impl DeleteRoleService {
    pub fn new(db: DbPool) -> Self {
        let queries = Arc::new(RoleQueries::new());
        Self {
            provider: Self::provider(db, queries),
        }
    }

    p! {
        provider(db: DbPool, queries: Arc<RoleQueries>) -> TdError {
            service_provider!(layers!(
                SrvCtxProvider::new(queries),
                from_fn(extract_req_name::<DeleteRequest<RoleParam>, _>),

                from_fn(With::<RoleParam>::extract::<RoleIdName>),

                TransactionProvider::new(db),
                from_fn(By::<RoleIdName>::select::<RoleQueries, RoleDB>),
                from_fn(With::<RoleDB>::extract::<RoleId>),
                from_fn(By::<RoleId>::delete::<RoleQueries, RoleDB>),
            ))
        }
    }

    pub async fn service(&self) -> TdBoxService<DeleteRequest<RoleParam>, (), TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_objects::crudl::RequestContext;
    use td_objects::test_utils::seed_role::{get_role, seed_role};
    use td_objects::test_utils::seed_user::admin_user;
    use td_objects::types::basic::{Description, RoleName};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_delete_role() {
        use td_tower::metadata::{type_of_val, Metadata};

        let db = td_database::test_utils::db().await.unwrap();
        let queries = Arc::new(RoleQueries::new());
        let provider = DeleteRoleService::provider(db, queries);
        let service = provider.make().await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();

        metadata.assert_service::<DeleteRequest<RoleParam>, ()>(&[
            type_of_val(&extract_req_name::<DeleteRequest<RoleParam>, _>),
            type_of_val(&With::<RoleParam>::extract::<RoleIdName>),
            type_of_val(&By::<RoleIdName>::select::<RoleQueries, RoleDB>),
            type_of_val(&With::<RoleDB>::extract::<RoleId>),
            type_of_val(&By::<RoleId>::delete::<RoleQueries, RoleDB>),
        ]);
    }

    #[tokio::test]
    async fn test_delete_role_by_id() -> Result<(), TdError> {
        let db = td_database::test_utils::db().await?;
        let admin_id = admin_user(&db).await;

        let role = seed_role(
            &db,
            RoleName::try_from("joaquin")?,
            Description::try_from("super user")?,
        )
        .await;

        // By id
        let request = RequestContext::with(&admin_id, "r", true).await.delete(
            RoleParam::builder()
                .role(RoleIdName::try_from(format!("~{}", role.id()))?)
                .build()?,
        );

        let service = DeleteRoleService::new(db.clone()).service().await;
        service.raw_oneshot(request).await?;

        let found = get_role(&db, role.id()).await;
        // It should not be found
        assert!(found.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_delete_role_by_name() -> Result<(), TdError> {
        let db = td_database::test_utils::db().await?;
        let admin_id = admin_user(&db).await;

        let _role = seed_role(
            &db,
            RoleName::try_from("joaquin")?,
            Description::try_from("super user")?,
        )
        .await;

        let request = RequestContext::with(&admin_id, "r", true).await.delete(
            RoleParam::builder()
                .role(RoleIdName::try_from("joaquin")?)
                .build()?,
        );

        let service = DeleteRoleService::new(db.clone()).service().await;
        service.raw_oneshot(request).await?;

        let found = get_role(&db, &RoleName::try_from("joaquin")?).await;
        // It should not be found
        assert!(found.is_err());
        Ok(())
    }
}
