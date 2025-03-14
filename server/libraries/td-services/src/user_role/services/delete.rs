//
// Copyright 2025 Tabs Data Inc.
//

use std::sync::Arc;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::DeleteRequest;
use td_objects::rest_urls::{RoleParam, UserParam, UserRoleParam};
use td_objects::sql::roles::RoleQueries;
use td_objects::tower_service::extractor::extract_req_name;
use td_objects::tower_service::from::{combine, ExtractService, With};
use td_objects::tower_service::sql::{By, SqlDeleteService, SqlSelectIdOrNameService};
use td_objects::types::basic::{RoleId, UserId};
use td_objects::types::role::{RoleDB, UserRoleDB};
use td_objects::types::user::UserDB;
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{SrvCtxProvider, TransactionProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct DeleteUserRoleService {
    provider: ServiceProvider<DeleteRequest<UserRoleParam>, (), TdError>,
}

impl DeleteUserRoleService {
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
                from_fn(extract_req_name::<DeleteRequest<UserRoleParam>, _>),

                TransactionProvider::new(db),

                from_fn(With::<UserRoleParam>::extract::<RoleParam>),
                from_fn(By::<RoleParam>::select::<RoleQueries, RoleDB>),
                from_fn(With::<RoleDB>::extract::<RoleId>),

                from_fn(With::<UserRoleParam>::extract::<UserParam>),
                from_fn(By::<UserParam>::select::<RoleQueries, UserDB>),
                from_fn(With::<UserDB>::extract::<UserId>),

                from_fn(combine::<RoleId, UserId>),
                from_fn(By::<(RoleId, UserId)>::delete::<RoleQueries, UserRoleDB>),
            ))
        }
    }

    pub async fn service(&self) -> TdBoxService<DeleteRequest<UserRoleParam>, (), TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_objects::crudl::RequestContext;
    use td_objects::test_utils::seed_role::seed_role;
    use td_objects::test_utils::seed_user::{admin_user, seed_user};
    use td_objects::test_utils::seed_user_role::{get_user_role, seed_user_role};
    use td_objects::types::basic::{Description, RoleName};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_delete_user_role() {
        use td_tower::metadata::{type_of_val, Metadata};

        let db = td_database::test_utils::db().await.unwrap();
        let queries = Arc::new(RoleQueries::new());
        let provider = DeleteUserRoleService::provider(db, queries);
        let service = provider.make().await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();

        metadata.assert_service::<DeleteRequest<UserRoleParam>, ()>(&[
            type_of_val(&extract_req_name::<DeleteRequest<UserRoleParam>, _>),
            type_of_val(&With::<UserRoleParam>::extract::<RoleParam>),
            type_of_val(&By::<RoleParam>::select::<RoleQueries, RoleDB>),
            type_of_val(&With::<RoleDB>::extract::<RoleId>),
            type_of_val(&With::<UserRoleParam>::extract::<UserParam>),
            type_of_val(&By::<UserParam>::select::<RoleQueries, UserDB>),
            type_of_val(&With::<UserDB>::extract::<UserId>),
            type_of_val(&combine::<RoleId, UserId>),
            type_of_val(&By::<(RoleId, UserId)>::delete::<RoleQueries, UserRoleDB>),
        ]);
    }

    #[tokio::test]
    async fn test_delete_user_role() -> Result<(), TdError> {
        let db = td_database::test_utils::db().await?;
        let admin_id = admin_user(&db).await;

        let user_id = seed_user(&db, None, "joaquin", false).await;
        let role = seed_role(
            &db,
            RoleName::try_from("king")?,
            Description::try_from("super user")?,
        )
        .await;
        let user_role = seed_user_role(&db, &UserId::from(user_id), role.id()).await;

        let request = RequestContext::with(&admin_id, "r", true).await.delete(
            UserRoleParam::builder()
                .role(RoleParam::try_from("king")?)
                .user(UserParam::try_from("joaquin")?)
                .build()?,
        );

        let service = DeleteUserRoleService::new(db.clone()).service().await;
        service.raw_oneshot(request).await?;

        let not_found = get_user_role(&db, user_role.id()).await;
        assert!(not_found.is_err());
        Ok(())
    }
}
