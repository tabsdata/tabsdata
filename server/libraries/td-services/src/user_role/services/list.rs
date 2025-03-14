//
// Copyright 2025 Tabs Data Inc.
//

use std::sync::Arc;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{ListRequest, ListResponse};
use td_objects::rest_urls::RoleParam;
use td_objects::sql::roles::RoleQueries;
use td_objects::tower_service::extractor::extract_req_name;
use td_objects::tower_service::from::{ExtractService, TryMapListService, With};
use td_objects::tower_service::sql::{By, SqlListService, SqlSelectIdOrNameService};
use td_objects::types::basic::{RoleId, RoleIdName};
use td_objects::types::role::RoleDB;
use td_objects::types::role::{UserRole, UserRoleBuilder, UserRoleDBWithNames};
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{ConnectionProvider, SrvCtxProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct ListUserRoleService {
    provider: ServiceProvider<ListRequest<RoleParam>, ListResponse<UserRole>, TdError>,
}

impl ListUserRoleService {
    pub fn new(db: DbPool) -> Self {
        let queries = Arc::new(RoleQueries::new());
        Self {
            provider: Self::provider(db, queries),
        }
    }

    p! {
        provider(db: DbPool, queries: Arc<RoleQueries>) -> TdError {
            service_provider!(layers!(
                from_fn(extract_req_name::<ListRequest<RoleParam>, _>),

                SrvCtxProvider::new(queries),

                ConnectionProvider::new(db),
                from_fn(With::<RoleParam>::extract::<RoleIdName>),
                from_fn(By::<RoleIdName>::select::<RoleQueries, RoleDB>),
                from_fn(With::<RoleDB>::extract::<RoleId>),
                from_fn(By::<RoleId>::list::<RoleParam, RoleQueries, UserRoleDBWithNames>),

                from_fn(With::<UserRoleDBWithNames>::try_map_list::<RoleParam, UserRoleBuilder, UserRole, _>),
            ))
        }
    }

    pub async fn service(
        &self,
    ) -> TdBoxService<ListRequest<RoleParam>, ListResponse<UserRole>, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_objects::crudl::{ListParams, RequestContext};
    use td_objects::test_utils::seed_role::seed_role;
    use td_objects::test_utils::seed_user::{admin_user, seed_user};
    use td_objects::test_utils::seed_user_role::{get_user_role, seed_user_role};
    use td_objects::types::basic::{Description, RoleName, UserId};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_list_user_role() {
        use td_tower::metadata::{type_of_val, Metadata};

        let db = td_database::test_utils::db().await.unwrap();
        let queries = Arc::new(RoleQueries::new());
        let provider = ListUserRoleService::provider(db, queries);
        let service = provider.make().await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();

        metadata.assert_service::<ListRequest<RoleParam>, ListResponse<UserRole>>(&[
            type_of_val(&extract_req_name::<ListRequest<RoleParam>, _>),
            type_of_val(&With::<RoleParam>::extract::<RoleIdName>),
            type_of_val(&By::<RoleIdName>::select::<RoleQueries, RoleDB>),
            type_of_val(&With::<RoleDB>::extract::<RoleId>),
            type_of_val(&By::<RoleId>::list::<RoleParam, RoleQueries, UserRoleDBWithNames>),
            type_of_val(&With::<UserRoleDBWithNames>::try_map_list::<RoleParam, UserRoleBuilder, UserRole, _>),
        ]);
    }

    #[tokio::test]
    async fn test_list_user_role() -> Result<(), TdError> {
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

        let request = RequestContext::with(&admin_id, "r", true).await.list(
            RoleParam::builder()
                .role(RoleIdName::try_from("king")?)
                .build()?,
            ListParams::default(),
        );

        let service = ListUserRoleService::new(db.clone()).service().await;
        let response = service.raw_oneshot(request).await;
        let response = response?;

        assert_eq!(*response.len(), 1);
        let found = get_user_role(&db, user_role.id()).await?;
        let response = response.data().first().unwrap();
        assert_eq!(response.id(), found.id());
        assert_eq!(response.user_id(), found.user_id());
        assert_eq!(response.role_id(), found.role_id());
        assert_eq!(response.added_on(), found.added_on());
        assert_eq!(response.added_by_id(), found.added_by_id());
        assert_eq!(response.fixed(), found.fixed());
        Ok(())
    }
}
