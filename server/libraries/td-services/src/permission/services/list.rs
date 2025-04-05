//
// Copyright 2025 Tabs Data Inc.
//

use std::sync::Arc;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{ListRequest, ListResponse};
use td_objects::rest_urls::RoleParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::extractor::extract_req_name;
use td_objects::tower_service::from::{ExtractService, TryMapListService, With};
use td_objects::tower_service::sql::{By, SqlListService, SqlSelectIdOrNameService};
use td_objects::types::basic::{RoleId, RoleIdName};
use td_objects::types::permission::{Permission, PermissionBuilder, PermissionDBWithNames};
use td_objects::types::role::RoleDB;
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{ConnectionProvider, SrvCtxProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct ListPermissionService {
    provider: ServiceProvider<ListRequest<RoleParam>, ListResponse<Permission>, TdError>,
}

impl ListPermissionService {
    pub fn new(db: DbPool) -> Self {
        let queries = Arc::new(DaoQueries::default());
        Self {
            provider: Self::provider(db, queries),
        }
    }

    p! {
        provider(db: DbPool, queries: Arc<DaoQueries>) -> TdError {
            service_provider!(layers!(
                from_fn(extract_req_name::<ListRequest<RoleParam>, _>),
                from_fn(With::<RoleParam>::extract::<RoleIdName>),

                SrvCtxProvider::new(queries),
                ConnectionProvider::new(db),
                from_fn(By::<RoleIdName>::select::<DaoQueries, RoleDB>),
                from_fn(With::<RoleDB>::extract::<RoleId>),
                from_fn(By::<RoleId>::list::<RoleParam, DaoQueries, PermissionDBWithNames>),

                from_fn(With::<PermissionDBWithNames>::try_map_list::<RoleParam, PermissionBuilder, Permission, _>),
            ))
        }
    }

    pub async fn service(
        &self,
    ) -> TdBoxService<ListRequest<RoleParam>, ListResponse<Permission>, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_objects::crudl::{ListParams, RequestContext};
    use td_objects::test_utils::seed_permission::seed_permission;
    use td_objects::test_utils::seed_role::seed_role;
    use td_objects::types::basic::{AccessTokenId, Description, PermissionType, RoleName, UserId};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_list_permission() {
        use td_tower::metadata::{type_of_val, Metadata};

        let db = td_database::test_utils::db().await.unwrap();
        let queries = Arc::new(DaoQueries::default());
        let provider = ListPermissionService::provider(db, queries);
        let service = provider.make().await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();

        metadata.assert_service::<ListRequest<RoleParam>, ListResponse<Permission>>(&[
            type_of_val(&extract_req_name::<ListRequest<RoleParam>, _>),
            type_of_val(&With::<RoleParam>::extract::<RoleIdName>),
            type_of_val(&By::<RoleIdName>::select::<DaoQueries, RoleDB>),
            type_of_val(&With::<RoleDB>::extract::<RoleId>),
            type_of_val(&By::<RoleId>::list::<RoleParam, DaoQueries, PermissionDBWithNames>),
            type_of_val(
                &With::<PermissionDBWithNames>::try_map_list::<
                    RoleParam,
                    PermissionBuilder,
                    Permission,
                    _,
                >,
            ),
        ]);
    }

    #[tokio::test]
    async fn test_list_permissions() -> Result<(), TdError> {
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
        .list(
            RoleParam::builder()
                .role(RoleIdName::try_from("king")?)
                .build()?,
            ListParams::default(),
        );

        let service = ListPermissionService::new(db.clone()).service().await;
        let response = service.raw_oneshot(request).await;
        let response = response?;

        assert_eq!(*response.len(), 1);
        let permission = response.data().first().unwrap();
        assert_eq!(permission.id(), seeded.id());
        assert_eq!(permission.role_id(), seeded.role_id());
        assert_eq!(permission.permission_type(), seeded.permission_type());
        assert_eq!(permission.entity_type(), seeded.entity_type());
        assert_eq!(permission.entity_id(), seeded.entity_id());
        assert_eq!(permission.granted_by_id(), seeded.granted_by_id());
        assert_eq!(permission.granted_on(), seeded.granted_on());
        assert_eq!(permission.fixed(), seeded.fixed());
        Ok(())
    }
}
