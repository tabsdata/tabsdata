//
// Copyright 2025 Tabs Data Inc.
//

use std::sync::Arc;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::ReadRequest;
use td_objects::rest_urls::UserRoleParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::extractor::extract_req_name;
use td_objects::tower_service::from::{
    combine, BuildService, ExtractService, TryIntoService, With,
};
use td_objects::tower_service::sql::{By, SqlSelectIdOrNameService, SqlSelectService};
use td_objects::types::basic::{RoleId, RoleIdName, UserId, UserIdName};
use td_objects::types::role::RoleDB;
use td_objects::types::role::{UserRole, UserRoleBuilder, UserRoleDBWithNames};
use td_objects::types::user::UserDB;
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{ConnectionProvider, SrvCtxProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct ReadUserRoleService {
    provider: ServiceProvider<ReadRequest<UserRoleParam>, UserRole, TdError>,
}

impl ReadUserRoleService {
    pub fn new(db: DbPool) -> Self {
        let queries = Arc::new(DaoQueries::default());
        Self {
            provider: Self::provider(db, queries),
        }
    }

    p! {
        provider(db: DbPool, queries: Arc<DaoQueries>) -> TdError {
            service_provider!(layers!(
                from_fn(extract_req_name::<ReadRequest<UserRoleParam>, _>),

                SrvCtxProvider::new(queries),

                ConnectionProvider::new(db),
                from_fn(With::<UserRoleParam>::extract::<RoleIdName>),
                from_fn(By::<RoleIdName>::select::<DaoQueries, RoleDB>),
                from_fn(With::<RoleDB>::extract::<RoleId>),

                from_fn(With::<UserRoleParam>::extract::<UserIdName>),
                from_fn(By::<UserIdName>::select::<DaoQueries, UserDB>),
                from_fn(With::<UserDB>::extract::<UserId>),

                from_fn(combine::<RoleId, UserId>),
                from_fn(By::<(RoleId, UserId)>::select::<DaoQueries, UserRoleDBWithNames>),
                from_fn(With::<UserRoleDBWithNames>::convert_to::<UserRoleBuilder, _>),
                from_fn(With::<UserRoleBuilder>::build::<UserRole, _>),
            ))
        }
    }

    pub async fn service(&self) -> TdBoxService<ReadRequest<UserRoleParam>, UserRole, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_objects::crudl::RequestContext;
    use td_objects::test_utils::seed_role::seed_role;
    use td_objects::test_utils::seed_user::seed_user;
    use td_objects::test_utils::seed_user_role::{get_user_role, seed_user_role};
    use td_objects::types::basic::{AccessTokenId, Description, RoleName};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_read_user_role() {
        use td_tower::metadata::{type_of_val, Metadata};

        let db = td_database::test_utils::db().await.unwrap();
        let queries = Arc::new(DaoQueries::default());
        let provider = ReadUserRoleService::provider(db, queries);
        let service = provider.make().await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();

        metadata.assert_service::<ReadRequest<UserRoleParam>, UserRole>(&[
            type_of_val(&extract_req_name::<ReadRequest<UserRoleParam>, _>),
            type_of_val(&With::<UserRoleParam>::extract::<RoleIdName>),
            type_of_val(&By::<RoleIdName>::select::<DaoQueries, RoleDB>),
            type_of_val(&With::<RoleDB>::extract::<RoleId>),
            type_of_val(&With::<UserRoleParam>::extract::<UserIdName>),
            type_of_val(&By::<UserIdName>::select::<DaoQueries, UserDB>),
            type_of_val(&With::<UserDB>::extract::<UserId>),
            type_of_val(&combine::<RoleId, UserId>),
            type_of_val(&By::<(RoleId, UserId)>::select::<DaoQueries, UserRoleDBWithNames>),
            type_of_val(&With::<UserRoleDBWithNames>::convert_to::<UserRoleBuilder, _>),
            type_of_val(&With::<UserRoleBuilder>::build::<UserRole, _>),
        ]);
    }

    #[tokio::test]
    async fn test_read_user_role() -> Result<(), TdError> {
        let db = td_database::test_utils::db().await?;

        let user_id = seed_user(&db, None, "joaquin", false).await;
        let role = seed_role(
            &db,
            RoleName::try_from("king")?,
            Description::try_from("super user")?,
        )
        .await;
        let user_role = seed_user_role(&db, &UserId::from(user_id), role.id()).await;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
            true,
        )
        .read(
            UserRoleParam::builder()
                .role(RoleIdName::try_from("king")?)
                .user(UserIdName::try_from("joaquin")?)
                .build()?,
        );

        let service = ReadUserRoleService::new(db.clone()).service().await;
        let response = service.raw_oneshot(request).await;
        let response = response?;

        let found = get_user_role(&db, user_role.id()).await?;
        assert_eq!(response.id(), found.id());
        assert_eq!(response.user_id(), found.user_id());
        assert_eq!(response.role_id(), found.role_id());
        assert_eq!(response.added_on(), found.added_on());
        assert_eq!(response.added_by_id(), found.added_by_id());
        assert_eq!(response.fixed(), found.fixed());
        Ok(())
    }
}
