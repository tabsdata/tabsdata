//
// Copyright 2025 Tabs Data Inc.
//

use std::sync::Arc;
use td_authz::{Authz, AuthzContext};
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{ReadRequest, RequestContext};
use td_objects::rest_urls::UserRoleParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{AuthzOn, CollAdmin, SecAdmin, System};
use td_objects::tower_service::from::{
    combine, BuildService, ExtractNameService, ExtractService, TryIntoService, With,
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
    pub fn new(db: DbPool, authz_context: Arc<AuthzContext>) -> Self {
        let queries = Arc::new(DaoQueries::default());
        Self {
            provider: Self::provider(db, queries, authz_context),
        }
    }

    p! {
        provider(db: DbPool, queries: Arc<DaoQueries>, authz_context: Arc<AuthzContext>) {
            service_provider!(layers!(
                SrvCtxProvider::new(queries),
                ConnectionProvider::new(db),
                SrvCtxProvider::new(authz_context),

                from_fn(With::<ReadRequest<UserRoleParam>>::extract::<RequestContext>),
                from_fn(AuthzOn::<System>::set),
                from_fn(Authz::<SecAdmin, CollAdmin>::check),

                from_fn(With::<ReadRequest<UserRoleParam>>::extract_name::<UserRoleParam>),

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
    use td_authz::AuthzContext;
    use td_objects::crudl::RequestContext;
    use td_objects::test_utils::seed_role::seed_role;
    use td_objects::test_utils::seed_user::seed_user;
    use td_objects::test_utils::seed_user_role::{get_user_role, seed_user_role};
    use td_objects::types::basic::{AccessTokenId, Description, RoleName, UserEnabled, UserName};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_read_user_role(db: DbPool) {
        use td_tower::metadata::{type_of_val, Metadata};

        let queries = Arc::new(DaoQueries::default());
        let provider =
            ReadUserRoleService::provider(db, queries, Arc::new(AuthzContext::default()));
        let service = provider.make().await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();

        metadata.assert_service::<ReadRequest<UserRoleParam>, UserRole>(&[
            type_of_val(&With::<ReadRequest<UserRoleParam>>::extract::<RequestContext>),
            type_of_val(&AuthzOn::<System>::set),
            type_of_val(&Authz::<SecAdmin, CollAdmin>::check),
            type_of_val(&With::<ReadRequest<UserRoleParam>>::extract_name::<UserRoleParam>),
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

    #[td_test::test(sqlx)]
    async fn test_read_user_role(db: DbPool) -> Result<(), TdError> {
        let user = seed_user(
            &db,
            &UserName::try_from("joaquin").unwrap(),
            &UserEnabled::from(false),
        )
        .await;
        let role = seed_role(
            &db,
            RoleName::try_from("king")?,
            Description::try_from("super user")?,
        )
        .await;
        let user_role = seed_user_role(&db, user.id(), role.id()).await;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sec_admin(),
            false,
        )
        .read(
            UserRoleParam::builder()
                .role(RoleIdName::try_from("king")?)
                .user(UserIdName::try_from("joaquin")?)
                .build()?,
        );

        let service = ReadUserRoleService::new(db.clone(), Arc::new(AuthzContext::default()))
            .service()
            .await;
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
