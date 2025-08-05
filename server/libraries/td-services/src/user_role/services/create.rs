//
// Copyright 2025 Tabs Data Inc.
//

use std::sync::Arc;
use td_authz::{Authz, AuthzContext};
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{CreateRequest, RequestContext};
use td_objects::rest_urls::RoleParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{AuthzOn, SecAdmin, System};
use td_objects::tower_service::from::{
    builder, BuildService, ExtractDataService, ExtractNameService, ExtractService, SetService,
    TryIntoService, UpdateService, With,
};
use td_objects::tower_service::sql::{insert, By, SqlSelectService};
use td_objects::types::basic::{RoleId, RoleIdName, UserId, UserName, UserRoleId};
use td_objects::types::role::{
    RoleDB, UserRole, UserRoleBuilder, UserRoleCreate, UserRoleDB, UserRoleDBBuilder,
    UserRoleDBWithNames,
};
use td_objects::types::user::UserDB;
use td_tower::default_services::{SrvCtxProvider, TransactionProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct CreateUserRoleService {
    provider: ServiceProvider<CreateRequest<RoleParam, UserRoleCreate>, UserRole, TdError>,
}

impl CreateUserRoleService {
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
                TransactionProvider::new(db),
                SrvCtxProvider::new(authz_context),

                from_fn(With::<CreateRequest<RoleParam, UserRoleCreate>>::extract::<RequestContext>),
                from_fn(AuthzOn::<System>::set),
                from_fn(Authz::<SecAdmin>::check),

                from_fn(With::<CreateRequest<RoleParam, UserRoleCreate>>::extract_name::<RoleParam>),
                from_fn(With::<CreateRequest<RoleParam, UserRoleCreate>>::extract_data::<UserRoleCreate>),

                from_fn(builder::<UserRoleDBBuilder>),

                from_fn(With::<RoleParam>::extract::<RoleIdName>),
                from_fn(By::<RoleIdName>::select::<DaoQueries, RoleDB>),
                from_fn(With::<RoleDB>::extract::<RoleId>),
                from_fn(With::<RoleId>::set::<UserRoleDBBuilder>),

                from_fn(With::<UserRoleCreate>::extract::<UserName>),
                from_fn(By::<UserName>::select::<DaoQueries, UserDB>),
                from_fn(With::<UserDB>::extract::<UserId>),
                from_fn(With::<UserId>::set::<UserRoleDBBuilder>),

                from_fn(With::<RequestContext>::update::<UserRoleDBBuilder, _>),
                from_fn(With::<UserRoleDBBuilder>::build::<UserRoleDB, _>),

                from_fn(insert::<DaoQueries, UserRoleDB>),

                from_fn(With::<UserRoleDB>::extract::<UserRoleId>),
                from_fn(By::<UserRoleId>::select::<DaoQueries, UserRoleDBWithNames>),
                from_fn(With::<UserRoleDBWithNames>::convert_to::<UserRoleBuilder, _>),
                from_fn(With::<UserRoleBuilder>::build::<UserRole, _>),
            ))
        }
    }

    pub async fn service(
        &self,
    ) -> TdBoxService<CreateRequest<RoleParam, UserRoleCreate>, UserRole, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_objects::test_utils::seed_role::seed_role;
    use td_objects::test_utils::seed_user::seed_user;
    use td_objects::test_utils::seed_user_role::get_user_role;
    use td_objects::types::basic::{AccessTokenId, Description, RoleName, UserEnabled};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_create_user_role(db: DbPool) {
        use td_objects::tower_service::authz::{AuthzOn, SecAdmin, System};
        use td_tower::metadata::{type_of_val, Metadata};

        let queries = Arc::new(DaoQueries::default());
        let provider =
            CreateUserRoleService::provider(db, queries, Arc::new(AuthzContext::default()));
        let service = provider.make().await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();

        metadata.assert_service::<CreateRequest<RoleParam, UserRoleCreate>, UserRole>(&[
            type_of_val(
                &With::<CreateRequest<RoleParam, UserRoleCreate>>::extract::<RequestContext>,
            ),
            type_of_val(&AuthzOn::<System>::set),
            type_of_val(&Authz::<SecAdmin>::check),
            type_of_val(
                &With::<CreateRequest<RoleParam, UserRoleCreate>>::extract_name::<RoleParam>,
            ),
            type_of_val(
                &With::<CreateRequest<RoleParam, UserRoleCreate>>::extract_data::<UserRoleCreate>,
            ),
            type_of_val(&builder::<UserRoleDBBuilder>),
            type_of_val(&With::<RoleParam>::extract::<RoleIdName>),
            type_of_val(&By::<RoleIdName>::select::<DaoQueries, RoleDB>),
            type_of_val(&With::<RoleDB>::extract::<RoleId>),
            type_of_val(&With::<RoleId>::set::<UserRoleDBBuilder>),
            type_of_val(&With::<UserRoleCreate>::extract::<UserName>),
            type_of_val(&By::<UserName>::select::<DaoQueries, UserDB>),
            type_of_val(&With::<UserDB>::extract::<UserId>),
            type_of_val(&With::<UserId>::set::<UserRoleDBBuilder>),
            type_of_val(&With::<RequestContext>::update::<UserRoleDBBuilder, _>),
            type_of_val(&With::<UserRoleDBBuilder>::build::<UserRoleDB, _>),
            type_of_val(&insert::<DaoQueries, UserRoleDB>),
            type_of_val(&With::<UserRoleDB>::extract::<UserRoleId>),
            type_of_val(&By::<UserRoleId>::select::<DaoQueries, UserRoleDBWithNames>),
            type_of_val(&With::<UserRoleDBWithNames>::convert_to::<UserRoleBuilder, _>),
            type_of_val(&With::<UserRoleBuilder>::build::<UserRole, _>),
        ]);
    }

    #[td_test::test(sqlx)]
    async fn test_create_user_role(db: DbPool) -> Result<(), TdError> {
        let _user = seed_user(
            &db,
            &UserName::try_from("joaquin")?,
            &UserEnabled::from(false),
        )
        .await;
        let _role = seed_role(
            &db,
            RoleName::try_from("king")?,
            Description::try_from("super user")?,
        )
        .await;

        let create = UserRoleCreate::builder()
            .user(UserName::try_from("joaquin")?)
            .build()?;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sec_admin(),
        )
        .create(
            RoleParam::builder()
                .role(RoleIdName::try_from("king")?)
                .build()?,
            create,
        );

        let service = CreateUserRoleService::new(db.clone(), Arc::new(AuthzContext::default()))
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        let response = response?;

        let found = get_user_role(&db, response.id()).await?;
        assert_eq!(response.id(), found.id());
        assert_eq!(response.user_id(), found.user_id());
        assert_eq!(response.role_id(), found.role_id());
        assert_eq!(response.added_on(), found.added_on());
        assert_eq!(response.added_by_id(), found.added_by_id());
        assert_eq!(response.fixed(), found.fixed());
        Ok(())
    }
}
