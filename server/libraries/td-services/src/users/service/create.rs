//
// Copyright 2024 Tabs Data Inc.
//

use crate::users::layers::create::UpdateCreateUserDBBuilder;
use std::sync::Arc;
use td_authz::{Authz, AuthzContext};
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{CreateRequest, RequestContext};
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{AuthzOn, SecAdmin, System};
use td_objects::tower_service::from::{
    builder, BuildService, ExtractDataService, ExtractService, SetService, TryIntoService,
    UpdateService, With,
};
use td_objects::tower_service::sql::{insert, By, SqlSelectService};
use td_objects::types::basic::{AtTime, UserId};
use td_objects::types::role::{FixedUserRole, FixedUserRoleBuilder, UserRoleDB, UserRoleDBBuilder};
use td_objects::types::user::{
    UserCreate, UserDB, UserDBBuilder, UserDBWithNames, UserRead, UserReadBuilder,
};
use td_security::config::PasswordHashingConfig;
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{SrvCtxProvider, TransactionProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::TdBoxService;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider};
use td_tower::{layers, p, service_provider};

pub struct CreateUserService {
    provider: ServiceProvider<CreateRequest<(), UserCreate>, UserRead, TdError>,
}

impl CreateUserService {
    pub fn new(
        db: DbPool,
        password_hashing_config: Arc<PasswordHashingConfig>,
        authz_context: Arc<AuthzContext>,
    ) -> Self {
        let queries = Arc::new(DaoQueries::default());
        CreateUserService {
            provider: Self::provider(db, queries, password_hashing_config, authz_context),
        }
    }

    p! {
        provider(
            db: DbPool,
            queries: Arc<DaoQueries>,
            password_hashing_config: Arc<PasswordHashingConfig>,
            authz_context: Arc<AuthzContext>
        ) {
            service_provider!(layers!(
                TransactionProvider::new(db),
                SrvCtxProvider::new(queries),
                SrvCtxProvider::new(authz_context),
                SrvCtxProvider::new(password_hashing_config),

                from_fn(With::<CreateRequest<(), UserCreate>>::extract::<RequestContext>),

                from_fn(AuthzOn::<System>::set),
                from_fn(Authz::<SecAdmin>::check),

                from_fn(With::<RequestContext>::extract::<AtTime>),
                from_fn(With::<RequestContext>::extract::<UserId>),
                from_fn(With::<CreateRequest<(), UserCreate>>::extract_data::<UserCreate>),

                from_fn(builder::<UserDBBuilder>),
                from_fn(With::<RequestContext>::update::<UserDBBuilder, _>),
                from_fn(With::<UserCreate>::update_create_user_db_builder),
                from_fn(With::<UserDBBuilder>::build::<UserDB, _>),
                from_fn(insert::<DaoQueries, UserDB>),

                // Add user to fixed 'user' role
                from_fn(builder::<UserRoleDBBuilder>),
                from_fn(With::<UserDB>::extract::<UserId>),
                from_fn(With::<UserId>::set::<UserRoleDBBuilder>),
                from_fn(With::<RequestContext>::update::<UserRoleDBBuilder, _>),
                from_fn(builder::<FixedUserRoleBuilder>),
                from_fn(With::<FixedUserRoleBuilder>::build::<FixedUserRole, _>),
                from_fn(With::<FixedUserRole>::update::<UserRoleDBBuilder, _>),
                from_fn(With::<UserRoleDBBuilder>::build::<UserRoleDB, _>),
                from_fn(insert::<DaoQueries, UserRoleDB>),

                from_fn(With::<UserDB>::extract::<UserId>),
                from_fn(By::<UserId>::select::<DaoQueries, UserDBWithNames>),
                from_fn(With::<UserDBWithNames>::convert_to::<UserReadBuilder, _>),
                from_fn(With::<UserReadBuilder>::build::<UserRead, _>),
            ))
        }
    }

    pub async fn service(&self) -> TdBoxService<CreateRequest<(), UserCreate>, UserRead, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use td_database::sql::DbPool;
    use td_objects::sql::SelectBy;
    use td_objects::types::basic::{
        AccessTokenId, AtTime, Email, FullName, RoleId, UserEnabled, UserId, UserName,
    };
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_create_provider(db: DbPool) {
        use td_tower::metadata::{type_of_val, Metadata};

        let queries = Arc::new(DaoQueries::default());
        let password_hashing_config = Arc::new(PasswordHashingConfig::default());
        let provider = CreateUserService::provider(
            db,
            queries,
            password_hashing_config,
            Arc::new(AuthzContext::default()),
        );
        let service = provider.make().await;
        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();
        metadata.assert_service::<CreateRequest<(), UserCreate>, UserRead>(&[
            type_of_val(&With::<CreateRequest<(), UserCreate>>::extract::<RequestContext>),
            type_of_val(&AuthzOn::<System>::set),
            type_of_val(&Authz::<SecAdmin>::check),
            type_of_val(&With::<RequestContext>::extract::<AtTime>),
            type_of_val(&With::<RequestContext>::extract::<UserId>),
            type_of_val(&With::<CreateRequest<(), UserCreate>>::extract_data::<UserCreate>),
            type_of_val(&builder::<UserDBBuilder>),
            type_of_val(&With::<RequestContext>::update::<UserDBBuilder, _>),
            type_of_val(&With::<UserCreate>::update_create_user_db_builder),
            type_of_val(&With::<UserDBBuilder>::build::<UserDB, _>),
            type_of_val(&insert::<DaoQueries, UserDB>),
            type_of_val(&builder::<UserRoleDBBuilder>),
            type_of_val(&With::<UserDB>::extract::<UserId>),
            type_of_val(&With::<UserId>::set::<UserRoleDBBuilder>),
            type_of_val(&With::<RequestContext>::update::<UserRoleDBBuilder, _>),
            type_of_val(&builder::<FixedUserRoleBuilder>),
            type_of_val(&With::<FixedUserRoleBuilder>::build::<FixedUserRole, _>),
            type_of_val(&With::<FixedUserRole>::update::<UserRoleDBBuilder, _>),
            type_of_val(&With::<UserRoleDBBuilder>::build::<UserRoleDB, _>),
            type_of_val(&insert::<DaoQueries, UserRoleDB>),
            type_of_val(&With::<UserDB>::extract::<UserId>),
            type_of_val(&By::<UserId>::select::<DaoQueries, UserDBWithNames>),
            type_of_val(&With::<UserDBWithNames>::convert_to::<UserReadBuilder, _>),
            type_of_val(&With::<UserReadBuilder>::build::<UserRead, _>),
        ]);
    }

    async fn test_create_user(
        db: &DbPool,
        enabled: UserEnabled,
        expected_enabled: bool,
        with_email: bool,
    ) {
        let password_hashing_config = Arc::new(PasswordHashingConfig::default());

        let service = CreateUserService::new(
            db.clone(),
            password_hashing_config,
            Arc::new(AuthzContext::default()),
        )
        .service()
        .await;

        let create = UserCreate::builder()
            .try_name("u1".to_string())
            .unwrap()
            .try_password("password".to_string())
            .unwrap()
            .try_full_name("U1".to_string())
            .unwrap()
            .email(if with_email {
                Some(Email::try_from("u1@email.com").unwrap())
            } else {
                None
            })
            .enabled(enabled)
            .build()
            .unwrap();

        let before = AtTime::now().await;
        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sec_admin(),
            false,
        )
        .create((), create);
        let response = service.raw_oneshot(request).await;
        assert!(response.is_ok());
        let created = response.unwrap();

        assert_eq!(*created.name(), UserName::try_from("u1").unwrap());
        assert_eq!(*created.full_name(), FullName::try_from("U1").unwrap());
        if with_email {
            assert_eq!(
                *created.email().as_ref().unwrap(),
                Email::try_from("u1@email.com").unwrap()
            );
        } else {
            assert!(created.email().is_none());
        }
        assert!(*created.created_on() >= before);
        assert_eq!(*created.created_by_id(), UserId::admin());
        assert_eq!(*created.created_by(), UserName::admin());
        assert_eq!(created.modified_on(), created.created_on());
        assert_eq!(*created.modified_by_id(), UserId::admin());
        assert_eq!(*created.modified_by(), UserName::admin());
        assert_eq!(**created.enabled(), expected_enabled);

        let res: Option<UserRoleDB> = DaoQueries::default()
            .select_by::<UserRoleDB>(&created.id())
            .unwrap()
            .build_query_as()
            .fetch_optional(db)
            .await
            .unwrap();
        assert!(res.is_some());
        assert_eq!(&RoleId::user(), res.unwrap().role_id())
    }

    #[td_test::test(sqlx)]
    async fn test_create_user_enabled_true_with_email(db: DbPool) {
        test_create_user(&db, UserEnabled::from(true), true, true).await;
    }

    #[td_test::test(sqlx)]
    async fn test_create_user_enabled_false_without_email(db: DbPool) {
        test_create_user(&db, UserEnabled::from(false), false, false).await;
    }

    #[td_test::test(sqlx)]
    async fn test_create_user_enabled_default_without_email(db: DbPool) {
        test_create_user(&db, UserEnabled::default(), true, false).await;
    }
}