//
// Copyright 2024 Tabs Data Inc.
//

use crate::user::layers::update::{
    update_user_validate, update_user_validate_enabled, update_user_validate_password_change,
    UpdateUserDBBuilderUpdate,
};
use std::sync::Arc;
use td_authz::{Authz, AuthzContext};
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{RequestContext, UpdateRequest};
use td_objects::rest_urls::UserParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{AuthzOn, Requester, SecAdmin, SystemOrUserId};
use td_objects::tower_service::from::{
    BuildService, ExtractDataService, ExtractNameService, ExtractService, TryIntoService,
    UpdateService, With,
};
use td_objects::tower_service::sql::{By, SqlSelectService, SqlUpdateService};
use td_objects::types::basic::{AtTime, UserId, UserIdName};
use td_objects::types::user::{
    UserDB, UserDBWithNames, UserRead, UserReadBuilder, UserUpdate, UserUpdateDB,
    UserUpdateDBBuilder,
};
use td_security::config::PasswordHashingConfig;
use td_tower::default_services::{SrvCtxProvider, TransactionProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct UpdateUserService {
    provider: ServiceProvider<UpdateRequest<UserParam, UserUpdate>, UserRead, TdError>,
}

impl UpdateUserService {
    pub fn new(
        db: DbPool,
        password_hashing_config: Arc<PasswordHashingConfig>,
        authz_context: Arc<AuthzContext>,
    ) -> Self {
        let queries = Arc::new(DaoQueries::default());
        UpdateUserService {
            provider: Self::provider(db, queries, password_hashing_config, authz_context),
        }
    }

    p! {
        provider(db: DbPool, queries: Arc<DaoQueries>, password_hashing_config: Arc<PasswordHashingConfig>, authz_context: Arc<AuthzContext>) {
            service_provider!(layers!(
                TransactionProvider::new(db),
                SrvCtxProvider::new(queries),
                SrvCtxProvider::new(authz_context),
                SrvCtxProvider::new(password_hashing_config),
                from_fn(With::<UpdateRequest<UserParam, UserUpdate>>::extract::<RequestContext>),
                from_fn(With::<RequestContext>::extract::<AtTime>),
                from_fn(With::<RequestContext>::extract::<UserId>),
                from_fn(With::<UpdateRequest<UserParam, UserUpdate>>::extract_name::<UserParam>),
                from_fn(With::<UserParam>::extract::<UserIdName>),

                from_fn(By::<UserIdName>::select::<DaoQueries, UserDB>),
                from_fn(AuthzOn::<SystemOrUserId>::set),
                from_fn(Authz::<SecAdmin, Requester>::check),

                from_fn(With::<UpdateRequest<UserParam, UserUpdate>>::extract_data::<UserUpdate>),
                from_fn(update_user_validate),
                from_fn(update_user_validate_password_change),
                from_fn(update_user_validate_enabled),

                from_fn(With::<UserDB>::convert_to::<UserUpdateDBBuilder, _>),
                from_fn(With::<RequestContext>::update::<UserUpdateDBBuilder, _>),
                from_fn(With::<UserUpdate>::update_user_update_db_builder),
                from_fn(With::<UserUpdateDBBuilder>::build::<UserUpdateDB, _>),

                from_fn(With::<UserDB>::extract::<UserId>),
                from_fn(By::<UserId>::update::<DaoQueries, UserUpdateDB, UserDB>),

                from_fn(By::<UserId>::select::<DaoQueries, UserDBWithNames>),
                from_fn(With::<UserDBWithNames>::convert_to::<UserReadBuilder, _>),
                from_fn(With::<UserReadBuilder>::build::<UserRead, _>),
            ))
        }
    }

    pub async fn service(
        &self,
    ) -> TdBoxService<UpdateRequest<UserParam, UserUpdate>, UserRead, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use td_authz::AuthzContext;
    use td_database::sql::DbPool;
    use td_objects::crudl::RequestContext;
    use td_objects::rest_urls::UserParam;
    use td_objects::sql::{DaoQueries, SelectBy};
    use td_objects::test_utils::seed_user::seed_user;
    use td_objects::types::basic::{
        AccessTokenId, AtTime, Email, FullName, Password, RoleId, UserEnabled, UserId, UserName,
    };
    use td_objects::types::user::{UserDB, UserUpdate};
    use td_security::config::PasswordHashingConfig;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_update_provider(db: DbPool) {
        use td_tower::metadata::{type_of_val, Metadata};

        let queries = Arc::new(DaoQueries::default());
        let password_config = Arc::new(PasswordHashingConfig::default());
        let provider = UpdateUserService::provider(
            db,
            queries,
            password_config,
            Arc::new(AuthzContext::default()),
        );
        let service = provider.make().await;
        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();
        metadata.assert_service::<UpdateRequest<UserParam, UserUpdate>, UserRead>(&[
            type_of_val(&With::<UpdateRequest<UserParam, UserUpdate>>::extract::<RequestContext>),
            type_of_val(&With::<RequestContext>::extract::<AtTime>),
            type_of_val(&With::<RequestContext>::extract::<UserId>),
            type_of_val(&With::<UpdateRequest<UserParam, UserUpdate>>::extract_name::<UserParam>),
            type_of_val(&With::<UserParam>::extract::<UserIdName>),
            type_of_val(&By::<UserIdName>::select::<DaoQueries, UserDB>),
            type_of_val(&AuthzOn::<SystemOrUserId>::set),
            type_of_val(&Authz::<SecAdmin, Requester>::check),
            type_of_val(&With::<UpdateRequest<UserParam, UserUpdate>>::extract_data::<UserUpdate>),
            type_of_val(&update_user_validate),
            type_of_val(&update_user_validate_password_change),
            type_of_val(&update_user_validate_enabled),
            type_of_val(&With::<UserDB>::convert_to::<UserUpdateDBBuilder, _>),
            type_of_val(&With::<RequestContext>::update::<UserUpdateDBBuilder, _>),
            type_of_val(&With::<UserUpdate>::update_user_update_db_builder),
            type_of_val(&With::<UserUpdateDBBuilder>::build::<UserUpdateDB, _>),
            type_of_val(&With::<UserDB>::extract::<UserId>),
            type_of_val(&By::<UserId>::update::<DaoQueries, UserUpdateDB, UserDB>),
            type_of_val(&By::<UserId>::select::<DaoQueries, UserDBWithNames>),
            type_of_val(&With::<UserDBWithNames>::convert_to::<UserReadBuilder, _>),
            type_of_val(&With::<UserReadBuilder>::build::<UserRead, _>),
        ]);
    }

    #[td_test::test(sqlx)]
    async fn test_update_user_admin(db: DbPool) {
        let password_config = Arc::new(PasswordHashingConfig::default());
        let _ = seed_user(
            &db,
            &UserName::try_from("u0").unwrap(),
            &UserEnabled::from(true),
        )
        .await;

        let service = UpdateUserService::new(
            db.clone(),
            password_config,
            Arc::new(AuthzContext::default()),
        )
        .service()
        .await;

        let user_update = UserUpdate::builder()
            .full_name(Some(FullName::try_from("U0 Update").unwrap()))
            .email(Some(Email::try_from("u0update@foo.com").unwrap()))
            .password(Some(Password::try_from("new_password").unwrap()))
            .enabled(Some(UserEnabled::from(false)))
            .build()
            .unwrap();

        let before = AtTime::now().await;
        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sec_admin(),
            true,
        )
        .update(
            UserParam::builder()
                .try_user("u0")
                .unwrap()
                .build()
                .unwrap(),
            user_update,
        );
        let request_time = request.context().time().clone();
        let response = service.raw_oneshot(request).await;
        assert!(response.is_ok());
        let updated = response.unwrap();

        assert_eq!(*updated.name(), UserName::try_from("u0").unwrap());
        assert_eq!(
            *updated.full_name(),
            FullName::try_from("U0 Update").unwrap()
        );
        assert_eq!(
            *updated.email().as_ref().unwrap(),
            Email::try_from("u0update@foo.com").unwrap()
        );
        assert!(*updated.created_on() < before);
        assert_eq!(*updated.created_by_id(), UserId::admin());
        assert_eq!(*updated.created_by(), UserName::admin());
        assert_eq!(
            updated.modified_on().timestamp_millis(),
            request_time.timestamp_millis()
        );
        assert_eq!(*updated.modified_by_id(), UserId::admin());
        assert_eq!(updated.modified_by(), &UserName::admin());
        assert!(!(**updated.enabled()));
        assert!(**updated.password_must_change());

        let user: UserDB = DaoQueries::default()
            .select_by::<UserDB>(&())
            .unwrap()
            .build_query_as()
            .fetch_one(&db)
            .await
            .unwrap();
        assert!(**user.password_must_change())
    }

    #[td_test::test(sqlx)]
    async fn test_update_user_self(db: DbPool) {
        let password_config = Arc::new(PasswordHashingConfig::default());
        let user = seed_user(
            &db,
            &UserName::try_from("u0").unwrap(),
            &UserEnabled::from(true),
        )
        .await;

        let service = UpdateUserService::new(
            db.clone(),
            password_config,
            Arc::new(AuthzContext::default()),
        )
        .service()
        .await;

        let user_update = UserUpdate::builder()
            .full_name(Some(FullName::try_from("U0 Update").unwrap()))
            .email(Some(Email::try_from("u0update@foo.com").unwrap()))
            .password(None)
            .enabled(None)
            .build()
            .unwrap();

        let before = AtTime::now().await;
        let request =
            RequestContext::with(AccessTokenId::default(), user.id(), RoleId::user(), false)
                .update(
                    UserParam::builder()
                        .try_user("u0")
                        .unwrap()
                        .build()
                        .unwrap(),
                    user_update,
                );
        let request_time = request.context().time().clone();
        let response = service.raw_oneshot(request).await;
        assert!(response.is_ok());
        let updated = response.unwrap();

        assert_eq!(*updated.name(), UserName::try_from("u0").unwrap());
        assert_eq!(
            *updated.full_name(),
            FullName::try_from("U0 Update").unwrap()
        );
        assert_eq!(
            *updated.email().as_ref().unwrap(),
            Email::try_from("u0update@foo.com").unwrap()
        );
        assert!(*updated.created_on() < before);
        assert_eq!(*updated.created_by_id(), UserId::admin());
        assert_eq!(*updated.created_by(), UserName::admin());
        assert_eq!(
            updated.modified_on().timestamp_millis(),
            request_time.timestamp_millis()
        );
        assert_eq!(updated.modified_by_id(), user.id());
        assert_eq!(updated.modified_by(), user.name());
        assert!(**updated.enabled());
    }
}
