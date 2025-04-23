//
// Copyright 2024 Tabs Data Inc.
//

use crate::users::layers::{
    update_user_build_dao, update_user_sql_update, update_user_validate,
    update_user_validate_enabled, update_user_validate_password_change,
    update_user_validate_password_force_change_as_admin,
    update_user_validate_password_force_change_as_non_admin, user_extract_password,
    user_validate_password,
};
use crate::users::service::read_user::user_id_to_user_id;
use std::sync::Arc;
use td_authz::{Authz, AuthzContext};
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::UpdateRequest;
use td_objects::dlo::{UserId, UserName};
use td_objects::tower_service::authz::{AuthzOn, Requester, SecAdmin, SystemOrUserId};
use td_objects::tower_service::condition::is_req_by_user;
use td_objects::tower_service::extractor::{
    extract_name, extract_req_context, extract_req_dto, extract_req_time, extract_req_user_id,
    extract_user_id,
};
use td_objects::tower_service::finder::{find_by_id, find_by_name};
use td_objects::tower_service::mapper::map;
use td_objects::users::dao::User;
use td_objects::users::dao::UserWithNames;
use td_objects::users::dto::{UserRead, UserUpdate};
use td_security::config::PasswordHashingConfig;
use td_tower::default_services::{
    conditional, Do, Else, If, ServiceEntry, ServiceReturn, Share, SrvCtxProvider,
    TransactionProvider,
};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::TdBoxService;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider};
use tower::ServiceBuilder;

pub struct UpdateUserService {
    provider: ServiceProvider<UpdateRequest<String, UserUpdate>, UserRead, TdError>,
}

impl UpdateUserService {
    pub fn new(
        db: DbPool,
        password_hashing_config: Arc<PasswordHashingConfig>,
        authz_context: Arc<AuthzContext>,
    ) -> Self {
        UpdateUserService {
            provider: Self::provider(db, password_hashing_config, authz_context.clone()),
        }
    }

    fn provider<Req: Share, Res: Share>(
        db: DbPool,
        password_hashing_config: Arc<PasswordHashingConfig>,
        authz_context: Arc<AuthzContext>,
    ) -> ServiceProvider<Req, Res, TdError> {
        ServiceBuilder::new()
            .layer(ServiceEntry::default())
            .layer(TransactionProvider::new(db))
            .layer(SrvCtxProvider::new(authz_context))
            .layer(SrvCtxProvider::new(password_hashing_config))
            .layer(from_fn(
                extract_req_context::<UpdateRequest<String, UserUpdate>>,
            ))
            .layer(from_fn(
                extract_req_time::<UpdateRequest<String, UserUpdate>>,
            ))
            .layer(from_fn(
                extract_req_user_id::<UpdateRequest<String, UserUpdate>>,
            ))
            .layer(from_fn(
                extract_name::<UpdateRequest<String, UserUpdate>, String, UserName>,
            ))
            .layer(from_fn(find_by_name::<UserName, User>))
            .layer(from_fn(extract_user_id::<User>))
            .layer(from_fn(user_id_to_user_id))
            .layer(from_fn(AuthzOn::<SystemOrUserId>::set))
            .layer(from_fn(Authz::<SecAdmin, Requester>::check))
            .layer(from_fn(
                extract_req_dto::<UpdateRequest<String, UserUpdate>, String, UserUpdate>,
            ))
            .layer(from_fn(update_user_validate))
            .layer(conditional(
                If(ServiceBuilder::new()
                    .layer(from_fn(is_req_by_user))
                    .service(ServiceReturn)),
                Do(ServiceBuilder::new()
                    .layer(from_fn(
                        update_user_validate_password_force_change_as_non_admin,
                    ))
                    .service(ServiceReturn)),
                Else(
                    ServiceBuilder::new()
                        .layer(from_fn(update_user_validate_password_force_change_as_admin))
                        .service(ServiceReturn),
                ),
            ))
            .layer(from_fn(update_user_validate_enabled))
            .layer(from_fn(update_user_validate_password_change))
            .layer(from_fn(user_extract_password::<UserUpdate>))
            .layer(from_fn(user_validate_password))
            .layer(from_fn(update_user_build_dao))
            .layer(from_fn(update_user_sql_update))
            .layer(from_fn(find_by_id::<UserId, UserWithNames>))
            .layer(from_fn(map::<UserWithNames, UserRead>))
            .service(ServiceReturn)
            .into_service_provider()
    }

    pub async fn service(
        &self,
    ) -> TdBoxService<UpdateRequest<String, UserUpdate>, UserRead, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
pub mod tests {
    use crate::users::service::update_user::UpdateUserService;
    use std::sync::Arc;
    use td_authz::AuthzContext;
    use td_common::id::Id;
    use td_common::time::UniqueUtc;
    use td_objects::crudl::RequestContext;
    use td_objects::test_utils::seed_user::seed_user;
    use td_objects::types::basic::{AccessTokenId, RoleId, UserId};
    use td_objects::users::dao::User;
    use td_objects::users::dto::{PasswordUpdate, UserUpdate};
    use td_security::config::PasswordHashingConfig;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_update_provider() {
        use crate::users::layers::{update_user_build_dao, update_user_sql_update};
        use crate::users::layers::{
            update_user_validate, update_user_validate_enabled,
            update_user_validate_password_change,
        };
        use crate::users::layers::{
            update_user_validate_password_force_change_as_admin,
            update_user_validate_password_force_change_as_non_admin,
        };
        use crate::users::service::read_user::user_id_to_user_id;
        use crate::users::service::update_user::UpdateUserService;
        use std::sync::Arc;
        use td_authz::{Authz, AuthzContext};
        use td_objects::crudl::UpdateRequest;
        use td_objects::dlo::UserId;
        use td_objects::dlo::UserName;
        use td_objects::tower_service::authz::{AuthzOn, Requester, SecAdmin, SystemOrUserId};
        use td_objects::tower_service::condition::is_req_by_user;
        use td_objects::tower_service::extractor::extract_req_context;
        use td_objects::tower_service::extractor::extract_user_id;
        use td_objects::tower_service::extractor::{
            extract_name, extract_req_dto, extract_req_time, extract_req_user_id,
        };
        use td_objects::tower_service::finder::find_by_id;
        use td_objects::tower_service::finder::find_by_name;
        use td_objects::tower_service::mapper::map;
        use td_objects::users::dao::User;
        use td_objects::users::dao::UserWithNames;
        use td_objects::users::dto::{UserRead, UserUpdate};
        use td_security::config::PasswordHashingConfig;
        use td_tower::metadata::*;

        let db = td_database::test_utils::db().await.unwrap();
        let password_config = Arc::new(PasswordHashingConfig::default());
        let provider =
            UpdateUserService::provider(db, password_config, Arc::new(AuthzContext::default()));
        let service = provider.make().await;
        use crate::users::layers::{user_extract_password, user_validate_password};
        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();
        metadata.assert_service::<UpdateRequest<String, UserUpdate>, UserRead>(&[
            type_of_val(&extract_req_context::<UpdateRequest<String, UserUpdate>>),
            type_of_val(&extract_req_time::<UpdateRequest<String, UserUpdate>>),
            type_of_val(&extract_req_user_id::<UpdateRequest<String, UserUpdate>>),
            type_of_val(&extract_name::<UpdateRequest<String, UserUpdate>, String, UserName>),
            type_of_val(&find_by_name::<UserName, User>), //*
            type_of_val(&extract_user_id::<User>),
            type_of_val(&user_id_to_user_id),
            type_of_val(&AuthzOn::<SystemOrUserId>::set),
            type_of_val(&Authz::<SecAdmin, Requester>::check),
            type_of_val(&extract_req_dto::<UpdateRequest<String, UserUpdate>, String, UserUpdate>),
            type_of_val(&update_user_validate), //*
            type_of_val(&is_req_by_user),
            type_of_val(&update_user_validate_password_force_change_as_non_admin), //*
            type_of_val(&update_user_validate_password_force_change_as_admin),     //*
            type_of_val(&update_user_validate_enabled),                            //*
            type_of_val(&update_user_validate_password_change),                    //*
            type_of_val(&user_extract_password::<UserUpdate>),
            type_of_val(&user_validate_password), //*
            type_of_val(&update_user_build_dao),  //*
            type_of_val(&update_user_sql_update), //*
            type_of_val(&find_by_id::<UserId, UserWithNames>),
            type_of_val(&map::<UserWithNames, UserRead>),
        ]);
    }

    #[tokio::test]
    async fn test_update_user_admin() {
        let db = td_database::test_utils::db().await.unwrap();
        let password_config = Arc::new(PasswordHashingConfig::default());

        let admin_id = td_database::test_utils::user_role_ids(&db, td_security::ADMIN_USER)
            .await
            .0;

        seed_user(&db, None, "u0", true).await;

        let service = UpdateUserService::new(
            db.clone(),
            password_config,
            Arc::new(AuthzContext::default()),
        )
        .service()
        .await;

        let user_update = UserUpdate {
            full_name: Some("U0 Update".to_string()),
            email: Some("u0update@foo.com".to_string()),
            password: Some(PasswordUpdate::ForceChange {
                temporary_password: None,
            }),
            enabled: Some(false),
        };

        let before = UniqueUtc::now_millis()
            .naive_utc()
            .and_utc()
            .timestamp_millis();
        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sec_admin(),
            true,
        )
        .update("u0", user_update);
        let request_time = request.context().time().clone();
        let response = service.raw_oneshot(request).await;
        assert!(response.is_ok());
        let updated = response.unwrap();

        assert!(Id::try_from(updated.id()).is_ok());
        assert_eq!(updated.name(), "u0");
        assert_eq!(updated.full_name(), "U0 Update");
        assert_eq!(updated.email().as_ref().unwrap(), "u0update@foo.com");
        assert!(*updated.created_on() < before);
        assert_eq!(updated.created_by_id(), &admin_id);
        assert_eq!(updated.created_by(), "admin");
        assert_eq!(*updated.modified_on(), request_time.timestamp_millis());
        assert_eq!(updated.modified_by_id(), &admin_id);
        assert_eq!(updated.modified_by(), "admin");
        assert!(!updated.enabled());

        const SELECT: &str = "SELECT * FROM users WHERE name = 'u0'";

        let user: User = sqlx::query_as(SELECT).fetch_one(&db).await.unwrap();
        assert!(user.password_must_change())
    }

    #[tokio::test]
    async fn test_update_user_self() {
        let db = td_database::test_utils::db().await.unwrap();
        let password_config = Arc::new(PasswordHashingConfig::default());

        let admin_id = td_database::test_utils::user_role_ids(&db, td_security::ADMIN_USER)
            .await
            .0;

        let user_id = seed_user(&db, None, "u0", true).await;

        let service = UpdateUserService::new(
            db.clone(),
            password_config,
            Arc::new(AuthzContext::default()),
        )
        .service()
        .await;

        let user_update = UserUpdate {
            full_name: Some("U0 Update".to_string()),
            email: Some("u0update@foo.com".to_string()),
            password: Some(PasswordUpdate::Change {
                old_password: "password".to_string(),
                new_password: "password_update".to_string(),
            }),
            enabled: None,
        };

        let before = UniqueUtc::now_millis()
            .naive_utc()
            .and_utc()
            .timestamp_millis();
        let request =
            RequestContext::with(AccessTokenId::default(), user_id, RoleId::user(), false)
                .update("u0", user_update);
        let request_time = request.context().time().clone();
        let response = service.raw_oneshot(request).await;
        assert!(response.is_ok());
        let updated = response.unwrap();

        assert!(Id::try_from(updated.id()).is_ok());
        assert_eq!(updated.name(), "u0");
        assert_eq!(updated.full_name(), "U0 Update");
        assert_eq!(updated.email().as_ref().unwrap(), "u0update@foo.com");
        assert!(*updated.created_on() < before);
        assert_eq!(updated.created_by_id(), &admin_id);
        assert_eq!(updated.created_by(), "admin");
        assert_eq!(*updated.modified_on(), request_time.timestamp_millis());
        assert_eq!(updated.modified_by_id(), &user_id.to_string());
        assert_eq!(updated.modified_by(), "u0");
        assert!(updated.enabled());
    }
}
