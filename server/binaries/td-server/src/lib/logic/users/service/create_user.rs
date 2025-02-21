//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::users::layers::{
    create_user_authorize, create_user_build_dao, create_user_sql_insert, user_extract_password,
    user_validate_password,
};
use std::sync::Arc;
use td_common::error::TdError;
use td_database::sql::DbPool;
use td_objects::crudl::CreateRequest;
use td_objects::dlo::UserId;
use td_objects::tower_service::creator::new_id;
use td_objects::tower_service::extractor::{
    extract_req_dto, extract_req_is_admin, extract_req_time, extract_req_user_id,
};
use td_objects::tower_service::finder::find_by_id;
use td_objects::tower_service::mapper::map;
use td_objects::users::dao::UserWithNames;
use td_objects::users::dto::{UserCreate, UserRead};
use td_security::config::PasswordHashingConfig;
use td_tower::default_services::{
    ServiceEntry, ServiceReturn, Share, SrvCtxProvider, TransactionProvider,
};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::TdBoxService;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider};
use tower::ServiceBuilder;

pub struct CreateUserService {
    provider: ServiceProvider<CreateRequest<(), UserCreate>, UserRead, TdError>,
}

impl CreateUserService {
    pub fn new(db: DbPool, password_hashing_config: Arc<PasswordHashingConfig>) -> Self {
        CreateUserService {
            provider: Self::provider(db, password_hashing_config.clone()),
        }
    }

    fn provider<Req: Share, Res: Share>(
        db: DbPool,
        password_hashing_config: Arc<PasswordHashingConfig>,
    ) -> ServiceProvider<Req, Res, TdError> {
        ServiceBuilder::new()
            .layer(ServiceEntry::default())
            .layer(TransactionProvider::new(db))
            .layer(SrvCtxProvider::new(password_hashing_config))
            .layer(from_fn(
                extract_req_is_admin::<CreateRequest<(), UserCreate>>,
            ))
            .layer(from_fn(create_user_authorize))
            .layer(from_fn(extract_req_time::<CreateRequest<(), UserCreate>>))
            .layer(from_fn(
                extract_req_user_id::<CreateRequest<(), UserCreate>>,
            ))
            .layer(from_fn(
                extract_req_dto::<CreateRequest<(), UserCreate>, (), UserCreate>,
            ))
            .layer(from_fn(user_extract_password::<UserCreate>))
            .layer(from_fn(user_validate_password))
            .layer(from_fn(new_id::<UserId>))
            .layer(from_fn(create_user_build_dao))
            .layer(from_fn(create_user_sql_insert))
            .layer(from_fn(find_by_id::<UserId, UserWithNames>))
            .layer(from_fn(map::<UserWithNames, UserRead>))
            .service(ServiceReturn)
            .into_service_provider()
    }

    pub async fn service(&self) -> TdBoxService<CreateRequest<(), UserCreate>, UserRead, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
pub mod tests {
    use crate::logic::apisrv::jwt::jwt_logic::JwtLogic;
    use crate::logic::users::service::create_user::CreateUserService;
    use crate::logic::users::service::UserServices;
    use chrono::Duration;
    use std::sync::Arc;
    use td_common::id::Id;
    use td_common::time::UniqueUtc;
    use td_database::sql::DbPool;
    use td_objects::crudl::RequestContext;
    use td_objects::test_utils::seed_user::admin_user;
    use td_objects::users::dto::{UserCreate, UserRead};
    use td_security::config::PasswordHashingConfig;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_create_provider() {
        use crate::logic::users::layers::{
            create_user_authorize, create_user_build_dao, create_user_sql_insert,
            user_extract_password, user_validate_password,
        };
        use crate::logic::users::service::create_user::CreateUserService;
        use td_objects::crudl::CreateRequest;
        use td_objects::dlo::UserId;
        use td_objects::tower_service::creator::new_id;
        use td_objects::tower_service::extractor::extract_req_dto;
        use td_objects::tower_service::extractor::extract_req_is_admin;
        use td_objects::tower_service::extractor::{extract_req_time, extract_req_user_id};
        use td_objects::tower_service::finder::find_by_id;
        use td_objects::tower_service::mapper::map;
        use td_objects::users::dao::UserWithNames;
        use td_tower::metadata::{type_of_val, Metadata};

        let password_config = Arc::new(PasswordHashingConfig::default());
        let db = td_database::test_utils::db().await.unwrap();
        let provider = CreateUserService::provider(db, password_config);
        let service = provider.make().await;
        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();
        metadata.assert_service::<CreateRequest<(), UserCreate>, UserRead>(&[
            type_of_val(&extract_req_is_admin::<CreateRequest<(), UserCreate>>),
            type_of_val(&create_user_authorize),
            type_of_val(&extract_req_time::<CreateRequest<(), UserCreate>>),
            type_of_val(&extract_req_user_id::<CreateRequest<(), UserCreate>>),
            type_of_val(&extract_req_dto::<CreateRequest<(), UserCreate>, (), UserCreate>),
            type_of_val(&user_extract_password::<UserCreate>),
            type_of_val(&user_validate_password), //*
            type_of_val(&new_id::<UserId>),
            type_of_val(&create_user_build_dao),  //*
            type_of_val(&create_user_sql_insert), //*
            type_of_val(&find_by_id::<UserId, UserWithNames>),
            type_of_val(&map::<UserWithNames, UserRead>),
        ]);
    }

    async fn test_create_user(enabled: Option<bool>, expected_enabled: bool, with_email: bool) {
        let db = td_database::test_utils::db().await.unwrap();
        let password_hashing_config = Arc::new(PasswordHashingConfig::default());
        let admin_id = admin_user(&db).await;

        let service = CreateUserService::new(db.clone(), password_hashing_config)
            .service()
            .await;

        let create = UserCreate {
            name: "u1".to_string(),
            full_name: "U1".to_string(),
            email: if with_email {
                Some("u1@email".to_string())
            } else {
                None
            },
            password: "password".to_string(),
            enabled,
        };

        let before = UniqueUtc::now_millis()
            .await
            .naive_utc()
            .and_utc()
            .timestamp_millis();
        let request = RequestContext::with(&admin_id, "r", true)
            .await
            .create((), create);
        let response = service.raw_oneshot(request).await;
        assert!(response.is_ok());
        let created = response.unwrap();

        assert!(Id::try_from(created.id()).is_ok());
        assert_eq!(created.name(), "u1");
        assert_eq!(created.full_name(), "U1");
        if with_email {
            assert_eq!(created.email().as_ref().unwrap(), "u1@email");
        } else {
            assert!(created.email().is_none());
        }
        assert!(*created.created_on() >= before);
        assert_eq!(created.created_by_id(), &admin_id);
        assert_eq!(created.created_by(), "admin");
        assert_eq!(created.modified_on(), created.created_on());
        assert_eq!(created.modified_by_id(), &admin_id);
        assert_eq!(created.modified_by(), "admin");
        assert_eq!(*created.enabled(), expected_enabled);
    }

    #[tokio::test]
    async fn test_create_user_enabled_true_with_email() {
        test_create_user(Some(true), true, true).await;
    }

    #[tokio::test]
    async fn test_create_user_enabled_false_without_email() {
        test_create_user(Some(false), false, false).await;
    }

    #[tokio::test]
    async fn test_create_user_enabled_default_without_email() {
        test_create_user(None, true, false).await;
    }

    /// Creates users for tests.
    ///
    /// The users are created with the name prefix and a number appended to it,
    /// full name is the name in uppercase, email is the name with `@test.com`,
    /// password is the name with `-password`.
    ///
    /// If creator_id is [`None`] the admin user is used as the creator.
    pub async fn create_test_users(
        db: &DbPool,
        creator_id: Option<String>,
        name_prefix: &str,
        count: usize,
        enabled: bool,
    ) -> Vec<UserRead> {
        let (mut admin_user, sysadmin_role) =
            td_database::test_utils::user_role_ids(db, td_security::ADMIN_USER).await;
        if let Some(creator_id) = creator_id {
            admin_user = creator_id;
        }
        let jwt_logic = Arc::new(JwtLogic::new(
            "SECRET",
            Duration::seconds(3600),
            Duration::seconds(7200),
        ));
        let logic = UserServices::new(
            db.clone(),
            Arc::new(PasswordHashingConfig::default()),
            jwt_logic,
        );
        let mut users = Vec::new();
        for i in 0..count {
            let name = format!("{}{}", name_prefix, i);
            let user = UserCreate::builder()
                .name(&name)
                .full_name(name.to_uppercase())
                .email(format!("{}@test.com", name))
                .password(format!("{}-password", name))
                .enabled(enabled)
                .build()
                .unwrap();
            let request = RequestContext::with(&admin_user, &sysadmin_role, true)
                .await
                .create((), user);
            let service = logic.create_user().await;
            let user = service.raw_oneshot(request).await.unwrap();
            users.push(user);
        }
        users
    }
}
