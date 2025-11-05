//
// Copyright 2024 Tabs Data Inc.
//

use crate::user::layers::create::UpdateCreateUserDBBuilder;
use ta_services::factory::service_factory;
use td_authz::{Authz, AuthzContext};
use td_objects::dxo::crudl::{CreateRequest, RequestContext};
use td_objects::dxo::user::defs::{
    UserCreate, UserDB, UserDBBuilder, UserDBWithNames, UserRead, UserReadBuilder,
};
use td_objects::dxo::user_role::defs::{
    FixedUserRole, FixedUserRoleBuilder, UserRoleDB, UserRoleDBBuilder,
};
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{AuthzOn, SecAdmin, System};
use td_objects::tower_service::from::{
    BuildService, ExtractDataService, ExtractService, SetService, TryIntoService, UpdateService,
    With, builder,
};
use td_objects::tower_service::sql::{By, SqlSelectService, insert};
use td_objects::types::id::UserId;
use td_objects::types::timestamp::AtTime;
use td_security::config::PasswordHashingConfig;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = CreateUserService,
    request = CreateRequest<(), UserCreate>,
    response = UserRead,
    connection = TransactionProvider,
    context = DaoQueries,
    context = AuthzContext,
    context = PasswordHashingConfig,
)]
fn service() {
    layers!(
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
        from_fn(insert::<UserDB>),
        // Add user to fixed 'user' role
        from_fn(builder::<UserRoleDBBuilder>),
        from_fn(With::<UserDB>::extract::<UserId>),
        from_fn(With::<UserId>::set::<UserRoleDBBuilder>),
        from_fn(With::<RequestContext>::update::<UserRoleDBBuilder, _>),
        from_fn(builder::<FixedUserRoleBuilder>),
        from_fn(With::<FixedUserRoleBuilder>::build::<FixedUserRole, _>),
        from_fn(With::<FixedUserRole>::update::<UserRoleDBBuilder, _>),
        from_fn(With::<UserRoleDBBuilder>::build::<UserRoleDB, _>),
        from_fn(insert::<UserRoleDB>),
        from_fn(With::<UserDB>::extract::<UserId>),
        from_fn(By::<UserId>::select::<UserDBWithNames>),
        from_fn(With::<UserDBWithNames>::convert_to::<UserReadBuilder, _>),
        from_fn(With::<UserReadBuilder>::build::<UserRead, _>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use ta_services::service::TdService;
    use td_database::sql::DbPool;
    use td_objects::sql::SelectBy;
    use td_objects::types::bool::UserEnabled;
    use td_objects::types::id::{AccessTokenId, RoleId};
    use td_objects::types::string::{Email, FullName, UserName};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_create_provider(db: DbPool) {
        use td_tower::metadata::type_of_val;

        CreateUserService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<CreateRequest<(), UserCreate>, UserRead>(&[
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
                type_of_val(&insert::<UserDB>),
                type_of_val(&builder::<UserRoleDBBuilder>),
                type_of_val(&With::<UserDB>::extract::<UserId>),
                type_of_val(&With::<UserId>::set::<UserRoleDBBuilder>),
                type_of_val(&With::<RequestContext>::update::<UserRoleDBBuilder, _>),
                type_of_val(&builder::<FixedUserRoleBuilder>),
                type_of_val(&With::<FixedUserRoleBuilder>::build::<FixedUserRole, _>),
                type_of_val(&With::<FixedUserRole>::update::<UserRoleDBBuilder, _>),
                type_of_val(&With::<UserRoleDBBuilder>::build::<UserRoleDB, _>),
                type_of_val(&insert::<UserRoleDB>),
                type_of_val(&With::<UserDB>::extract::<UserId>),
                type_of_val(&By::<UserId>::select::<UserDBWithNames>),
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
        let service = CreateUserService::with_defaults(db.clone()).service().await;

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

        let before = AtTime::now();
        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sec_admin(),
        )
        .create((), create);
        let response = service.raw_oneshot(request).await;
        let created = response.unwrap();

        assert_eq!(created.name, UserName::try_from("u1").unwrap());
        assert_eq!(created.full_name, FullName::try_from("U1").unwrap());
        if with_email {
            assert_eq!(
                created.email.unwrap(),
                Email::try_from("u1@email.com").unwrap()
            );
        } else {
            assert!(created.email.is_none());
        }
        assert!(created.created_on >= before);
        assert_eq!(created.created_by_id, UserId::admin());
        assert_eq!(created.created_by, UserName::admin());
        assert_eq!(created.modified_on, created.created_on);
        assert_eq!(created.modified_by_id, UserId::admin());
        assert_eq!(created.modified_by, UserName::admin());
        assert_eq!(*created.enabled, expected_enabled);

        let res: Option<UserRoleDB> = DaoQueries::default()
            .select_by::<UserRoleDB>(&created.id)
            .unwrap()
            .build_query_as()
            .fetch_optional(db)
            .await
            .unwrap();
        assert!(res.is_some());
        assert_eq!(RoleId::user(), res.unwrap().role_id)
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_create_user_enabled_true_with_email(db: DbPool) {
        test_create_user(&db, UserEnabled::from(true), true, true).await;
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_create_user_enabled_false_without_email(db: DbPool) {
        test_create_user(&db, UserEnabled::from(false), false, false).await;
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_create_user_enabled_default_without_email(db: DbPool) {
        test_create_user(&db, UserEnabled::default(), true, false).await;
    }
}
