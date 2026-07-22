//
// Copyright 2024 Tabsdata Inc.
//

use crate::user::layers::assert_user_is_not_system;
use crate::user::layers::update::{
    UpdateUserDBBuilderUpdate, update_user_validate, update_user_validate_enabled,
    update_user_validate_password_change,
};
use ta_services::factory::service_factory;
use td_authz::AuthzContext;
use td_authz::rbac::{ManageAccess, Rbac, Scope, SelfUser, User};
use td_objects::dxo::crudl::{RequestContext, UpdateRequest};
use td_objects::dxo::user::{
    UserDB, UserDBWithNames, UserRead, UserReadBuilder, UserUpdate, UserUpdateDB,
    UserUpdateDBBuilder,
};
use td_objects::rest_urls::UserParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{AtTime, UserId, UserIdName};
use td_queries::By;
use td_security::config::PasswordHashingConfig;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = UpdateUserService,
    request = UpdateRequest<UserParam, UserUpdate>,
    response = UserRead,
    connection = TransactionProvider,
    scope = "Global",
    context = AuthzContext,
    context = PasswordHashingConfig,
)]
fn service() {
    layers!(
        from_fn(With::<UpdateRequest<UserParam, UserUpdate>>::extract::<RequestContext>),
        from_fn(With::<RequestContext>::extract::<AtTime>),
        from_fn(With::<RequestContext>::extract::<UserId>),
        from_fn(With::<UpdateRequest<UserParam, UserUpdate>>::extract_name::<UserParam>),
        from_fn(With::<UserParam>::extract::<UserIdName>),
        from_fn(By::<UserIdName>::fetch_one::<UserDB>),
        from_fn(assert_user_is_not_system),
        from_fn(With::<UserDB>::extract::<UserId>),
        from_fn(Scope::<User>::set),
        from_fn(Rbac::<User, (SelfUser, ManageAccess)>::check),
        from_fn(With::<RequestContext>::extract::<UserId>),
        from_fn(With::<UpdateRequest<UserParam, UserUpdate>>::extract_data::<UserUpdate>),
        from_fn(update_user_validate),
        from_fn(update_user_validate_password_change),
        from_fn(update_user_validate_enabled),
        from_fn(With::<UserDB>::try_convert_to::<UserUpdateDBBuilder, _>),
        from_fn(With::<RequestContext>::update::<UserUpdateDBBuilder, _>),
        from_fn(With::<UserUpdate>::update_user_update_db_builder),
        from_fn(With::<UserUpdateDBBuilder>::build::<UserUpdateDB>),
        from_fn(With::<UserDB>::extract::<UserId>),
        from_fn(By::<UserId>::update::<UserUpdateDB, UserDB>),
        from_fn(By::<UserId>::fetch_one::<UserDBWithNames>),
        from_fn(With::<UserDBWithNames>::try_convert_to::<UserReadBuilder, _>),
        from_fn(With::<UserReadBuilder>::build::<UserRead>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_objects::Buildable;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::rest_urls::UserParam;
    use td_objects::types::basic::{
        AccessTokenId, Email, FullName, Password, UserEnabled, UserName,
    };
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_update_user(db: TdPool) {
        use td_tower::metadata::type_of_val;

        UpdateUserService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<UpdateRequest<UserParam, UserUpdate>, UserRead>(&[
                type_of_val(
                    &With::<UpdateRequest<UserParam, UserUpdate>>::extract::<RequestContext>,
                ),
                type_of_val(&With::<RequestContext>::extract::<AtTime>),
                type_of_val(&With::<RequestContext>::extract::<UserId>),
                type_of_val(
                    &With::<UpdateRequest<UserParam, UserUpdate>>::extract_name::<UserParam>,
                ),
                type_of_val(&With::<UserParam>::extract::<UserIdName>),
                type_of_val(&By::<UserIdName>::fetch_one::<UserDB>),
                type_of_val(&assert_user_is_not_system),
                type_of_val(&With::<UserDB>::extract::<UserId>),
                type_of_val(&Scope::<User>::set),
                type_of_val(&Rbac::<User, (SelfUser, ManageAccess)>::check),
                type_of_val(&With::<RequestContext>::extract::<UserId>),
                type_of_val(
                    &With::<UpdateRequest<UserParam, UserUpdate>>::extract_data::<UserUpdate>,
                ),
                type_of_val(&update_user_validate),
                type_of_val(&update_user_validate_password_change),
                type_of_val(&update_user_validate_enabled),
                type_of_val(&With::<UserDB>::try_convert_to::<UserUpdateDBBuilder, _>),
                type_of_val(&With::<RequestContext>::update::<UserUpdateDBBuilder, _>),
                type_of_val(&With::<UserUpdate>::update_user_update_db_builder),
                type_of_val(&With::<UserUpdateDBBuilder>::build::<UserUpdateDB>),
                type_of_val(&With::<UserDB>::extract::<UserId>),
                type_of_val(&By::<UserId>::update::<UserUpdateDB, UserDB>),
                type_of_val(&By::<UserId>::fetch_one::<UserDBWithNames>),
                type_of_val(&With::<UserDBWithNames>::try_convert_to::<UserReadBuilder, _>),
                type_of_val(&With::<UserReadBuilder>::build::<UserRead>),
            ]);
    }

    #[td_test::test(db)]
    async fn test_update_user_admin(db: TdPool) {
        let _ = db
            .seed()
            .user(&UserName::try_from("u0").unwrap(), &UserEnabled::from(true))
            .await;

        let service = UpdateUserService::with_defaults(db.clone()).service();

        let user_update = UserUpdate::builder()
            .full_name(Some(FullName::try_from("U0 Update").unwrap()))
            .email(Some(Email::try_from("u0update@foo.com").unwrap()))
            .password(Some(Password::try_from("new_password").unwrap()))
            .enabled(Some(UserEnabled::from(false)))
            .build()
            .unwrap();

        let before = AtTime::now();
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).update(
            UserParam::builder()
                .user(UserIdName::from_str("u0").unwrap())
                .build()
                .unwrap(),
            user_update,
        );
        let request_time = request.context.time;
        let response = service.raw_oneshot(request).await;
        let updated = response.unwrap();

        assert_eq!(updated.name, UserName::try_from("u0").unwrap());
        assert_eq!(updated.full_name, FullName::try_from("U0 Update").unwrap());
        assert_eq!(
            updated.email.unwrap(),
            Email::try_from("u0update@foo.com").unwrap()
        );
        assert!(updated.created_on < before);
        assert_eq!(updated.created_by_id, UserId::admin());
        assert_eq!(updated.created_by, UserName::admin());
        assert_eq!(
            updated.modified_on.timestamp_millis(),
            request_time.timestamp_millis()
        );
        assert_eq!(updated.modified_by_id, UserId::admin());
        assert_eq!(updated.modified_by, UserName::admin());
        assert!(!*updated.enabled);
        assert!(*updated.password_must_change);

        let user: UserDB = db.live().fetch_one::<UserDB>(()).await.unwrap();
        assert!(*user.password_must_change)
    }

    #[td_test::test(db)]
    async fn test_update_user_self(db: TdPool) {
        let user = db
            .seed()
            .user(&UserName::try_from("u0").unwrap(), &UserEnabled::from(true))
            .await
            .unwrap();

        let service = UpdateUserService::with_defaults(db.clone()).service();

        let user_update = UserUpdate::builder()
            .full_name(Some(FullName::try_from("U0 Update").unwrap()))
            .email(Some(Email::try_from("u0update@foo.com").unwrap()))
            .password(None)
            .enabled(None)
            .build()
            .unwrap();

        let before = AtTime::now();
        let request = RequestContext::with(AccessTokenId::default(), user.id).update(
            UserParam::builder()
                .user(UserIdName::from_str("u0").unwrap())
                .build()
                .unwrap(),
            user_update,
        );
        let request_time = request.context.time;
        let response = service.raw_oneshot(request).await;
        let updated = response.unwrap();

        assert_eq!(updated.name, UserName::try_from("u0").unwrap());
        assert_eq!(updated.full_name, FullName::try_from("U0 Update").unwrap());
        assert_eq!(
            updated.email.unwrap(),
            Email::try_from("u0update@foo.com").unwrap()
        );
        assert!(updated.created_on < before);
        assert_eq!(updated.created_by_id, UserId::admin());
        assert_eq!(updated.created_by, UserName::admin());
        assert_eq!(
            updated.modified_on.timestamp_millis(),
            request_time.timestamp_millis()
        );
        assert_eq!(updated.modified_by_id, user.id);
        assert_eq!(updated.modified_by, user.name);
        assert!(*updated.enabled);
    }

    /// email is tri-state: setting it, then clearing it (the JSON `null` =
    /// `Some(None)` state) persists NULL. Exercises the macro-injected
    /// deserializer plus the `UserUpdateDB.email` tri-state through update_values.
    #[td_test::test(db)]
    async fn test_update_user_clear_email(db: TdPool) {
        let user = db
            .seed()
            .user(&UserName::try_from("u0").unwrap(), &UserEnabled::from(true))
            .await
            .unwrap();

        // Set an email (Some(Email) -> Some(Some(..)) = set).
        let set = UserUpdate::builder()
            .full_name(None)
            .email(Some(Email::try_from("u0@foo.com").unwrap()))
            .password(None)
            .enabled(None)
            .build()
            .unwrap();
        let updated = UpdateUserService::with_defaults(db.clone())
            .service()
            .raw_oneshot(
                RequestContext::with(AccessTokenId::default(), UserId::admin()).update(
                    UserParam::builder()
                        .user(UserIdName::from_str("u0").unwrap())
                        .build()
                        .unwrap(),
                    set,
                ),
            )
            .await
            .unwrap();
        assert_eq!(
            updated.email.unwrap(),
            Email::try_from("u0@foo.com").unwrap()
        );

        // Clear it (Some(None) = the JSON `null` state).
        let clear = UserUpdate::builder()
            .full_name(None)
            .email(Some(None::<Email>))
            .password(None)
            .enabled(None)
            .build()
            .unwrap();
        let cleared = UpdateUserService::with_defaults(db.clone())
            .service()
            .raw_oneshot(
                RequestContext::with(AccessTokenId::default(), UserId::admin()).update(
                    UserParam::builder()
                        .user(UserIdName::from_str("u0").unwrap())
                        .build()
                        .unwrap(),
                    clear,
                ),
            )
            .await
            .unwrap();
        assert!(cleared.email.is_none(), "email cleared via null");

        // It persisted as NULL on the row.
        let db_user: UserDB = db.live().fetch_one::<UserDB>(&user.id).await.unwrap();
        assert!(db_user.email.is_none());
    }
}
