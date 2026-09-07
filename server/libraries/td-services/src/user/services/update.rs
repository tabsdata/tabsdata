//
// Copyright 2024 Tabsdata Inc.
//

use crate::user::UserError;
use crate::user::assert_user_is_not_system;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{ManageAccess, SelfUser, User};
use td_database::access::{GatedConnection, RW};
use td_error::TdError;
use td_objects::FromDao;
use td_objects::dxo::crudl::UpdateRequest;
use td_objects::dxo::user::{
    UserDB, UserDBWithNames, UserParam, UserRead, UserUpdate, UserUpdateDB,
};
use td_objects::types::basic::UserId;
use td_queries::Queries;
use td_security::config::PasswordHashingConfig;

#[service_factory]
async fn update_user(
    authz: Ctx<AuthzContext>,
    password_hashing_config: Ctx<PasswordHashingConfig>,
    gated_trx: &mut GatedConnection<RW>,
    request: UpdateRequest<UserUpdate, UserParam>,
) -> Result<UserRead, TdError> {
    let (ctx, param, update) = request.into_parts();
    let id_name = param.user;
    let conn = gated_trx.get(authz.none());
    let user: UserDB = conn.fetch_one(&id_name).await?;
    assert_user_is_not_system(&user)?;
    let _grant = authz
        .require::<User, (SelfUser, ManageAccess)>(conn, &ctx, &user.id)
        .await?;

    update_user_validate(&update)?;
    update_user_validate_password_change(&ctx.user_id, &update, &user)?;
    update_user_validate_enabled(&ctx.user_id, &user, &update)?;

    let user_id = user.id;
    let update_db = update.to_dao(user, &ctx, &password_hashing_config)?;
    conn.update::<UserUpdateDB>(&update_db, user_id).await?;

    let with_names: UserDBWithNames = conn.fetch_one(&user_id).await?;
    let read = UserRead::from_dao(with_names)?;
    Ok(read)
}

pub fn update_user_validate(update: &UserUpdate) -> Result<(), TdError> {
    if update.full_name.is_none()
        && update.email.is_none()
        && update.password.is_none()
        && update.enabled.is_none()
    {
        Err(UserError::UpdateRequestHasNothingToUpdate)?;
    }
    Ok(())
}

pub fn update_user_validate_password_change(
    request_user_id: &UserId,
    update: &UserUpdate,
    user_db: &UserDB,
) -> Result<(), TdError> {
    #[allow(clippy::collapsible_if)]
    if update.password.is_some() {
        if *request_user_id == user_db.id {
            // a self password change must be done via de password_change endpoint
            Err(UserError::PasswordChangeNotAllowed)?;
        }
        // only a manage_access holder can make it here without being the requester
    }
    Ok(())
}

pub fn update_user_validate_enabled(
    request_user_id: &UserId,
    user_db: &UserDB,
    update: &UserUpdate,
) -> Result<(), TdError> {
    if update.enabled.is_some() && *request_user_id == user_db.id {
        Err(UserError::UserCannotEnableDisableThemselves)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use std::str::FromStr;
    use td_database::pool::TdPool;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::user::UserParam;
    use td_objects::types::basic::UserIdName;
    use td_objects::types::basic::{
        AccessTokenId, AtTime, Email, FullName, Password, UserEnabled, UserId, UserName,
    };
    use td_queries::Queries;
    use td_queries::test_utils::SeedEntry;

    #[td_test::test(db)]
    async fn test_update_user_admin(db: TdPool) {
        let _ = db
            .seed()
            .user(&UserName::try_from("u0").unwrap(), &UserEnabled::TRUE)
            .await;

        let service = Services::with_defaults(db.clone());

        let user_update = UserUpdate {
            full_name: Some(FullName::try_from("U0 Update").unwrap()),
            email: Some(Some(Email::try_from("u0update@foo.com").unwrap())),
            password: Some(Password::try_from("new_password").unwrap()),
            enabled: Some(UserEnabled::FALSE),
        };

        let before = AtTime::now();
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).update(
            UserParam {
                user: UserIdName::from_str("u0").unwrap(),
            },
            user_update,
        );
        let request_time = request.context.time;
        let response = service.update_user(request).await;
        let updated = response.unwrap().data;

        assert_eq!(updated.name, UserName::try_from("u0").unwrap());
        assert_eq!(updated.full_name, FullName::try_from("U0 Update").unwrap());
        assert_eq!(
            updated.email.unwrap(),
            Email::try_from("u0update@foo.com").unwrap()
        );
        assert!(*updated.created_on < before);
        assert_eq!(*updated.created_by_id, UserId::admin());
        assert_eq!(*updated.created_by, UserName::admin());
        assert_eq!(
            updated.modified_on.timestamp_millis(),
            request_time.timestamp_millis()
        );
        assert_eq!(*updated.modified_by_id, UserId::admin());
        assert_eq!(*updated.modified_by, UserName::admin());
        assert!(!*updated.enabled);
        assert!(*updated.password_must_change);

        let user: UserDB = db.fetch_one::<UserDB>(&updated.id).await.unwrap();
        assert!(*user.password_must_change)
    }

    #[td_test::test(db)]
    async fn test_update_user_self(db: TdPool) {
        let user = db
            .seed()
            .user(&UserName::try_from("u0").unwrap(), &UserEnabled::TRUE)
            .await
            .unwrap();

        let service = Services::with_defaults(db.clone());

        let user_update = UserUpdate {
            full_name: Some(FullName::try_from("U0 Update").unwrap()),
            email: Some(Some(Email::try_from("u0update@foo.com").unwrap())),
            password: None,
            enabled: None,
        };

        let before = AtTime::now();
        let request = RequestContext::with(AccessTokenId::default(), user.id).update(
            UserParam {
                user: UserIdName::from_str("u0").unwrap(),
            },
            user_update,
        );
        let request_time = request.context.time;
        let response = service.update_user(request).await;
        let updated = response.unwrap().data;

        assert_eq!(updated.name, UserName::try_from("u0").unwrap());
        assert_eq!(updated.full_name, FullName::try_from("U0 Update").unwrap());
        assert_eq!(
            updated.email.unwrap(),
            Email::try_from("u0update@foo.com").unwrap()
        );
        assert!(*updated.created_on < before);
        assert_eq!(*updated.created_by_id, UserId::admin());
        assert_eq!(*updated.created_by, UserName::admin());
        assert_eq!(
            updated.modified_on.timestamp_millis(),
            request_time.timestamp_millis()
        );
        assert_eq!(*updated.modified_by_id, user.id);
        assert_eq!(*updated.modified_by, user.name);
        assert!(*updated.enabled);
    }

    /// email is tri-state: setting it, then clearing it (the JSON `null` =
    /// `Some(None)` state) persists NULL. Exercises the macro-injected
    /// deserializer plus the `UserUpdateDB.email` tri-state through update_values.
    #[td_test::test(db)]
    async fn test_update_user_clear_email(db: TdPool) {
        let user = db
            .seed()
            .user(&UserName::try_from("u0").unwrap(), &UserEnabled::TRUE)
            .await
            .unwrap();

        // Set an email (Some(Email) -> Some(Some(..)) = set).
        let set = UserUpdate {
            full_name: None,
            email: Some(Some(Email::try_from("u0@foo.com").unwrap())),
            password: None,
            enabled: None,
        };
        let updated = Services::with_defaults(db.clone())
            .update_user(
                RequestContext::with(AccessTokenId::default(), UserId::admin()).update(
                    UserParam {
                        user: UserIdName::from_str("u0").unwrap(),
                    },
                    set,
                ),
            )
            .await
            .unwrap()
            .data;
        assert_eq!(
            updated.email.unwrap(),
            Email::try_from("u0@foo.com").unwrap()
        );

        // Clear it (Some(None) = the JSON `null` state).
        let clear = UserUpdate {
            full_name: None,
            email: Some(None::<Email>),
            password: None,
            enabled: None,
        };
        let cleared = Services::with_defaults(db.clone())
            .update_user(
                RequestContext::with(AccessTokenId::default(), UserId::admin()).update(
                    UserParam {
                        user: UserIdName::from_str("u0").unwrap(),
                    },
                    clear,
                ),
            )
            .await
            .unwrap();
        assert!(cleared.email.is_none(), "email cleared via null");

        // It persisted as NULL on the row.
        let db_user: UserDB = db.fetch_one::<UserDB>(&user.id).await.unwrap();
        assert!(db_user.email.is_none());
    }
}
