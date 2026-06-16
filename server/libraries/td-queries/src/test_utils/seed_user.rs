//
// Copyright 2025 Tabs Data Inc.
//

use crate::{LiveEntry, QueryExecutor};
use ta_objects::{Buildable, TryUpdater};
use td_error::TdError;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::user::{UserCreate, UserDB};
use td_objects::types::basic::{AccessTokenId, Password, RoleId, UserEnabled, UserId, UserName};
use td_security::config::PasswordHashingConfig;
use td_security::password::create_password_hash;

pub(crate) async fn seed_user(
    db: impl QueryExecutor,
    name: &UserName,
    enabled: &UserEnabled,
) -> Result<UserDB, TdError> {
    let password_hashing_config = PasswordHashingConfig::default();
    let create = UserCreate::builder()
        .name(name.clone())
        .try_full_name(name.to_string())?
        .email(None)
        .password(Password::try_from("password")?)
        .enabled(enabled)
        .build()?;

    let request_context = RequestContext::with(
        AccessTokenId::default(),
        UserId::admin()
    );

    let user_db = UserDB::builder()
        .name(create.name)
        .full_name(create.full_name)
        .email(create.email.clone())
        .try_password_hash(create_password_hash(
            &password_hashing_config,
            create.password.trim(),
        ))?
        .try_password_set_on(request_context.time)?
        .password_must_change(false)
        .enabled(create.enabled)
        .try_update_from(&request_context)?
        .build()?;

    db.live().insert(&user_db).await?;
    Ok(user_db)
}

#[cfg(test)]
mod tests {
    use crate::LiveEntry;
    use crate::test_utils::SeedEntry;
    use td_database::pool::TdPool;
    use td_objects::dxo::user::UserDB;
    use td_objects::types::basic::{UserEnabled, UserName};

    #[td_test::test(db)]
    async fn test_seed_user(db: TdPool) {
        let user = db
            .seed()
            .user(
                &UserName::try_from("joaquin").unwrap(),
                &UserEnabled::from(true),
            )
            .await
            .unwrap();
        let found = db.live().fetch_one::<UserDB>(&user.id).await.unwrap();
        assert_eq!(found, user);
    }
}
