//
// Copyright 2025 Tabsdata Inc.
//

use crate::{Queries, Writes};
use td_database::access::SeesProjectRows;
use td_database::domain::Apiserver;
use td_error::TdError;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::user::{UserCreate, UserDB};
use td_objects::types::basic::{
    AccessTokenId, Password, PasswordChangeTime, PasswordHash, PasswordMustChange, UserEnabled,
    UserId, UserName, UserSystem,
};
use td_security::config::PasswordHashingConfig;
use td_security::password::create_password_hash;

pub(crate) async fn seed_user(
    db: impl Writes<Domain = Apiserver, Scope: SeesProjectRows>,
    name: &UserName,
    enabled: &UserEnabled,
) -> Result<UserDB, TdError> {
    seed_user_with(db, name, enabled, &UserSystem::FALSE).await
}

/// Seed a system-flagged user (e.g. to exercise the hiding/immutability that the
/// `system` flag drives for service users). Enabled so tests prove the
/// row is hidden by the `system` flag rather than by `enabled`.
pub(crate) async fn seed_system_user(
    db: impl Writes<Domain = Apiserver, Scope: SeesProjectRows>,
    name: &UserName,
) -> Result<UserDB, TdError> {
    seed_user_with(db, name, &UserEnabled::TRUE, &UserSystem::TRUE).await
}

async fn seed_user_with(
    db: impl Writes<Domain = Apiserver, Scope: SeesProjectRows>,
    name: &UserName,
    enabled: &UserEnabled,
    system: &UserSystem,
) -> Result<UserDB, TdError> {
    let password_hashing_config = PasswordHashingConfig::default();
    let create = UserCreate {
        name: name.clone(),
        full_name: name.to_string().try_into()?,
        email: None,
        password: Password::try_from("password")?,
        enabled: *enabled,
    };

    let request_context = RequestContext::with(AccessTokenId::default(), UserId::admin());

    let user_db = UserDB {
        id: UserId::default(),
        name: create.name,
        full_name: create.full_name,
        email: create.email.clone(),
        created_on: request_context.time.into(),
        created_by_id: request_context.user_id.into(),
        modified_on: request_context.time.into(),
        modified_by_id: request_context.user_id.into(),
        password_hash: PasswordHash::try_from(create_password_hash(
            &password_hashing_config,
            create.password.trim(),
        ))?,
        password_set_on: PasswordChangeTime::try_from(&request_context.time)?,
        password_must_change: PasswordMustChange::FALSE,
        enabled: create.enabled,
        system: *system,
    };

    db.insert(&user_db).await?;
    Ok(user_db)
}

#[cfg(test)]
mod tests {
    use crate::Queries;
    use crate::test_utils::SeedEntry;
    use td_database::pool::TdPool;
    use td_objects::dxo::user::UserDB;
    use td_objects::types::basic::{UserEnabled, UserName};

    #[td_test::test(db)]
    async fn test_seed_user(db: TdPool) {
        let user = db
            .seed()
            .user(&UserName::try_from("joaquin").unwrap(), &UserEnabled::TRUE)
            .await
            .unwrap();
        let found = db.fetch_one::<UserDB>(&user.id).await.unwrap();
        assert_eq!(found, user);
    }
}
