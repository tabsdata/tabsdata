//
// Copyright 2025 Tabs Data Inc.
//

use crate::crudl::{ReadRequest, RequestContext, handle_sql_err};
use crate::sql::{DaoQueries, Insert, SelectBy};
use crate::types::SqlEntity;
use crate::types::basic::{AccessTokenId, Password, RoleId, UserEnabled, UserId, UserName};
use crate::types::user::{UserCreate, UserDB, UserDBBuilder};
use td_database::sql::DbPool;
use td_error::TdError;
use td_security::config::PasswordHashingConfig;
use td_security::password::create_password_hash;

pub async fn seed_user(db: &DbPool, name: &UserName, enabled: &UserEnabled) -> UserDB {
    let password_hashing_config = PasswordHashingConfig::default();
    let create = UserCreate::builder()
        .name(name)
        .try_full_name(name.to_string())
        .unwrap()
        .email(None)
        .password(Password::try_from("password").unwrap())
        .enabled(enabled)
        .build()
        .unwrap();

    let request_context: ReadRequest<String> = RequestContext::with(
        AccessTokenId::default(),
        UserId::admin(),
        RoleId::sec_admin(),
    )
    .read("");
    let request_context = request_context.context();

    let mut builder = UserDB::builder();
    builder
        .name(create.name())
        .full_name(create.full_name())
        .email(create.email().clone())
        .try_password_hash(create_password_hash(
            &password_hashing_config,
            create.password().trim(),
        ))
        .unwrap()
        .try_password_set_on(request_context.time())
        .unwrap()
        .password_must_change(false)
        .enabled(create.enabled());
    let builder = UserDBBuilder::try_from((request_context, builder)).unwrap();
    let user_db = builder.build().unwrap();

    let queries = DaoQueries::default();
    queries
        .insert(&user_db)
        .unwrap()
        .build()
        .execute(db)
        .await
        .unwrap();

    user_db
}

pub async fn get_user<E>(db: &DbPool, by: &E) -> Result<UserDB, TdError>
where
    E: SqlEntity,
{
    let queries = DaoQueries::default();
    queries
        .select_by::<UserDB>(&by)?
        .build_query_as()
        .fetch_one(db)
        .await
        .map_err(handle_sql_err)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::seed_user::seed_user;

    #[td_test::test(sqlx)]
    async fn test_seed_user(db: DbPool) {
        let user = seed_user(
            &db,
            &UserName::try_from("joaquin").unwrap(),
            &UserEnabled::from(true),
        )
        .await;
        let found = get_user(&db, user.id()).await.unwrap();
        assert_eq!(found, user);
    }
}
