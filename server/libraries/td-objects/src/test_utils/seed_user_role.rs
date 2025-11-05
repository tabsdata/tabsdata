//
// Copyright 2025 Tabs Data Inc.
//

use crate::dxo::crudl::{ReadRequest, RequestContext, handle_sql_err};
use crate::dxo::user_role::defs::{UserRoleDB, UserRoleDBBuilder};
use crate::sql::{DaoQueries, Insert, SelectBy};
use crate::types::SqlEntity;
use crate::types::id::{AccessTokenId, RoleId, UserId};
use td_database::sql::DbPool;
use td_error::TdError;

pub async fn seed_user_role(db: &DbPool, user: &UserId, role: &RoleId) -> UserRoleDB {
    let request_context: ReadRequest<String> = RequestContext::with(
        AccessTokenId::default(),
        UserId::admin(),
        RoleId::sec_admin(),
    )
    .read("");
    let request_context = request_context.context;

    let builder = UserRoleDB::builder();
    let builder = UserRoleDBBuilder::try_from((&request_context, builder)).unwrap();
    let builder = UserRoleDBBuilder::from((user, builder));
    let builder = UserRoleDBBuilder::from((role, builder));
    let user_role_db = builder.build().unwrap();

    let queries = DaoQueries::default();
    queries
        .insert(&user_role_db)
        .unwrap()
        .build()
        .execute(db)
        .await
        .unwrap();

    user_role_db
}

pub async fn get_user_role<E>(db: &DbPool, by: &E) -> Result<UserRoleDB, TdError>
where
    E: SqlEntity,
{
    let queries = DaoQueries::default();
    queries
        .select_by::<UserRoleDB>(by)?
        .build_query_as()
        .fetch_one(db)
        .await
        .map_err(handle_sql_err)
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::test_utils::seed_role::seed_role;
    use crate::test_utils::seed_user::seed_user;
    use crate::types::bool::UserEnabled;
    use crate::types::string::{Description, RoleName, UserName};

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_seed_user_role(db: DbPool) {
        let user = seed_user(
            &db,
            &UserName::try_from("joaquin").unwrap(),
            &UserEnabled::from(false),
        )
        .await;
        let role = seed_role(
            &db,
            RoleName::try_from("king").unwrap(),
            Description::try_from("super user").unwrap(),
        )
        .await;

        let user_role = seed_user_role(&db, &user.id, &role.id).await;

        let found = get_user_role(&db, &role.id).await.unwrap();
        assert_eq!(user_role.id, found.id);
        assert_eq!(user_role.user_id, found.user_id);
        assert_eq!(user_role.role_id, found.role_id);
        assert_eq!(user_role.added_on, found.added_on);
        assert_eq!(user_role.added_by_id, found.added_by_id);
        assert_eq!(user_role.fixed, found.fixed);
    }
}
