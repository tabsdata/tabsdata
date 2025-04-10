//
// Copyright 2025 Tabs Data Inc.
//

use crate::crudl::{handle_sql_err, ReadRequest, RequestContext};
use crate::sql::{DaoQueries, Insert, SelectBy};
use crate::test_utils::seed_user::admin_user;
use crate::types::basic::{RoleId, UserId};
use crate::types::role::{UserRoleDB, UserRoleDBBuilder};
use crate::types::SqlEntity;
use td_database::sql::DbPool;
use td_error::TdError;

pub async fn seed_user_role(db: &DbPool, user: &UserId, role: &RoleId) -> UserRoleDB {
    let admin_id = admin_user(db).await;
    let request_context: ReadRequest<String> =
        RequestContext::with(&admin_id, "r", true).await.read("");
    let request_context = request_context.context();

    let builder = UserRoleDB::builder();
    let builder = UserRoleDBBuilder::try_from((request_context, builder)).unwrap();
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

pub async fn get_user_role<E: SqlEntity>(db: &DbPool, by: &E) -> Result<UserRoleDB, TdError> {
    let queries = DaoQueries::default();
    queries
        .select_by::<UserRoleDB>(&by)?
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
    use crate::types::basic::{Description, RoleName};

    #[tokio::test]
    async fn test_seed_user_role() {
        let db = td_database::test_utils::db().await.unwrap();

        let user_id = seed_user(&db, None, "joaquin", false).await;
        let role = seed_role(
            &db,
            RoleName::try_from("king").unwrap(),
            Description::try_from("super user").unwrap(),
        )
        .await;

        let user_role = seed_user_role(&db, &UserId::from(user_id), role.id()).await;

        let found = get_user_role(&db, role.id()).await.unwrap();
        assert_eq!(user_role.id(), found.id());
        assert_eq!(user_role.user_id(), found.user_id());
        assert_eq!(user_role.role_id(), found.role_id());
        assert_eq!(user_role.added_on(), found.added_on());
        assert_eq!(user_role.added_by_id(), found.added_by_id());
        assert_eq!(user_role.fixed(), found.fixed());
    }
}
