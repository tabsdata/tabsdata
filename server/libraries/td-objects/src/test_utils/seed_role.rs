//
// Copyright 2025 Tabs Data Inc.
//

use crate::crudl::{handle_sql_err, ReadRequest, RequestContext};
use crate::sql::{DaoQueries, Insert, SelectBy};
use crate::types::basic::{AccessTokenId, Description, RoleId, RoleName, UserId};
use crate::types::role::{RoleCreate, RoleDB, RoleDBBuilder};
use crate::types::SqlEntity;
use td_database::sql::DbPool;
use td_error::TdError;

pub async fn seed_role(db: &DbPool, name: RoleName, description: Description) -> RoleDB {
    let role_create = RoleCreate::builder()
        .name(name)
        .description(description)
        .build()
        .unwrap();

    let request_context: ReadRequest<String> = RequestContext::with(
        AccessTokenId::default(),
        UserId::admin(),
        RoleId::sec_admin(),
    )
    .read("");
    let request_context = request_context.context();

    let builder = RoleDBBuilder::try_from(&role_create).unwrap();
    let builder = RoleDBBuilder::try_from((request_context, builder)).unwrap();
    let role_db = builder.build().unwrap();

    let queries = DaoQueries::default();
    queries
        .insert(&role_db)
        .unwrap()
        .build()
        .execute(db)
        .await
        .unwrap();

    role_db
}

pub async fn get_role<E>(db: &DbPool, by: &E) -> Result<RoleDB, TdError>
where
    E: SqlEntity,
{
    let queries = DaoQueries::default();
    queries
        .select_by::<RoleDB>(&by)?
        .build_query_as()
        .fetch_one(db)
        .await
        .map_err(handle_sql_err)
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::test_utils::seed_role::seed_role;

    #[tokio::test]
    async fn test_seed_role() {
        let db = td_database::test_utils::db().await.unwrap();
        let role = seed_role(
            &db,
            RoleName::try_from("joaquin").unwrap(),
            Description::try_from("super user").unwrap(),
        )
        .await;

        let found = get_role(&db, &RoleName::try_from("joaquin").unwrap())
            .await
            .unwrap();
        assert_eq!(role.id(), found.id());
        assert_eq!(role.name(), found.name());
        assert_eq!(role.description(), found.description());
        assert_eq!(role.created_on(), found.created_on());
        assert_eq!(role.created_by_id(), found.created_by_id());
        assert_eq!(role.modified_on(), found.modified_on());
        assert_eq!(role.modified_by_id(), found.modified_by_id());
        assert_eq!(role.fixed(), found.fixed());
    }
}
