//
// Copyright 2025 Tabs Data Inc.
//

use crate::types::basic::{
    AtTime, EntityId, EntityName, FixedRole, PermissionEntityType, PermissionId, PermissionType,
    RoleId, RoleName, UserId, UserName,
};

#[td_type::Dto]
pub struct PermissionCreate {
    permission_type: PermissionType,
    entity_name: Option<EntityName>, // None means ALL
}

#[td_type::Dao]
#[td_type(builder(try_from = PermissionCreate, skip_all))]
pub struct PermissionDB {
    #[td_type(builder(default))]
    id: PermissionId,
    role_id: RoleId,
    #[td_type(builder(include))]
    permission_type: PermissionType,
    entity_type: PermissionEntityType,
    entity_id: Option<EntityId>,
    granted_by_id: UserId,
    granted_on: AtTime,
    #[td_type(builder(default = false))]
    fixed: FixedRole,
}

#[td_type::Dao]
pub struct PermissionDBWithNames {
    id: PermissionId,
    role_id: RoleId,
    permission_type: PermissionType,
    entity_type: PermissionEntityType,
    entity_id: Option<EntityId>,
    granted_by_id: UserId,
    granted_on: AtTime,
    fixed: FixedRole,

    granted_by: UserName,
    role: RoleName,
    entity: Option<EntityName>,
}

#[td_type::Dto]
#[td_type(builder(try_from = PermissionDBWithNames))]
pub struct PermissionList {
    id: PermissionId,
    role_id: RoleId,
    permission_type: PermissionType,
    entity_type: PermissionEntityType,
    entity_id: Option<EntityId>,
    granted_by_id: UserId,
    granted_on: AtTime,
    fixed: FixedRole,

    granted_by: UserName,
    role: RoleName,
    entity: Option<EntityName>,
}

#[cfg(test)]
mod tests {
    use crate::sql::dependency;
    use crate::sql::{Columns, Which, With};
    use crate::types::dependency::{
        DependencyDB, DependencyDBWithNames, DependencyVersionDB, DependencyVersionDBWithNamesList,
        DependencyVersionDBWithNamesRead,
    };
    use crate::types::DataAccessObject;
    use td_database::test_utils::db;

    #[tokio::test]
    async fn test_daos_from_row() {
        let db = db().await.unwrap();
        let mut conn = db.acquire().await.unwrap();

        let statement = dependency::Queries::new().select_dependencies_current(
            &Columns::Some(DependencyDB::fields()),
            Which::all(),
            Which::all(),
            With::Ids,
        );
        let _res: Vec<DependencyDB> = sqlx::query_as(statement.sql())
            .bind(chrono::Utc::now().to_utc())
            .fetch_all(&mut *conn)
            .await
            .unwrap();

        let statement = dependency::Queries::new().select_dependencies_current(
            &Columns::Some(DependencyDBWithNames::fields()),
            Which::all(),
            Which::all(),
            With::Names,
        );
        let _res: Vec<DependencyDBWithNames> = sqlx::query_as(statement.sql())
            .bind(chrono::Utc::now().to_utc())
            .fetch_all(&mut *conn)
            .await
            .unwrap();

        let statement = dependency::Queries::new().select_dependencies_at_time(
            &Columns::Some(DependencyVersionDB::fields()),
            Which::all(),
            Which::all(),
            With::Ids,
        );
        let _res: Vec<DependencyVersionDB> = sqlx::query_as(statement.sql())
            .bind(chrono::Utc::now().to_utc())
            .fetch_all(&mut *conn)
            .await
            .unwrap();

        let statement = dependency::Queries::new().select_dependencies_at_time(
            &Columns::Some(DependencyVersionDBWithNamesRead::fields()),
            Which::all(),
            Which::all(),
            With::Names,
        );
        let _res: Vec<DependencyVersionDBWithNamesRead> = sqlx::query_as(statement.sql())
            .bind(chrono::Utc::now().to_utc())
            .fetch_all(&mut *conn)
            .await
            .unwrap();

        let statement = dependency::Queries::new().select_dependencies_at_time(
            &Columns::Some(DependencyVersionDBWithNamesList::fields()),
            Which::all(),
            Which::all(),
            With::Names,
        );
        let _res: Vec<DependencyVersionDBWithNamesList> = sqlx::query_as(statement.sql())
            .bind(chrono::Utc::now().to_utc())
            .fetch_all(&mut *conn)
            .await
            .unwrap();
    }
}
