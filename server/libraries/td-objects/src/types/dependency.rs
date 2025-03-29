//
// Copyright 2025 Tabs Data Inc.
//

use crate::crudl::RequestContext;
use crate::types::basic::{
    AtTime, CollectionId, CollectionName, DependencyId, DependencyPos, DependencyStatus,
    DependencyVersionId, FunctionId, FunctionName, FunctionVersionId, TableId, TableName,
    TableVersions, UserId, UserName,
};
use crate::types::function::FunctionVersionDB;

#[td_type::Dao(sql_table = "dependencies")]
#[td_type(builder(try_from = DependencyVersionDB))]
pub struct DependencyDB {
    #[td_type(builder(field = "dependency_id"))]
    id: DependencyId,
    collection_id: CollectionId,
    function_id: FunctionId,
    #[td_type(builder(field = "id"))]
    dependency_version_id: DependencyVersionId,
    table_collection_id: CollectionId,
    table_id: TableId,
    table_name: TableName,
    table_versions: TableVersions,
}

#[td_type::Dao(sql_table = "dependencies__with_names")]
pub struct DependencyDBWithNames {
    id: DependencyId,
    collection_id: CollectionId,
    function_id: FunctionId,
    dependency_version_id: DependencyVersionId,
    table_collection_id: CollectionId,
    table_id: TableId,
    table_name: TableName,
    table_versions: TableVersions,

    collection: CollectionName,
    table_collection: CollectionName,
}

#[td_type::Dao(sql_table = "dependency_versions")]
#[td_type(builder(try_from = FunctionVersionDB, skip_all))]
#[td_type(updater(try_from = RequestContext, skip_all))]
pub struct DependencyVersionDB {
    #[builder(default)]
    id: DependencyVersionId,
    #[td_type(builder(include))]
    collection_id: CollectionId,
    #[builder(default)]
    dependency_id: DependencyId,
    #[td_type(builder(include, field = "function_id"))]
    function_id: FunctionId,
    #[td_type(builder(include, field = "id"))]
    function_version_id: FunctionVersionId,
    table_collection_id: CollectionId,
    table_id: TableId,
    table_name: TableName,
    table_versions: TableVersions,
    dep_pos: DependencyPos,
    #[builder(default = "DependencyStatus::active()")]
    status: DependencyStatus,
    #[td_type(updater(include, field = "time"))]
    defined_on: AtTime,
    #[td_type(updater(include, field = "user_id"))]
    defined_by_id: UserId,
}

#[td_type::Dao(sql_table = "dependency_versions__with_names", order_by = "dep_pos")]
pub struct DependencyVersionDBWithNames {
    id: DependencyVersionId,
    collection_id: CollectionId,
    dependency_id: DependencyId,
    function_id: FunctionId,
    function_version_id: FunctionVersionId,
    table_collection_id: CollectionId,
    table_id: TableId,
    table_name: TableName,
    table_versions: TableVersions,
    dep_pos: DependencyPos,
    status: DependencyStatus,
    defined_on: AtTime,
    defined_by_id: UserId,

    collection: CollectionName,
    function: FunctionName,
    table_collection: CollectionName,
    defined_by: UserName,
}

#[td_type::Dto]
#[td_type(builder(try_from = DependencyVersionDBWithNames))]
pub struct DependencyVersionRead {
    id: DependencyVersionId,
    collection_id: CollectionId,
    dependency_id: DependencyId,
    function_id: FunctionId,
    function_version_id: FunctionVersionId,
    table_collection_id: CollectionId,
    table_id: TableId,
    table_name: TableName,
    table_versions: TableVersions,
    dep_pos: DependencyPos,
    defined_on: AtTime,
    defined_by_id: UserId,

    collection: CollectionName,
    function: FunctionName,
    defined_by: UserName,
}

pub type DependencyVersionDBWithNamesList = DependencyVersionDBWithNames;

pub type DependencyVersionList = DependencyVersionRead;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::dependency;
    use crate::sql::{Columns, Which, With};
    use crate::types::DataAccessObject;
    use td_database::test_utils::db;

    #[tokio::test]
    async fn test_daos_from_row() {
        let db = db().await.unwrap();
        let mut conn = db.acquire().await.unwrap();

        let statement = dependency::Queries::new().select_dependencies_current(
            &Columns::Some(DependencyDB::fields()),
            &Which::all(),
            &Which::all(),
            &With::Ids,
        );
        let _res: Vec<DependencyDB> = sqlx::query_as(statement.sql())
            .bind(chrono::Utc::now().to_utc())
            .fetch_all(&mut *conn)
            .await
            .unwrap();

        let statement = dependency::Queries::new().select_dependencies_current(
            &Columns::Some(DependencyDBWithNames::fields()),
            &Which::all(),
            &Which::all(),
            &With::Names,
        );
        let _res: Vec<DependencyDBWithNames> = sqlx::query_as(statement.sql())
            .bind(chrono::Utc::now().to_utc())
            .fetch_all(&mut *conn)
            .await
            .unwrap();

        let statement = dependency::Queries::new().select_dependencies_at_time(
            &Columns::Some(DependencyVersionDB::fields()),
            &Which::all(),
            &Which::all(),
            &With::Ids,
        );
        let _res: Vec<DependencyVersionDB> = sqlx::query_as(statement.sql())
            .bind(chrono::Utc::now().to_utc())
            .fetch_all(&mut *conn)
            .await
            .unwrap();

        let statement = dependency::Queries::new().select_dependencies_at_time(
            &Columns::Some(DependencyVersionDBWithNames::fields()),
            &Which::all(),
            &Which::all(),
            &With::Names,
        );
        let _res: Vec<DependencyVersionDBWithNames> = sqlx::query_as(statement.sql())
            .bind(chrono::Utc::now().to_utc())
            .fetch_all(&mut *conn)
            .await
            .unwrap();

        let statement = dependency::Queries::new().select_dependencies_at_time(
            &Columns::Some(DependencyVersionDBWithNamesList::fields()),
            &Which::all(),
            &Which::all(),
            &With::Names,
        );
        let _res: Vec<DependencyVersionDBWithNamesList> = sqlx::query_as(statement.sql())
            .bind(chrono::Utc::now().to_utc())
            .fetch_all(&mut *conn)
            .await
            .unwrap();
    }
}
