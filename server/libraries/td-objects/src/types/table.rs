//
// Copyright 2025 Tabs Data Inc.
//

use crate::types::basic::{
    AtTime, CollectionId, CollectionName, Frozen, FunctionId, FunctionVersionId,
    TableFunctionParamPos, TableId, TableName, TableStatus, TableVersionId, UserId, UserName,
};

#[td_type::Dao]
pub struct TableDB {
    id: TableVersionId,
    collection_id: CollectionId,
    name: TableName,
    function_id: FunctionId,
    function_version_id: FunctionVersionId,
    table_version_id: TableVersionId,
    frozen: Frozen,
    created_on: AtTime,
    created_by_id: UserId,
}

#[td_type::Dao]
pub struct TableDBWithNames {
    id: TableVersionId,
    collection_id: CollectionId,
    name: TableName,
    function_id: FunctionId,
    function_version_id: FunctionVersionId,
    table_version_id: TableVersionId,
    frozen: Frozen,
    created_on: AtTime,
    created_by_id: UserId,

    collection: CollectionName,
    created_by: UserName,
}

#[td_type::Dao]
pub struct TableVersionDB {
    id: TableVersionId,
    collection_id: CollectionId,
    table_id: TableId,
    name: TableName,
    function_version_id: FunctionVersionId,
    function_param_pos: Option<TableFunctionParamPos>,
    defined_on: AtTime,
    defined_by_id: UserId,
    status: TableStatus,
}

#[td_type::Dao]
pub struct TableVersionDBWithNamesRead {
    id: TableVersionId,
    collection_id: CollectionId,
    table_id: TableId,
    name: TableName,
    function_version_id: FunctionVersionId,
    function_param_pos: Option<TableFunctionParamPos>,
    defined_on: AtTime,
    defined_by_id: UserId,
    status: TableStatus,

    collection: CollectionName,
    defined_by: UserName,
}

#[td_type::Dto]
#[td_type(builder(try_from = TableVersionDBWithNamesRead))]
pub struct TableVersionRead {
    id: TableVersionId,
    collection_id: CollectionId,
    table_id: TableId,
    name: TableName,
    function_version_id: FunctionVersionId,
    function_param_pos: Option<TableFunctionParamPos>,
    defined_on: AtTime,
    defined_by_id: UserId,
    status: TableStatus,

    collection: CollectionName,
    defined_by: UserName,
}

pub type TableVersionDBWithNamesList = TableVersionDBWithNamesRead;

pub type TableVersionList = TableVersionRead;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::table;
    use crate::sql::{Columns, Which, With};
    use crate::types::DataAccessObject;
    use td_database::test_utils::db;

    #[tokio::test]
    async fn test_daos_from_row() {
        let db = db().await.unwrap();
        let mut conn = db.acquire().await.unwrap();

        let statement = table::Queries::new().select_tables_current(
            &Columns::Some(TableDB::fields()),
            &Which::all(),
            &Which::all(),
            &With::Ids,
        );
        let _res: Vec<TableDB> = sqlx::query_as(statement.sql())
            .bind(chrono::Utc::now().to_utc())
            .fetch_all(&mut *conn)
            .await
            .unwrap();

        let statement = table::Queries::new().select_tables_current(
            &Columns::Some(TableDBWithNames::fields()),
            &Which::all(),
            &Which::all(),
            &With::Names,
        );
        let _res: Vec<TableDBWithNames> = sqlx::query_as(statement.sql())
            .bind(chrono::Utc::now().to_utc())
            .fetch_all(&mut *conn)
            .await
            .unwrap();

        let statement = table::Queries::new().select_tables_at_time(
            &Columns::Some(TableVersionDB::fields()),
            &Which::all(),
            &Which::all(),
            &With::Ids,
        );
        let _res: Vec<TableVersionDB> = sqlx::query_as(statement.sql())
            .bind(chrono::Utc::now().to_utc())
            .fetch_all(&mut *conn)
            .await
            .unwrap();

        let statement = table::Queries::new().select_tables_at_time(
            &Columns::Some(TableVersionDBWithNamesRead::fields()),
            &Which::all(),
            &Which::all(),
            &With::Names,
        );
        let _res: Vec<TableVersionDBWithNamesRead> = sqlx::query_as(statement.sql())
            .bind(chrono::Utc::now().to_utc())
            .fetch_all(&mut *conn)
            .await
            .unwrap();

        let statement = table::Queries::new().select_tables_at_time(
            &Columns::Some(TableVersionDBWithNamesList::fields()),
            &Which::all(),
            &Which::all(),
            &With::Names,
        );
        let _res: Vec<TableVersionDBWithNamesList> = sqlx::query_as(statement.sql())
            .bind(chrono::Utc::now().to_utc())
            .fetch_all(&mut *conn)
            .await
            .unwrap();
    }
}
