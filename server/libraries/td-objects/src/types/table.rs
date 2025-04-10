//
// Copyright 2025 Tabs Data Inc.
//

use crate::crudl::RequestContext;
use crate::types::basic::{
    AtTime, CollectionId, CollectionName, Frozen, FunctionId, FunctionVersionId, Private,
    TableFunctionParamPos, TableId, TableName, TableStatus, TableVersionId, UserId, UserName,
};
use crate::types::function::{FunctionDB, FunctionVersionDB};

#[td_type::Dao(sql_table = "tables")]
#[td_type(builder(try_from = TableVersionDB, skip_all))]
#[td_type(updater(try_from = FunctionDB, skip_all))]
pub struct TableDB {
    #[td_type(builder(include, field = "table_id"))]
    id: TableId,
    #[td_type(builder(include))]
    collection_id: CollectionId,
    #[td_type(builder(include))]
    name: TableName,
    #[td_type(updater(include, field = "id"))]
    function_id: FunctionId,
    #[td_type(builder(include))]
    function_version_id: FunctionVersionId,
    #[td_type(builder(include, field = "id"))]
    table_version_id: TableVersionId,
    #[builder(default = "Frozen::from(false)")]
    frozen: Frozen,
    #[builder(default = "Private::from(false)")]
    private: Private,
    #[td_type(builder(include, field = "defined_on"))]
    created_on: AtTime,
    #[td_type(builder(include, field = "defined_by_id"))]
    created_by_id: UserId,
}

#[td_type::Dao(sql_table = "tables")]
#[td_type(builder(try_from = TableDB))]
pub struct UpdateTableDB {
    function_id: FunctionId,
    function_version_id: FunctionVersionId,
    table_version_id: TableVersionId,
    #[td_type(builder(skip))]
    frozen: Frozen,
    private: Private,
}

#[td_type::Dao(sql_table = "tables__with_names")]
pub struct TableDBWithNames {
    id: TableId,
    collection_id: CollectionId,
    name: TableName,
    function_id: FunctionId,
    function_version_id: FunctionVersionId,
    table_version_id: TableVersionId,
    frozen: Frozen,
    private: Private,
    created_on: AtTime,
    created_by_id: UserId,

    collection: CollectionName,
    created_by: UserName,
}

#[td_type::Dao(sql_table = "table_versions")]
#[td_type(builder(try_from = FunctionVersionDB, skip_all))]
#[td_type(updater(try_from = RequestContext, skip_all))]
pub struct TableVersionDB {
    #[builder(default)]
    id: TableVersionId,
    #[td_type(extractor, builder(include))]
    collection_id: CollectionId,
    table_id: TableId,
    #[td_type(extractor)]
    name: TableName,
    #[td_type(builder(include, field = "id"))]
    function_version_id: FunctionVersionId,
    function_param_pos: Option<TableFunctionParamPos>,
    #[td_type(updater(include, field = "time"))]
    defined_on: AtTime,
    #[td_type(updater(include, field = "user_id"))]
    defined_by_id: UserId,
    #[builder(default = "TableStatus::active()")]
    status: TableStatus,
}

#[td_type::Dao(
    sql_table = "table_versions__with_names",
    order_by = "function_param_pos"
)]
pub struct TableVersionDBWithNames {
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
#[td_type(builder(try_from = TableVersionDBWithNames))]
pub struct TableVersion {
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
            &Columns::Some(TableVersionDBWithNames::fields()),
            &Which::all(),
            &Which::all(),
            &With::Names,
        );
        let _res: Vec<TableVersionDBWithNames> = sqlx::query_as(statement.sql())
            .bind(chrono::Utc::now().to_utc())
            .fetch_all(&mut *conn)
            .await
            .unwrap();
    }
}
