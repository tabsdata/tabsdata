//
// Copyright 2025 Tabs Data Inc.
//

use crate::crudl::RequestContext;
use crate::types::basic::{
    AtTime, CollectionId, CollectionName, FunctionId, FunctionName, FunctionVersionId, TableId,
    TableName, TriggerId, TriggerStatus, TriggerVersionId, UserId, UserName,
};
use crate::types::function::FunctionVersionDB;

#[td_type::Dao(sql_table = "triggers")]
#[td_type(builder(try_from = TriggerVersionDB))]
pub struct TriggerDB {
    #[td_type(builder(field = "trigger_id"))]
    id: TriggerId,
    collection_id: CollectionId,
    function_id: FunctionId,
    #[td_type(builder(field = "id"))]
    trigger_version_id: TriggerVersionId,
    trigger_by_collection_id: CollectionId,
    trigger_by_function_id: FunctionId,
    trigger_by_table_id: TableId,
}

#[td_type::Dao(sql_table = "triggers__with_names")]
pub struct TriggerDBWithNames {
    id: TriggerId,
    collection_id: CollectionId,
    function_id: FunctionId,
    trigger_version_id: TriggerVersionId,
    trigger_by_collection_id: CollectionId,
    trigger_by_function_id: FunctionId,
    trigger_by_table_id: TableId,

    collection: CollectionName,
    trigger_by_collection: CollectionName,
    trigger_by_table_name: TableName,
}

#[td_type::Dao(sql_table = "trigger_versions")]
#[td_type(builder(try_from = FunctionVersionDB, skip_all))]
#[td_type(updater(try_from = RequestContext, skip_all))]
pub struct TriggerVersionDB {
    #[builder(default)]
    id: TriggerVersionId,
    #[td_type(builder(include))]
    collection_id: CollectionId,
    #[builder(default)]
    trigger_id: TriggerId,
    #[td_type(builder(include, field = "function_id"))]
    function_id: FunctionId,
    #[td_type(builder(include, field = "id"))]
    function_version_id: FunctionVersionId,
    trigger_by_collection_id: CollectionId,
    trigger_by_function_id: FunctionId,
    trigger_by_function_version_id: FunctionVersionId,
    trigger_by_table_id: TableId,
    #[builder(default = "TriggerStatus::active()")]
    status: TriggerStatus,
    #[td_type(updater(include, field = "time"))]
    defined_on: AtTime,
    #[td_type(updater(include, field = "user_id"))]
    defined_by_id: UserId,
}

#[td_type::Dao(sql_table = "trigger_versions__with_names")]
pub struct TriggerVersionDBWithNames {
    id: TriggerVersionId,
    collection_id: CollectionId,
    trigger_id: TriggerId,
    function_id: FunctionId,
    function_version_id: FunctionVersionId,
    trigger_by_collection_id: CollectionId,
    trigger_by_function_id: FunctionId,
    trigger_by_function_version_id: FunctionVersionId,
    trigger_by_table_id: TableId,
    status: TriggerStatus,
    defined_on: AtTime,
    defined_by_id: UserId,

    collection: CollectionName,
    function: FunctionName,
    trigger_by_collection: CollectionName,
    trigger_by_table_name: TableName,
    trigger_by_function: FunctionName,
    defined_by: UserName,
}

#[td_type::Dto]
#[td_type(builder(try_from = TriggerVersionDBWithNames))]
pub struct TriggerVersionRead {
    id: TriggerVersionId,
    collection_id: CollectionId,
    trigger_id: TriggerId,
    function_id: FunctionId,
    function_version_id: FunctionVersionId,
    trigger_by_collection_id: CollectionId,
    trigger_by_function_id: FunctionId,
    trigger_by_function_version_id: FunctionVersionId,
    trigger_by_table_id: TableId,
    defined_on: AtTime,
    defined_by_id: UserId,

    collection: CollectionName,
    function: FunctionName,
    trigger_by_collection: CollectionName,
    trigger_by_function: FunctionName,
    defined_by: UserName,
}

pub type TriggerVersionDBWithNamesList = TriggerVersionDBWithNames;

pub type TriggerVersionList = TriggerVersionRead;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::trigger;
    use crate::sql::{Columns, Which, With};
    use crate::types::DataAccessObject;
    use td_database::test_utils::db;

    #[tokio::test]
    async fn test_daos_from_row() {
        let db = db().await.unwrap();
        let mut conn = db.acquire().await.unwrap();

        let statement = trigger::Queries::new().select_triggers_current(
            &Columns::Some(TriggerDB::fields()),
            &Which::all(),
            &Which::all(),
            &With::Ids,
        );
        let _res: Vec<TriggerDB> = sqlx::query_as(statement.sql())
            .bind(chrono::Utc::now().to_utc())
            .fetch_all(&mut *conn)
            .await
            .unwrap();

        let statement = trigger::Queries::new().select_triggers_current(
            &Columns::Some(TriggerDBWithNames::fields()),
            &Which::all(),
            &Which::all(),
            &With::Names,
        );
        let _res: Vec<TriggerDBWithNames> = sqlx::query_as(statement.sql())
            .bind(chrono::Utc::now().to_utc())
            .fetch_all(&mut *conn)
            .await
            .unwrap();

        let statement = trigger::Queries::new().select_triggers_at_time(
            &Columns::Some(TriggerVersionDB::fields()),
            &Which::all(),
            &Which::all(),
            &With::Ids,
        );
        let _res: Vec<TriggerVersionDB> = sqlx::query_as(statement.sql())
            .bind(chrono::Utc::now().to_utc())
            .fetch_all(&mut *conn)
            .await
            .unwrap();

        let statement = trigger::Queries::new().select_triggers_at_time(
            &Columns::Some(TriggerVersionDBWithNames::fields()),
            &Which::all(),
            &Which::all(),
            &With::Names,
        );
        let _res: Vec<TriggerVersionDBWithNames> = sqlx::query_as(statement.sql())
            .bind(chrono::Utc::now().to_utc())
            .fetch_all(&mut *conn)
            .await
            .unwrap();

        let statement = trigger::Queries::new().select_triggers_at_time(
            &Columns::Some(TriggerVersionDBWithNamesList::fields()),
            &Which::all(),
            &Which::all(),
            &With::Names,
        );
        let _res: Vec<TriggerVersionDBWithNamesList> = sqlx::query_as(statement.sql())
            .bind(chrono::Utc::now().to_utc())
            .fetch_all(&mut *conn)
            .await
            .unwrap();
    }
}
