//
// Copyright 2025 Tabs Data Inc.
//

use crate::types::basic::{
    AtTime, CollectionId, CollectionName, FunctionId, FunctionName, FunctionVersionId, TableId,
    TableName, TriggerId, TriggerVersionId, UserId, UserName,
};

#[td_type::Dao]
pub struct TriggerDB {
    id: TriggerId,
    collection_id: CollectionId,
    function_id: FunctionId,
    trigger_version_id: TriggerVersionId,
    trigger_by_collection_id: CollectionId,
    trigger_by_function_id: FunctionId,
    trigger_by_table_id: TableId,
}

#[td_type::Dao]
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

#[td_type::Dao]
pub struct TriggerVersionDB {
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
}

#[td_type::Dao]
pub struct TriggerVersionDBWithNamesRead {
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

#[td_type::Dto]
#[td_type(builder(try_from = TriggerVersionDBWithNamesRead))]
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

pub type TriggerVersionDBWithNamesList = TriggerVersionDBWithNamesRead;

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
            Which::all(),
            Which::all(),
            With::Ids,
        );
        let _res: Vec<TriggerDB> = sqlx::query_as(statement.sql())
            .bind(chrono::Utc::now().to_utc())
            .fetch_all(&mut *conn)
            .await
            .unwrap();

        let statement = trigger::Queries::new().select_triggers_current(
            &Columns::Some(TriggerDBWithNames::fields()),
            Which::all(),
            Which::all(),
            With::Names,
        );
        let _res: Vec<TriggerDBWithNames> = sqlx::query_as(statement.sql())
            .bind(chrono::Utc::now().to_utc())
            .fetch_all(&mut *conn)
            .await
            .unwrap();

        let statement = trigger::Queries::new().select_triggers_at_time(
            &Columns::Some(TriggerVersionDB::fields()),
            Which::all(),
            Which::all(),
            With::Ids,
        );
        let _res: Vec<TriggerVersionDB> = sqlx::query_as(statement.sql())
            .bind(chrono::Utc::now().to_utc())
            .fetch_all(&mut *conn)
            .await
            .unwrap();

        let statement = trigger::Queries::new().select_triggers_at_time(
            &Columns::Some(TriggerVersionDBWithNamesRead::fields()),
            Which::all(),
            Which::all(),
            With::Names,
        );
        let _res: Vec<TriggerVersionDBWithNamesRead> = sqlx::query_as(statement.sql())
            .bind(chrono::Utc::now().to_utc())
            .fetch_all(&mut *conn)
            .await
            .unwrap();

        let statement = trigger::Queries::new().select_triggers_at_time(
            &Columns::Some(TriggerVersionDBWithNamesList::fields()),
            Which::all(),
            Which::all(),
            With::Names,
        );
        let _res: Vec<TriggerVersionDBWithNamesList> = sqlx::query_as(statement.sql())
            .bind(chrono::Utc::now().to_utc())
            .fetch_all(&mut *conn)
            .await
            .unwrap();
    }
}
