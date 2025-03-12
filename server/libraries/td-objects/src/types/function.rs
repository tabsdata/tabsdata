//
// Copyright 2025 Tabs Data Inc.
//

use crate::types::basic::{
    AtTime, BundleId, CollectionId, CollectionName, DataLocation, Description, Frozen, FunctionId,
    FunctionName, FunctionRuntimeValues, FunctionStatus, FunctionVersionId, Snippet,
    StorageVersion, TableDependency, TableName, TableTrigger, UserId, UserName,
};

#[td_type::Dao]
pub struct FunctionDB {
    id: FunctionId,
    collection_id: CollectionId,
    name: FunctionName,
    function_version_id: FunctionVersionId,
    frozen: Frozen,
    created_on: AtTime,
    created_by_id: UserId,
}

#[td_type::Dao]
pub struct FunctionDBWithNames {
    id: FunctionId,
    collection_id: CollectionId,
    name: FunctionName,
    function_version_id: FunctionVersionId,
    frozen: Frozen,
    created_on: AtTime,
    created_by_id: UserId,

    collection: CollectionName,
    created_by: UserName,
}

#[td_type::Dto]
pub struct FunctionCreate {
    name: FunctionName,
    description: Description,
    bundle_id: BundleId,
    snippet: Snippet,
    dependencies: Option<Vec<TableDependency>>,
    triggers: Option<Vec<TableTrigger>>,
    tables: Option<Vec<TableName>>,
    runtime_values: FunctionRuntimeValues,
}

pub type FunctionUpdate = FunctionCreate;

#[td_type::Dao]
#[td_type(builder(try_from = FunctionCreate, skip_all))]
pub struct FunctionVersionDB {
    id: FunctionId,
    collection_id: CollectionId,
    #[td_type(builder(include))]
    name: FunctionName,
    #[td_type(builder(include))]
    description: Description,
    function_id: FunctionVersionId,
    data_location: DataLocation,
    storage_version: StorageVersion,
    #[td_type(builder(include))]
    bundle_id: BundleId,
    #[td_type(builder(include))]
    snippet: Snippet,
    defined_on: AtTime,
    defined_by_id: UserId,
    status: FunctionStatus,
}

#[td_type::Dao]
pub struct FunctionVersionDBWithNamesRead {
    id: FunctionVersionId,
    collection_id: CollectionId,
    name: FunctionName,
    description: Description,
    function_id: FunctionId,
    data_location: DataLocation,
    storage_version: StorageVersion,
    bundle_id: BundleId,
    snippet: Snippet,
    defined_on: AtTime,
    defined_by_id: UserId,
    status: FunctionStatus,

    collection: CollectionName,
    defined_by: UserName,
}

#[td_type::Dto]
#[td_type(builder(try_from = FunctionVersionDBWithNamesRead))]
pub struct FunctionVersionRead {
    id: FunctionVersionId,
    collection_id: CollectionId,
    name: FunctionName,
    description: Description,
    function_id: FunctionId,
    data_location: DataLocation,
    storage_version: StorageVersion,
    bundle_id: BundleId,
    snippet: Snippet,
    defined_on: AtTime,
    defined_by_id: UserId,
    status: FunctionStatus,

    collection: CollectionName,
    defined_by: UserName,

    #[td_type(builder(skip))]
    dependencies: Option<Vec<TableDependency>>,
    #[td_type(builder(skip))]
    triggers: Option<Vec<TableTrigger>>,
    #[td_type(builder(skip))]
    tables: Option<Vec<TableName>>,
}

#[td_type::Dao]
pub struct FunctionVersionDBWithNamesList {
    id: FunctionVersionId,
    collection_id: CollectionId,
    name: FunctionName,
    function_id: FunctionId,
    defined_on: AtTime,
    defined_by_id: UserId,
    status: FunctionStatus,

    collection: CollectionName,
    defined_by: UserName,
}

#[td_type::Dto]
#[td_type(builder(try_from = FunctionVersionDBWithNamesList))]
pub struct FunctionVersionList {
    id: FunctionVersionId,
    collection_id: CollectionId,
    name: FunctionName,
    function_id: FunctionId,
    defined_on: AtTime,
    defined_by_id: UserId,
    status: FunctionStatus,

    collection: CollectionName,
    defined_by: UserName,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::function;
    use crate::sql::{Columns, Which, With};
    use crate::types::DataAccessObject;
    use td_database::test_utils::db;

    #[tokio::test]
    async fn test_daos_from_row() {
        let db = db().await.unwrap();
        let mut conn = db.acquire().await.unwrap();

        let statement = function::Queries::new().select_functions_current(
            &Columns::Some(FunctionDB::fields()),
            &Which::all(),
            &Which::all(),
            &With::Ids,
        );
        let _res: Vec<FunctionDB> = sqlx::query_as(statement.sql())
            .bind(chrono::Utc::now().to_utc())
            .fetch_all(&mut *conn)
            .await
            .unwrap();

        let statement = function::Queries::new().select_functions_current(
            &Columns::Some(FunctionDBWithNames::fields()),
            &Which::all(),
            &Which::all(),
            &With::Names,
        );
        let _res: Vec<FunctionDBWithNames> = sqlx::query_as(statement.sql())
            .bind(chrono::Utc::now().to_utc())
            .fetch_all(&mut *conn)
            .await
            .unwrap();

        let statement = function::Queries::new().select_functions_at_time(
            &Columns::Some(FunctionVersionDB::fields()),
            &Which::all(),
            &Which::all(),
            &With::Ids,
        );
        let _res: Vec<FunctionVersionDB> = sqlx::query_as(statement.sql())
            .bind(chrono::Utc::now().to_utc())
            .fetch_all(&mut *conn)
            .await
            .unwrap();

        let statement = function::Queries::new().select_functions_at_time(
            &Columns::Some(FunctionVersionDBWithNamesRead::fields()),
            &Which::all(),
            &Which::all(),
            &With::Names,
        );
        let _res: Vec<FunctionVersionDBWithNamesRead> = sqlx::query_as(statement.sql())
            .bind(chrono::Utc::now().to_utc())
            .fetch_all(&mut *conn)
            .await
            .unwrap();

        let statement = function::Queries::new().select_functions_at_time(
            &Columns::Some(FunctionVersionDBWithNamesList::fields()),
            &Which::all(),
            &Which::all(),
            &With::Names,
        );
        let _res: Vec<FunctionVersionDBWithNamesList> = sqlx::query_as(statement.sql())
            .bind(chrono::Utc::now().to_utc())
            .fetch_all(&mut *conn)
            .await
            .unwrap();
    }
}
