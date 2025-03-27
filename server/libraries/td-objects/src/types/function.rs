//
// Copyright 2025 Tabs Data Inc.
//

use crate::crudl::RequestContext;
use crate::types::basic::{
    AtTime, BundleId, CollectionId, CollectionName, DataLocation, Description, Frozen, FunctionId,
    FunctionName, FunctionRuntimeValues, FunctionStatus, FunctionVersionId, ReuseFrozen, Snippet,
    StorageVersion, TableDependency, TableName, TableTrigger, UserId, UserName,
};

#[td_type::Dao(sql_table = "functions")]
#[td_type(builder(try_from = FunctionVersionDB, skip_all))]
pub struct FunctionDB {
    #[td_type(extractor, builder(include, field = "function_id"))]
    id: FunctionId,
    #[td_type(builder(include))]
    collection_id: CollectionId,
    #[td_type(builder(include))]
    name: FunctionName,
    #[td_type(extractor, builder(include, field = "id"))]
    function_version_id: FunctionVersionId,
    #[builder(default)]
    frozen: Frozen,
    #[td_type(builder(include, field = "defined_on"))]
    created_on: AtTime,
    #[td_type(builder(include, field = "defined_by_id"))]
    created_by_id: UserId,
}

#[td_type::Dao(sql_table = "functions__with_names")]
pub struct FunctionDBWithNames {
    #[td_type(extractor)]
    id: FunctionId,
    collection_id: CollectionId,
    name: FunctionName,
    #[td_type(extractor)]
    function_version_id: FunctionVersionId,
    frozen: Frozen,
    created_on: AtTime,
    created_by_id: UserId,

    collection: CollectionName,
    created_by: UserName,
}

#[td_type::Dto]
#[td_type(builder(try_from = FunctionDBWithNames))]
pub struct Function {
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
    #[td_type(extractor)]
    name: FunctionName,
    description: Description,
    bundle_id: BundleId,
    snippet: Snippet,
    #[td_type(extractor)]
    dependencies: Option<Vec<TableDependency>>,
    #[td_type(extractor)]
    triggers: Option<Vec<TableTrigger>>,
    #[td_type(extractor)]
    tables: Option<Vec<TableName>>,
    runtime_values: FunctionRuntimeValues,
    #[td_type(extractor)]
    reuse_frozen_tables: ReuseFrozen,
}

pub type FunctionUpdate = FunctionCreate;

#[td_type::Dao(sql_table = "function_versions")]
#[td_type(builder(try_from = FunctionCreate, skip_all))]
#[td_type(updater(try_from = RequestContext, skip_all))]
pub struct FunctionVersionDB {
    #[builder(default)]
    #[td_type(extractor)]
    id: FunctionVersionId,
    #[td_type(setter)]
    collection_id: CollectionId,
    #[td_type(builder(include))]
    name: FunctionName,
    #[td_type(builder(include))]
    description: Description,
    #[td_type(builder(include))]
    runtime_values: FunctionRuntimeValues,
    #[builder(default)]
    #[td_type(setter)]
    function_id: FunctionId,
    #[builder(default)] // TODO: remove this
    data_location: DataLocation,
    #[builder(default)] // TODO: remove this
    storage_version: StorageVersion,
    #[td_type(builder(include))]
    bundle_id: BundleId,
    #[td_type(builder(include))]
    snippet: Snippet,
    #[td_type(updater(include, field = "time"))]
    defined_on: AtTime,
    #[td_type(updater(include, field = "user_id"))]
    defined_by_id: UserId,
    #[builder(default)]
    status: FunctionStatus,
}

#[td_type::Dao(sql_table = "function_versions__with_names")]
pub struct FunctionVersionDBWithNames {
    #[td_type(extractor)]
    id: FunctionVersionId,
    collection_id: CollectionId,
    name: FunctionName,
    description: Description,
    #[td_type(extractor)]
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
#[td_type(builder(try_from = FunctionVersionDBWithNames))]
pub struct FunctionVersion {
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
pub struct FunctionVersionWithTables {
    #[serde(flatten)]
    #[td_type(setter)]
    function_version: FunctionVersion,

    #[td_type(setter)]
    dependencies: Vec<TableDependency>,
    #[td_type(setter)]
    triggers: Vec<TableTrigger>,
    #[td_type(setter)]
    tables: Vec<TableName>,
}

#[td_type::Dto]
pub struct FunctionVersionWithAllVersions {
    #[serde(flatten)]
    #[td_type(setter)]
    current: FunctionVersionWithTables,
    #[td_type(setter)]
    all: Vec<FunctionVersion>,
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
            &Columns::Some(FunctionVersionDBWithNames::fields()),
            &Which::all(),
            &Which::all(),
            &With::Names,
        );
        let _res: Vec<FunctionVersionDBWithNames> = sqlx::query_as(statement.sql())
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
