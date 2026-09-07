//
// Copyright 2025 Tabsdata Inc.
//

use crate::QueryExecutor;
use crate::{CurrentEntry, LiveEntry};
use ta_objects::and;
use td_database::domain::{Apiserver, Domained};
use td_error::TdError;
use td_objects::dxo::collection::ActiveCollectionDB;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::dependency::DependencyDB;
use td_objects::dxo::function::{FunctionDB, FunctionRegister};
use td_objects::dxo::table::TableDB;
use td_objects::dxo::table::TableDBWithNames;
use td_objects::dxo::trigger::TriggerDB;
use td_objects::dxo::worker::WorkerDB;
use td_objects::types::basic::{
    AccessTokenId, BasePath, DependencyId, DependencyPos, DependencyStatus, DependencyVersionId,
    FunctionId, FunctionStatus, FunctionVersionId, Partitioned, Private, Snippet, StageId,
    StorageVersion, System, TableFunctionParamPos, TableId, TableName, TableStatus, TableType,
    TableVersionId, TriggerId, TriggerStatus, TriggerVersionId, UserId, WorkerId,
    WorkerMessageStatus, WorkerStatus,
};

/// Runs every statement on the caller's executor; if it is project-scoped
/// (`db.scoped(p)`), the field-less rows (function, tables, dependencies,
/// triggers) default their `project_id` from that scope.
pub(crate) async fn seed_function(
    mut executor: impl QueryExecutor + Domained<Domain = Apiserver>,
    collection: &ActiveCollectionDB,
    function_create: &FunctionRegister,
) -> Result<FunctionDB, TdError> {
    let request_context = RequestContext::with(AccessTokenId::default(), UserId::admin());

    let function_db = FunctionDB {
        id: FunctionVersionId::new(),
        collection_id: collection.id,
        name: function_create.name.clone(),
        description: function_create.description.clone(),
        decorator: function_create.decorator.clone(),
        connector: function_create.connector.clone(),
        connection: function_create.connection.clone(),
        runtime_values: function_create.runtime_values.clone(),
        function_id: FunctionId::default(),
        base_path: function_create.base_path.clone(),
        stage_id: StageId::default(),
        stage_base_path: BasePath::default(),
        storage_version: StorageVersion::default(),
        bundle_id: function_create.bundle_id,
        snippet: Snippet::try_from(&function_create.snippet)?,
        defined_on: request_context.time,
        defined_by_id: request_context.user_id,
        status: FunctionStatus::Active,
        enabled: function_create.enabled.unwrap_or_default(),
    };

    // Insert function version
    executor.reuse().live().insert(&function_db).await?;

    // Dependencies, tables, triggers
    // Very similar to build_table_versions
    if let Some(tables) = &function_create.tables {
        for (pos, table_name) in tables.iter().enumerate() {
            let name = TableName::try_from(table_name)?;
            let table = TableDB {
                id: TableVersionId::default(),
                collection_id: function_db.collection_id,
                table_id: TableId::default(),
                private: Private::from(name.is_private()),
                name,
                function_id: function_db.function_id,
                function_version_id: function_db.id.clone(),
                function_param_pos: Some(TableFunctionParamPos::try_from(pos as i32)?),
                partitioned: Partitioned::FALSE,
                defined_on: request_context.time,
                defined_by_id: request_context.user_id,
                status: TableStatus::Active,
                table_type: TableType::default(),
            };

            executor.reuse().live().insert(&table).await?;
        }
    }

    // Very similar to build_dependency_versions
    if let Some(dependency_tables) = &function_create.dependencies {
        for (pos, dependency_table) in dependency_tables.iter().enumerate() {
            let (table_collection, table_name) = {
                let collection = dependency_table
                    .collection
                    .clone()
                    .unwrap_or(collection.name.clone());
                (
                    collection,
                    TableName::try_from(dependency_table.table.clone())?,
                )
            };

            let table_db: TableDBWithNames = executor
                .reuse()
                .current()
                .fetch_one::<_>(and!(table_collection, table_name))
                .await?;

            let dependency = DependencyDB {
                id: DependencyVersionId::default(),
                collection_id: function_db.collection_id,
                dependency_id: DependencyId::default(),
                function_id: function_db.function_id,
                table_collection_id: table_db.base.collection_id.into(),
                table_function_id: table_db.base.function_id.into(),
                table_id: table_db.base.table_id,
                table_versions: dependency_table.versions.clone().into(),
                dep_pos: DependencyPos::try_from(pos as i32)?,
                status: DependencyStatus::Active,
                defined_on: request_context.time,
                defined_by_id: request_context.user_id,
                system: System::FALSE,
            };

            executor.reuse().live().insert(&dependency).await?;
        }
    }

    // Very similar to build_trigger_versions
    if let Some(trigger_tables) = &function_create.triggers {
        for trigger_table in trigger_tables {
            let (table_collection, table_name) = {
                let collection = trigger_table
                    .collection
                    .clone()
                    .unwrap_or(collection.name.clone());
                (
                    collection,
                    TableName::try_from(trigger_table.table.clone())?,
                )
            };

            let table_db: TableDBWithNames = executor
                .reuse()
                .current()
                .fetch_one::<_>(and!(table_collection, table_name))
                .await?;

            let trigger = TriggerDB {
                id: TriggerVersionId::default(),
                collection_id: function_db.collection_id,
                trigger_id: TriggerId::default(),
                function_id: function_db.function_id,
                trigger_by_collection_id: table_db.base.collection_id.into(),
                trigger_by_function_id: table_db.base.function_id.into(),
                trigger_by_table_id: table_db.base.table_id,
                status: TriggerStatus::Active,
                defined_on: request_context.time,
                defined_by_id: request_context.user_id,
                system: System::FALSE,
            };

            executor.reuse().live().insert(&trigger).await?;
        }
    }

    // Very similar to build_worker
    if let Some(workers) = &function_create.workers {
        for (worker_type, worker_config) in workers {
            let worker = WorkerDB {
                id: WorkerId::new(),
                // `WorkerDB.project_id` is not derived from the function; stamp
                // it from the collection.
                project_id: collection.project_id,
                collection_id: function_db.collection_id,
                execution_id: None,
                transaction_id: None,
                physical_function_run_id: None,
                function_version_id: function_db.id.clone(),
                message_status: WorkerMessageStatus::None,
                started_on: None,
                ended_on: None,
                status: WorkerStatus::Scheduled,
                worker_type: worker_type.clone().into(),
                worker_enabled: function_db.enabled,
                worker_config: Some(worker_config.clone()),
            };

            executor.reuse().live().insert(&worker).await?;
        }
    }

    Ok(function_db)
}

#[cfg(test)]
mod tests {
    use crate::HistoricEntry;
    use crate::test_utils::SeedEntry;
    use td_database::pool::TdPool;
    use td_objects::dxo::function::{FunctionDB, FunctionRegister};
    use td_objects::types::basic::ReuseFrozen;
    use td_objects::types::basic::{AtTime, BundleId, CollectionName, Decorator, UserId};
    use td_security::ENCODED_ID_SYSTEM;

    #[td_test::test(db)]
    async fn test_seed_function(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(
                &CollectionName::try_from("collection").unwrap(),
                &UserId::try_from(ENCODED_ID_SYSTEM).unwrap(),
            )
            .await
            .unwrap();

        let dependencies = None;
        let triggers = None;
        let tables = None;

        let create = FunctionRegister {
            name: "joaquin".try_into().unwrap(),
            description: "function_foo description".try_into().unwrap(),
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into().unwrap(),
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies,
            triggers,
            tables,
            table_types: None,
            runtime_values: "mock runtime values".try_into().unwrap(),
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let function = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await
            .unwrap();
        let found = db
            .historic(&AtTime::now())
            .fetch_one::<FunctionDB>(())
            .await
            .unwrap();

        assert_eq!(function.id, found.id);
        assert_eq!(function.collection_id, found.collection_id);
        assert_eq!(function.name, found.name);
        assert_eq!(function.description, found.description);
        assert_eq!(function.runtime_values, found.runtime_values);
        assert_eq!(function.function_id, found.function_id);
        assert_eq!(function.storage_version, found.storage_version);
        assert_eq!(function.bundle_id, found.bundle_id);
        assert_eq!(function.snippet, found.snippet);
        assert_eq!(function.defined_on, found.defined_on);
        assert_eq!(function.defined_by_id, found.defined_by_id);
        assert_eq!(function.status, found.status);
        assert_eq!(function.enabled, found.enabled);
    }
}
