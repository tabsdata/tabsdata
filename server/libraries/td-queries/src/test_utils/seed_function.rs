//
// Copyright 2025 Tabsdata Inc.
//

use crate::QueryExecutor;
use crate::{CurrentEntry, LiveEntry};
use ta_objects::TryUpdater;
use ta_objects::and;
use td_database::domain::{Apiserver, Domained};
use td_error::TdError;
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::dependency::DependencyDBBuilder;
use td_objects::dxo::function::{FunctionDB, FunctionDBBuilder, FunctionRegister};
use td_objects::dxo::table::{TableDBBuilder, TableDBWithNames};
use td_objects::dxo::trigger::TriggerDBBuilder;
use td_objects::dxo::worker::WorkerDBBuilder;
use td_objects::types::basic::{
    AccessTokenId, DependencyPos, DependencyStatus, StorageVersion, TableFunctionParamPos, TableId,
    TableName, TableStatus, TriggerStatus, UserId,
};

/// Runs every statement on the caller's executor; if it is project-scoped
/// (`db.scoped(p)`), the field-less rows (function, tables, dependencies,
/// triggers) default their `project_id` from that scope.
pub(crate) async fn seed_function(
    mut executor: impl QueryExecutor + Domained<Domain = Apiserver>,
    collection: &CollectionDB,
    function_create: &FunctionRegister,
) -> Result<FunctionDB, TdError> {
    let request_context = RequestContext::with(AccessTokenId::default(), UserId::admin());

    // Function version builder
    let mut builder = FunctionDBBuilder::try_from(function_create)?;
    let function_db = builder
        .try_update_from(&request_context)?
        .collection_id(collection.id)
        .storage_version(StorageVersion::default())
        .enabled(function_create.enabled.unwrap_or_default())
        .build()?;

    // Insert function version
    executor.reuse().live().insert(&function_db).await?;

    // Dependencies, tables, triggers
    // Very similar to build_table_versions
    if let Some(tables) = &function_create.tables {
        let mut builder = TableDBBuilder::try_from(&function_db)?;
        let builder = builder.try_update_from(&request_context)?;

        for (pos, table_name) in tables.iter().enumerate() {
            let name = TableName::try_from(table_name)?;
            let table = builder
                .clone()
                .table_id(TableId::default())
                .private(name.is_private())
                .name(name)
                .function_param_pos(Some(TableFunctionParamPos::try_from(pos as i32)?))
                .status(TableStatus::Active)
                .build()?;

            executor.reuse().live().insert(&table).await?;
        }
    }

    // Very similar to build_dependency_versions
    if let Some(dependency_tables) = &function_create.dependencies {
        let mut builder = DependencyDBBuilder::try_from(&function_db)?;
        let builder = builder.try_update_from(&request_context)?;

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

            let dependency = builder
                .clone()
                .table_collection_id(table_db.collection_id)
                .table_function_id(table_db.function_id)
                .table_id(table_db.table_id)
                .table_versions(dependency_table.versions.clone())
                .dep_pos(DependencyPos::try_from(pos as i32)?)
                .status(DependencyStatus::Active)
                .system(false)
                .build()?;

            executor.reuse().live().insert(&dependency).await?;
        }
    }

    // Very similar to build_trigger_versions
    if let Some(trigger_tables) = &function_create.triggers {
        let mut builder = TriggerDBBuilder::try_from(&function_db)?;
        let builder = builder.try_update_from(&request_context)?;

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

            let trigger = builder
                .clone()
                .trigger_by_collection_id(table_db.collection_id)
                .trigger_by_function_id(table_db.function_id)
                .trigger_by_table_id(table_db.table_id)
                .status(TriggerStatus::Active)
                .system(false)
                .build()?;

            executor.reuse().live().insert(&trigger).await?;
        }
    }

    // Very similar to build_worker
    if let Some(workers) = &function_create.workers {
        // `WorkerDB.project_id` is no longer mapped from `FunctionDB`; stamp it
        // from the collection.
        let mut builder = WorkerDBBuilder::try_from(&function_db)?;
        builder.project_id(collection.project_id);

        for (worker_type, worker_config) in workers {
            let worker = builder
                .clone()
                .worker_type(worker_type.clone())
                .worker_config(Some(worker_config.clone()))
                .build()?;

            executor.reuse().live().insert(&worker).await?;
        }
    }

    Ok(function_db)
}

#[cfg(test)]
mod tests {
    use crate::HistoricEntry;
    use crate::test_utils::SeedEntry;
    use ta_objects::Buildable;
    use td_database::pool::TdPool;
    use td_objects::dxo::function::{FunctionDB, FunctionRegister};
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

        let create = FunctionRegister::builder()
            .try_name("joaquin")
            .unwrap()
            .try_description("function_foo description")
            .unwrap()
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")
            .unwrap()
            .decorator(Decorator::Publisher)
            .dependencies(dependencies)
            .triggers(triggers)
            .tables(tables)
            .try_runtime_values("mock runtime values")
            .unwrap()
            .reuse_frozen_tables(false)
            .build()
            .unwrap();

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
