//
// Copyright 2025 Tabsdata Inc.
//

use crate::Queries;
use crate::QueryExecutor;
use ta_objects::and;
use td_database::access::{InProject, LockInstant};
use td_database::domain::{Apiserver, Domained};
use td_error::TdError;
use td_objects::dxo::collection::ActiveCollectionDB;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::dependency::DependencyInsertDB;
use td_objects::dxo::function::{FunctionDB, FunctionInsertDB, FunctionRegister};
use td_objects::dxo::table::TableDBWithNames;
use td_objects::dxo::table::TableInsertDB;
use td_objects::dxo::trigger::TriggerInsertDB;
use td_objects::dxo::worker::WorkerInsertDB;
use td_objects::types::basic::{
    AccessTokenId, BasePath, DependencyId, DependencyPos, DependencyVersionId, FunctionId,
    FunctionStatus, FunctionVersionId, Partitioned, Private, Snippet, StageId, StorageVersion,
    System, TableFunctionParamPos, TableId, TableName, TableStatus, TableType, TableVersionId,
    TriggerId, TriggerStatus, TriggerVersionId, UserId, WorkerId, WorkerMessageStatus,
    WorkerStatus,
};

/// Runs every statement on the caller's executor; if it is project-scoped
/// (`db.scoped(p)`), the field-less rows (function, tables, dependencies,
/// triggers) default their `project_id` from that scope.
pub(crate) async fn seed_function(
    mut executor: impl QueryExecutor + Domained<Domain = Apiserver, Scope = InProject>,
    collection: &ActiveCollectionDB,
    function_create: &FunctionRegister,
) -> Result<FunctionDB, TdError> {
    let request_context = RequestContext::with(AccessTokenId::default(), UserId::admin());
    let at = LockInstant::asserted(*request_context.time);

    let function_db = FunctionInsertDB {
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
        defined_by_id: request_context.user_id,
        status: FunctionStatus::Active,
        enabled: function_create.enabled.unwrap_or_default(),
    };

    executor.reuse().scd2().open(&function_db, at).await?;

    if let Some(tables) = &function_create.tables {
        for (pos, table_name) in tables.iter().enumerate() {
            let name = TableName::try_from(table_name)?;
            let table = TableInsertDB {
                id: TableVersionId::default(),
                collection_id: function_db.collection_id,
                table_id: TableId::default(),
                private: Private::from(name.is_private()),
                name,
                function_id: function_db.function_id,
                function_version_id: function_db.id.clone(),
                function_param_pos: Some(TableFunctionParamPos::try_from(pos as i32)?),
                partitioned: Partitioned::FALSE,
                defined_by_id: request_context.user_id,
                status: TableStatus::Active,
                table_type: TableType::default(),
            };

            executor.reuse().scd2().open(&table, at).await?;
        }
    }

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
                .scd2()
                .fetch_first::<_>(and!(table_collection, table_name))
                .await?
                .expect("seeded dependency table must exist");

            let dependency = DependencyInsertDB {
                id: DependencyVersionId::default(),
                collection_id: function_db.collection_id,
                dependency_id: DependencyId::default(),
                function_id: function_db.function_id,
                table_collection_id: table_db.base.base.collection_id.into(),
                table_function_id: table_db.base.base.function_id.into(),
                table_id: table_db.base.base.table_id,
                table_versions: dependency_table.versions.clone().into(),
                dep_pos: DependencyPos::try_from(pos as i32)?,
                defined_by_id: request_context.user_id,
                system: System::FALSE,
            };

            executor.reuse().scd2().open(&dependency, at).await?;
        }
    }

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
                .scd2()
                .fetch_first::<_>(and!(table_collection, table_name))
                .await?
                .expect("seeded trigger table must exist");

            let trigger = TriggerInsertDB {
                id: TriggerVersionId::default(),
                collection_id: function_db.collection_id,
                trigger_id: TriggerId::default(),
                function_id: function_db.function_id,
                trigger_by_collection_id: table_db.base.base.collection_id.into(),
                trigger_by_function_id: table_db.base.base.function_id.into(),
                trigger_by_table_id: table_db.base.base.table_id,
                status: TriggerStatus::Active,
                defined_by_id: request_context.user_id,
                system: System::FALSE,
            };

            executor.reuse().scd2().open(&trigger, at).await?;
        }
    }

    if let Some(workers) = &function_create.workers {
        for (worker_type, worker_config) in workers {
            let worker = WorkerInsertDB {
                id: WorkerId::new(),
                // `WorkerDB.project_id` is not derived from the function; stamp
                // it from the collection.
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

            executor.reuse().insert(&worker).await?;
        }
    }

    Ok(executor
        .reuse()
        .timeline()
        .fetch_one(&function_db.id)
        .await?)
}

#[cfg(test)]
mod tests {
    use crate::Queries;
    use crate::test_utils::SeedEntry;
    use td_database::pool::TdPool;
    use td_objects::dxo::function::{FunctionDB, FunctionRegister};
    use td_objects::types::basic::ReuseFrozen;
    use td_objects::types::basic::{BundleId, CollectionName, Decorator, UserId};
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
            .timeline()
            .fetch_one::<FunctionDB>(&function.base.id)
            .await
            .unwrap();

        assert_eq!(function.base.id, found.base.id);
        assert_eq!(function.base.collection_id, found.base.collection_id);
        assert_eq!(function.base.name, found.base.name);
        assert_eq!(function.base.description, found.base.description);
        assert_eq!(function.base.runtime_values, found.base.runtime_values);
        assert_eq!(function.base.function_id, found.base.function_id);
        assert_eq!(function.base.storage_version, found.base.storage_version);
        assert_eq!(function.base.bundle_id, found.base.bundle_id);
        assert_eq!(function.base.snippet, found.base.snippet);
        assert_eq!(function.interval.valid_from, found.interval.valid_from);
        assert_eq!(function.base.defined_by_id, found.base.defined_by_id);
        assert_eq!(function.base.status, found.base.status);
        assert_eq!(function.base.enabled, found.base.enabled);
    }
}
