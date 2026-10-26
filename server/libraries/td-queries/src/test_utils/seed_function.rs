//
// Copyright 2025 Tabsdata Inc.
//

use crate::Queries;
use crate::Writes;
use ta_objects::and;
use td_database::access::InProject;
use td_database::domain::Apiserver;
use td_error::TdError;
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::dependency::DependencyInsertDB;
use td_objects::dxo::function::{FunctionDB, FunctionInsertDB, FunctionRegister};
use td_objects::dxo::table::TableDBWithNames;
use td_objects::dxo::table::TableInsertDB;
use td_objects::dxo::tag::TopologyTagInsertDB;
use td_objects::dxo::topology::TopologyInstant;
use td_objects::dxo::trigger::TriggerInsertDB;
use td_objects::dxo::worker::WorkerInsertDB;
use td_objects::types::basic::{
    AccessTokenId, BasePath, DependencyId, DependencyPos, DependencyVersionId, FunctionId,
    FunctionStatus, FunctionType, FunctionVersionId, Partitioned, Private, Snippet, StageId,
    StorageVersion, System, TableFunctionParamPos, TableId, TableName, TableStatus, TableType,
    TableVersionId, TagEntityId, TagEntityType, TagId, TagValue, TopologyTagKey, TriggerId,
    TriggerStatus, TriggerVersionId, UserId, WorkerId, WorkerStatus,
};

/// Runs every statement on the caller's executor; if it is project-scoped
/// (`db.scoped(p)`), the field-less rows (function, tables, dependencies,
/// triggers) default their `project_id` from that scope.
pub(crate) async fn seed_function(
    mut executor: impl Writes<Domain = Apiserver, Scope = InProject>,
    topology_at: TopologyInstant,
    collection: &CollectionDB,
    function_create: &FunctionRegister,
) -> Result<FunctionDB, TdError> {
    let request_context = RequestContext::with(AccessTokenId::default(), UserId::admin());

    let function_db = FunctionInsertDB {
        id: FunctionVersionId::new(),
        collection_id: collection.collection_id,
        name: function_create.name.clone(),
        description: function_create.description.clone(),
        decorator: function_create.decorator.clone(),
        function_type: FunctionType::default(),
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

    executor
        .reuse()
        .scd2()
        .open(&function_db, topology_at)
        .await?;

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

            executor.reuse().scd2().open(&table, topology_at).await?;
        }
    }

    if let Some(dependency_tables) = &function_create.dependencies {
        for (pos, dependency_table) in dependency_tables.iter().enumerate() {
            let table_collection = match &dependency_table.collection {
                Some(name) => {
                    let named: CollectionDB = executor.reuse().scd2().fetch_one(name).await?;
                    named.collection_id
                }
                None => collection.collection_id,
            };
            let table_name = TableName::try_from(dependency_table.table.clone())?;

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
                table_collection_id: table_db.collection_id.into(),
                table_function_id: table_db.function_id.into(),
                table_id: table_db.table_id,
                table_versions: dependency_table.versions.clone().into(),
                dep_pos: DependencyPos::try_from(pos as i32)?,
                defined_by_id: request_context.user_id,
                system: System::FALSE,
            };

            executor
                .reuse()
                .scd2()
                .open(&dependency, topology_at)
                .await?;
        }
    }

    if let Some(trigger_tables) = &function_create.triggers {
        for trigger_table in trigger_tables {
            let table_collection = match &trigger_table.collection {
                Some(name) => {
                    let named: CollectionDB = executor.reuse().scd2().fetch_one(name).await?;
                    named.collection_id
                }
                None => collection.collection_id,
            };
            let table_name = TableName::try_from(trigger_table.table.clone())?;

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
                trigger_by_collection_id: table_db.collection_id.into(),
                trigger_by_function_id: table_db.function_id.into(),
                trigger_by_table_id: table_db.table_id,
                status: TriggerStatus::Active,
                defined_by_id: request_context.user_id,
                system: System::FALSE,
            };

            executor.reuse().scd2().open(&trigger, topology_at).await?;
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

/// A child function row. A request cannot express a parent, so a test needing
/// a platform-owned row states it here rather than going through the
/// reconcile that would normally create one.
pub(crate) async fn seed_child_function(
    mut executor: impl Writes<Domain = Apiserver, Scope = InProject>,
    topology_at: TopologyInstant,
    collection: &CollectionDB,
    function_create: &FunctionRegister,
    parent: &FunctionId,
) -> Result<FunctionDB, TdError> {
    let function =
        seed_function(executor.reuse(), topology_at, collection, function_create).await?;
    let tag = TopologyTagInsertDB::new(
        TagId::default(),
        TagEntityType::Function,
        TagEntityId::try_from(function.base.function_id)?,
        TopologyTagKey::Parent,
        TagValue::of_id(*parent),
        UserId::admin(),
    )?;
    executor.scd2().open(&tag, topology_at).await?;
    Ok(function)
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
            .scoped(p.project_id)
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
            child_functions: None,
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
            parent_tables: None,
            runtime_values: "mock runtime values".try_into().unwrap(),
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let function = db
            .scoped(p.project_id)
            .seed()
            .function(&collection, &create)
            .await
            .unwrap();
        let found = db
            .timeline()
            .fetch_one::<FunctionDB>(&function.id)
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
        assert_eq!(function.interval.valid_from, found.interval.valid_from);
        assert_eq!(function.defined_by_id, found.defined_by_id);
        assert_eq!(function.status, found.status);
        assert_eq!(function.enabled, found.enabled);
    }
}
