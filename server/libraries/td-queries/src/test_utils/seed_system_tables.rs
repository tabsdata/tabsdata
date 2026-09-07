//
// Copyright 2025 Tabsdata Inc.
//

use crate::QueryExecutor;
use crate::{CurrentEntry, LiveEntry};
use ta_objects::and;
use td_database::domain::{Apiserver, Domained};
use td_error::TdError;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::dependency::DependencyDB;
use td_objects::dxo::function::FunctionDB;
use td_objects::dxo::table::{TableDB, TableDBWithNames};
use td_objects::system::{
    SYSTEM_INPUT_TABLE_DEPENDENCY_PREFIXES, SYSTEM_OUTPUT_TABLE_NAMES_PREFIXES,
};
use td_objects::types::basic::{
    AccessTokenId, DependencyId, DependencyPos, DependencyStatus, DependencyVersionId, Partitioned,
    Private, System, TableFunctionParamPos, TableId, TableName, TableStatus, TableType,
    TableVersionId, UserId,
};
use td_objects::types::composed::TableVersions;
use td_objects::types::table_ref::Versions;

// Very similar to seed_function or build_table_versions, but to seed system
// tables. Runs on the caller's executor: if it is project-scoped
// (`db.scoped(p)`), the field-less rows default their `project_id` from that
// scope.
pub(crate) async fn seed_system_tables(
    mut executor: impl QueryExecutor + Domained<Domain = Apiserver>,
    function: &FunctionDB,
) -> Result<Vec<TableDB>, TdError> {
    let request_context = RequestContext::with(AccessTokenId::default(), UserId::admin());

    // System tables
    let mut system_tables = vec![];
    for (pos, prefix) in SYSTEM_OUTPUT_TABLE_NAMES_PREFIXES.iter().enumerate() {
        let table = TableDB {
            id: TableVersionId::default(),
            collection_id: function.collection_id,
            table_id: TableId::default(),
            name: TableName::try_from(format!("{prefix}__{}", function.function_id))?,
            function_id: function.function_id,
            function_version_id: function.id.clone(),
            function_param_pos: Some(TableFunctionParamPos::try_from(-(pos as i32 + 1))?),
            private: Private::FALSE,
            partitioned: Partitioned::FALSE,
            defined_on: request_context.time,
            defined_by_id: request_context.user_id,
            status: TableStatus::Active,
            table_type: TableType::default(),
        };

        executor.reuse().live().insert(&table).await?;
        system_tables.push(table);
    }

    // System dependencies
    for (pos, prefix) in SYSTEM_INPUT_TABLE_DEPENDENCY_PREFIXES.iter().enumerate() {
        let (table_collection, table_name) = (
            function.collection_id,
            TableName::try_from(format!("{prefix}__{}", function.function_id))?,
        );

        let table_db = executor
            .reuse()
            .current()
            .fetch_one::<TableDBWithNames>(and!(table_collection, table_name))
            .await?;

        let dependency = DependencyDB {
            id: DependencyVersionId::default(),
            collection_id: function.collection_id,
            dependency_id: DependencyId::default(),
            function_id: function.function_id,
            table_collection_id: table_db.base.collection_id,
            table_function_id: table_db.base.function_id,
            table_id: table_db.base.table_id,
            table_versions: TableVersions::new(Versions::None),
            dep_pos: DependencyPos::try_from(-(pos as i32 + 1))?,
            status: DependencyStatus::Active,
            defined_on: request_context.time,
            defined_by_id: request_context.user_id,
            system: System::TRUE,
        };

        executor.reuse().live().insert(&dependency).await?;
    }

    Ok(system_tables)
}

#[cfg(test)]
mod tests {
    use crate::LiveEntry;
    use crate::test_utils::SeedEntry;
    use td_database::pool::TdPool;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::dxo::table::TableDB;
    use td_objects::types::basic::ReuseFrozen;
    use td_objects::types::basic::{BundleId, CollectionName, Decorator, UserId};
    use td_security::ENCODED_ID_SYSTEM;

    #[td_test::test(db)]
    async fn test_seed_system_tables(db: TdPool) {
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
        let system_tables = db
            .scoped(p.id)
            .seed()
            .system_tables(&function)
            .await
            .unwrap();

        assert_eq!(system_tables.len(), 1);
        for table in system_tables {
            let found = db.live().fetch_one::<TableDB>(&table.id).await.unwrap();
            assert_eq!(table.id, found.id);
            assert_eq!(table.collection_id, found.collection_id);
            assert_eq!(table.table_id, found.table_id);
            assert_eq!(table.name, found.name);
            assert_eq!(table.function_id, found.function_id);
            assert_eq!(table.function_version_id, found.function_version_id);
            assert_eq!(table.function_param_pos, found.function_param_pos);
            assert_eq!(table.private, found.private);
            assert_eq!(table.partitioned, found.partitioned);
            assert_eq!(table.defined_on, found.defined_on);
            assert_eq!(table.defined_by_id, found.defined_by_id);
            assert_eq!(table.status, found.status);
        }
    }
}
