//
// Copyright 2025 Tabsdata Inc.
//

use crate::Queries;
use crate::QueryExecutor;
use ta_objects::and;
use td_database::access::InProject;
use td_database::domain::{Apiserver, Domained};
use td_error::TdError;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::dependency::DependencyInsertDB;
use td_objects::dxo::function::FunctionDB;
use td_objects::dxo::table::{TableDB, TableDBWithNames, TableInsertDB};
use td_objects::system::{
    SYSTEM_INPUT_TABLE_DEPENDENCY_PREFIXES, SYSTEM_OUTPUT_TABLE_NAMES_PREFIXES,
};
use td_objects::types::basic::{
    AccessTokenId, DependencyId, DependencyPos, DependencyVersionId, Partitioned, Private, System,
    TableFunctionParamPos, TableId, TableName, TableStatus, TableType, TableVersionId, UserId,
};
use td_objects::types::composed::TableVersions;
use td_objects::types::table_ref::Versions;

// Very similar to seed_function or build_table_versions, but to seed system
// tables. Runs on the caller's executor: if it is project-scoped
// (`db.scoped(p)`), the field-less rows default their `project_id` from that
// scope.
pub(crate) async fn seed_system_tables(
    mut executor: impl QueryExecutor + Domained<Domain = Apiserver, Scope = InProject>,
    function: &FunctionDB,
) -> Result<Vec<TableDB>, TdError> {
    let request_context = RequestContext::with(AccessTokenId::default(), UserId::admin());

    // System tables
    let mut system_tables = vec![];
    for (pos, prefix) in SYSTEM_OUTPUT_TABLE_NAMES_PREFIXES.iter().enumerate() {
        let table = TableInsertDB {
            id: TableVersionId::default(),
            collection_id: function.base.collection_id,
            table_id: TableId::default(),
            name: TableName::try_from(format!("{prefix}__{}", function.base.function_id))?,
            function_id: function.base.function_id,
            function_version_id: function.base.id.clone(),
            function_param_pos: Some(TableFunctionParamPos::try_from(-(pos as i32 + 1))?),
            private: Private::FALSE,
            partitioned: Partitioned::FALSE,
            defined_on: request_context.time,
            defined_by_id: request_context.user_id,
            status: TableStatus::Active,
            table_type: TableType::default(),
        };

        executor.reuse().scd2().insert(&table).await?;
        system_tables.push(table);
    }

    // System dependencies
    for (pos, prefix) in SYSTEM_INPUT_TABLE_DEPENDENCY_PREFIXES.iter().enumerate() {
        let (table_collection, table_name) = (
            function.base.collection_id,
            TableName::try_from(format!("{prefix}__{}", function.base.function_id))?,
        );

        let table_db = executor
            .reuse()
            .scd2()
            .fetch_first::<TableDBWithNames>(and!(table_collection, table_name))
            .await?
            .expect("seeded system table must exist");

        let dependency = DependencyInsertDB {
            id: DependencyVersionId::default(),
            collection_id: function.base.collection_id,
            dependency_id: DependencyId::default(),
            function_id: function.base.function_id,
            table_collection_id: table_db.base.base.collection_id.into(),
            table_function_id: table_db.base.base.function_id.into(),
            table_id: table_db.base.base.table_id,
            table_versions: TableVersions::new(Versions::None),
            dep_pos: DependencyPos::try_from(-(pos as i32 + 1))?,
            defined_on: request_context.time,
            defined_by_id: request_context.user_id,
            system: System::TRUE,
        };

        executor.reuse().scd2().insert(&dependency).await?;
    }

    let ids: Vec<_> = system_tables.iter().map(|t| t.id).collect();
    Ok(executor
        .reuse()
        .timeline()
        .chunked()
        .fetch_all(&ids, ())
        .await?)
}

#[cfg(test)]
mod tests {
    use crate::Queries;
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
            let found = db
                .timeline()
                .fetch_one::<TableDB>(&table.base.id)
                .await
                .unwrap();
            assert_eq!(table.base.id, found.base.id);
            assert_eq!(table.base.collection_id, found.base.collection_id);
            assert_eq!(table.base.table_id, found.base.table_id);
            assert_eq!(table.base.name, found.base.name);
            assert_eq!(table.base.function_id, found.base.function_id);
            assert_eq!(
                table.base.function_version_id,
                found.base.function_version_id
            );
            assert_eq!(table.base.function_param_pos, found.base.function_param_pos);
            assert_eq!(table.base.private, found.base.private);
            assert_eq!(table.base.partitioned, found.base.partitioned);
            assert_eq!(table.base.defined_on, found.base.defined_on);
            assert_eq!(table.base.defined_by_id, found.base.defined_by_id);
            assert_eq!(table.base.status, found.base.status);
        }
    }
}
