//
// Copyright 2025 Tabs Data Inc.
//

use crate::QueryExecutor;
use crate::{CurrentEntry, LiveEntry};
use ta_objects::TryUpdater;
use ta_objects::and;
use td_database::domain::{Apiserver, Domained};
use td_error::TdError;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::dependency::DependencyDBBuilder;
use td_objects::dxo::function::FunctionDB;
use td_objects::dxo::table::{TableDB, TableDBBuilder, TableDBWithNames};
use td_objects::system::{
    SYSTEM_INPUT_TABLE_DEPENDENCY_PREFIXES, SYSTEM_OUTPUT_TABLE_NAMES_PREFIXES,
};
use td_objects::types::basic::{
    AccessTokenId, DependencyPos, DependencyStatus, TableFunctionParamPos, TableId, TableName,
    TableStatus, UserId,
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
        let mut builder = TableDBBuilder::try_from(function)?;
        let builder = builder.try_update_from(&request_context)?;

        let table = builder
            .clone()
            .table_id(TableId::default())
            .name(TableName::try_from(format!(
                "{prefix}__{}",
                function.function_id
            ))?)
            .function_param_pos(Some(TableFunctionParamPos::try_from(-(pos as i32 + 1))?))
            .status(TableStatus::Active)
            .build()?;

        executor.reuse().live().insert(&table).await?;
        system_tables.push(table);
    }

    // System dependencies
    for (pos, prefix) in SYSTEM_INPUT_TABLE_DEPENDENCY_PREFIXES.iter().enumerate() {
        let mut builder = DependencyDBBuilder::try_from(function)?;
        let builder = builder.try_update_from(&request_context)?;

        let (table_collection, table_name) = (
            function.collection_id,
            TableName::try_from(format!("{prefix}__{}", function.function_id))?,
        );

        let table_db = executor
            .reuse()
            .current()
            .fetch_one::<TableDBWithNames>(and!(table_collection, table_name))
            .await?;

        let dependency = builder
            .clone()
            .table_collection_id(table_db.collection_id)
            .table_function_id(table_db.function_id)
            .table_id(table_db.table_id)
            .table_versions(TableVersions::new(Versions::None))
            .dep_pos(DependencyPos::try_from(-(pos as i32 + 1))?)
            .status(DependencyStatus::Active)
            .system(true)
            .build()?;

        executor.reuse().live().insert(&dependency).await?;
    }

    Ok(system_tables)
}

#[cfg(test)]
mod tests {
    use crate::LiveEntry;
    use crate::test_utils::SeedEntry;
    use ta_objects::Buildable;
    use td_database::pool::TdPool;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::dxo::table::TableDB;
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
