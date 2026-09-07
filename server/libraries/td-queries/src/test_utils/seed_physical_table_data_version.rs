//
// Copyright 2025 Tabsdata Inc.
//

use crate::Queries;
use crate::QueryExecutor;
use td_database::access::InProject;
use td_database::domain::{Apiserver, Domained};
use td_error::TdError;
use td_objects::and;
use td_objects::dxo::collection::ActiveCollectionDB;
use td_objects::dxo::execution::ExecutionDB;
use td_objects::dxo::logical::{
    LogicalFunctionRunDB, LogicalTableDataVersionDB, LogicalTableDataVersionInsertDB,
};
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionDB;
use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionInsertDB;
use td_objects::dxo::table::TableDB;
use td_objects::dxo::transaction::TransactionDB;
use td_objects::types::basic::{
    HasData, LogicalTableDataVersionId, PhysicalTableDataVersionId, TableFunctionParamPos,
};

pub(crate) async fn seed_physical_table_data_version_with_data(
    mut executor: impl QueryExecutor + Domained<Domain = Apiserver, Scope = InProject>,
    collection: &ActiveCollectionDB,
    execution: &ExecutionDB,
    transaction: &TransactionDB,
    physical_function_run: &PhysicalFunctionRunDB,
    table: &TableDB,
) -> Result<PhysicalTableDataVersionDB, TdError> {
    _seed_physical_table_data_version(
        executor.reuse(),
        collection,
        execution,
        transaction,
        physical_function_run,
        table,
        Some(&HasData::TRUE),
    )
    .await
}

pub(crate) async fn seed_physical_table_data_version(
    mut executor: impl QueryExecutor + Domained<Domain = Apiserver, Scope = InProject>,
    collection: &ActiveCollectionDB,
    execution: &ExecutionDB,
    transaction: &TransactionDB,
    physical_function_run: &PhysicalFunctionRunDB,
    table: &TableDB,
) -> Result<PhysicalTableDataVersionDB, TdError> {
    _seed_physical_table_data_version(
        executor.reuse(),
        collection,
        execution,
        transaction,
        physical_function_run,
        table,
        None,
    )
    .await
}

pub async fn _seed_physical_table_data_version(
    mut executor: impl QueryExecutor + Domained<Domain = Apiserver, Scope = InProject>,
    collection: &ActiveCollectionDB,
    execution: &ExecutionDB,
    transaction: &TransactionDB,
    physical_function_run: &PhysicalFunctionRunDB,
    table: &TableDB,
    has_data: Option<&HasData>,
) -> Result<PhysicalTableDataVersionDB, TdError> {
    // The logical version this materialization is of, so the seed mirrors
    // what plan-build + dispatch produce: reuse the LTDV minted by the
    // function-run seed when present (real flows materialize the
    // plan-minted row, they never mint a second one), minting only if the
    // table has none for this execution.
    let producer_lfr: LogicalFunctionRunDB = executor
        .reuse()
        .fetch_one(and!(
            &physical_function_run.base.execution_id,
            &physical_function_run.base.function_id
        ))
        .await?;
    let existing: Option<LogicalTableDataVersionDB> = executor
        .reuse()
        .timeline()
        .fetch_first(and!(&execution.base.id, &table.base.table_id))
        .await?;
    let logical_id = match existing {
        Some(ltdv) => ltdv.base.id,
        None => {
            let ltdv = LogicalTableDataVersionInsertDB {
                id: LogicalTableDataVersionId::new(),
                collection_id: collection.id,
                execution_id: execution.base.id.clone(),
                transaction_id: transaction.base.id.clone(),
                table_id: table.base.table_id,
                producer_logical_function_run_id: Some(producer_lfr.base.id.clone()),
                function_param_pos: Some(TableFunctionParamPos::try_from(0)?),
                private: table.base.private,
                triggered_on: physical_function_run.base.triggered_on,
                triggered_by_id: transaction.base.triggered_by_id,
                group_id: producer_lfr.base.group_id,
            };
            executor.reuse().insert(&ltdv).await?;
            ltdv.id
        }
    };

    let ptdv_id = PhysicalTableDataVersionId::new();
    let table_data_version_db = PhysicalTableDataVersionInsertDB {
        id: ptdv_id.clone(),
        collection_id: collection.id,
        table_id: table.base.table_id,
        name: table.base.name.clone(),
        table_version_id: table.base.id,
        function_version_id: table.base.function_version_id.clone(),
        function_version_patch_id: None,
        execution_id: execution.base.id.clone(),
        transaction_id: transaction.base.id.clone(),
        physical_function_run_id: physical_function_run.base.id.clone(),
        function_param_pos: TableFunctionParamPos::try_from(0)?,
        triggered_on: physical_function_run.base.triggered_on,
        // Mirror the mint: an FR and its output TDVs stamp the same prefix.
        base_path: physical_function_run.base.base_path.clone(),
        has_data: has_data.cloned(),
        schema_changed: None,
        column_count: None,
        row_count: None,
        data_bytes: None,
        schema_hash: None,
        with_data_table_data_version_id: None,
        logical_table_data_version_id: logical_id.clone(),
    };
    executor
        .reuse()
        .scd2()
        .insert(&table_data_version_db)
        .await?;
    Ok(executor.reuse().timeline().fetch_one(&ptdv_id).await?)
}

#[cfg(test)]
mod tests {
    use crate::Queries;
    use crate::test_utils::SeedEntry;
    use ta_objects::and;
    use td_database::pool::TdPool;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::dxo::table::TableDB;
    use td_objects::types::basic::ReuseFrozen;
    use td_objects::types::basic::{
        BundleId, CollectionName, Decorator, LogicalFunctionRunStatus, TableName, TableNameDto,
        TransactionKey, UserId,
    };
    use td_security::ENCODED_ID_SYSTEM;

    #[td_test::test(db)]
    async fn test_seed_physical_table_data_version(db: TdPool) {
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

        let table_name = TableNameDto::try_from("table_version").unwrap();

        let dependencies = None;
        let triggers = None;
        let tables = vec![table_name.clone()];

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
            tables: Some(tables),
            table_types: None,
            runtime_values: "mock runtime values".try_into().unwrap(),
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let function_version = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await
            .unwrap();

        let execution = db
            .scoped(p.id)
            .seed()
            .execution(&function_version)
            .await
            .unwrap();

        let transaction_key = TransactionKey::try_from("ANY").unwrap();
        let transaction = db
            .scoped(p.id)
            .seed()
            .transaction(&execution, &transaction_key)
            .await
            .unwrap();

        let physical_function_run = db
            .scoped(p.id)
            .seed()
            .physical_function_run(
                &collection,
                &function_version,
                &execution,
                &transaction,
                &LogicalFunctionRunStatus::Scheduled,
            )
            .await
            .unwrap();

        let table_version = db
            .scd2()
            .fetch_first::<TableDB>(and!(
                &collection.id,
                TableName::try_from(table_name).unwrap()
            ))
            .await
            .unwrap()
            .expect("seeded table must exist");

        let physical_table_data_version = db
            .scoped(p.id)
            .seed()
            .physical_table_data_version(
                &collection,
                &execution,
                &transaction,
                &physical_function_run,
                &table_version,
            )
            .await
            .unwrap();

        assert_eq!(
            physical_table_data_version.base.collection_id,
            collection.id
        );
        assert_eq!(
            physical_table_data_version.base.table_version_id,
            table_version.base.id
        );
        assert_eq!(
            physical_table_data_version.base.function_version_id,
            function_version.base.id
        );
        assert_eq!(physical_table_data_version.base.has_data, None);
        assert_eq!(
            physical_table_data_version.base.execution_id,
            execution.base.id
        );
        assert_eq!(
            physical_table_data_version.base.transaction_id,
            transaction.base.id
        );
        assert_eq!(
            physical_table_data_version.base.physical_function_run_id,
            physical_function_run.base.id
        );
    }
}
