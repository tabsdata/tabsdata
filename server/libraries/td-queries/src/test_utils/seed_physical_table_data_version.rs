//
// Copyright 2025 Tabs Data Inc.
//

use crate::{LiveEntry, QueryExecutor};
use ta_objects::Buildable;
use td_error::TdError;
use td_objects::and;
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::execution::ExecutionDB;
use td_objects::dxo::logical::{
    LogicalFunctionRunDB, LogicalPointerDao, LogicalTableDataVersionDB,
    LogicalTableDataVersionPointerDB,
};
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionDB;
use td_objects::dxo::table::TableDB;
use td_objects::dxo::transaction::TransactionDB;
use td_objects::types::basic::{
    AtTime, HasData, LogicalPointerReason, LogicalPointerStatus, TableFunctionParamPos,
};

pub(crate) async fn seed_physical_table_data_version_with_data(
    db: impl QueryExecutor + Copy,
    collection: &CollectionDB,
    execution: &ExecutionDB,
    transaction: &TransactionDB,
    physical_function_run: &PhysicalFunctionRunDB,
    table: &TableDB,
) -> Result<PhysicalTableDataVersionDB, TdError> {
    _seed_physical_table_data_version(
        db,
        collection,
        execution,
        transaction,
        physical_function_run,
        table,
        Some(&HasData::from(true)),
    )
    .await
}

pub(crate) async fn seed_physical_table_data_version(
    db: impl QueryExecutor + Copy,
    collection: &CollectionDB,
    execution: &ExecutionDB,
    transaction: &TransactionDB,
    physical_function_run: &PhysicalFunctionRunDB,
    table: &TableDB,
) -> Result<PhysicalTableDataVersionDB, TdError> {
    _seed_physical_table_data_version(
        db,
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
    db: impl QueryExecutor + Copy,
    collection: &CollectionDB,
    execution: &ExecutionDB,
    transaction: &TransactionDB,
    physical_function_run: &PhysicalFunctionRunDB,
    table: &TableDB,
    has_data: Option<&HasData>,
) -> Result<PhysicalTableDataVersionDB, TdError> {
    let table_data_version_db = PhysicalTableDataVersionDB::builder()
        .project_id(physical_function_run.project_id.clone())
        .collection_id(collection.id)
        .table_id(table.table_id)
        .name(table.name.clone())
        .table_version_id(table.id)
        .function_version_id(&table.function_version_id)
        .has_data(has_data.cloned())
        .execution_id(&execution.id)
        .transaction_id(&transaction.id)
        .physical_function_run_id(&physical_function_run.id)
        .function_param_pos(TableFunctionParamPos::try_from(0)?)
        .triggered_on(physical_function_run.triggered_on)
        .build()?;

    // Mint the LTDV identity row + a Bound pointer event so the seed
    // mirrors what plan-build + dispatch produce. The producer LFR is
    // derived via (execution_id, function_id) from the FR.
    let producer_lfr: LogicalFunctionRunDB = db
        .live()
        .fetch_one(and!(
            &physical_function_run.execution_id,
            &physical_function_run.function_id
        ))
        .await?;
    let logical = LogicalTableDataVersionDB::builder()
        .project_id(producer_lfr.project_id.clone())
        .collection_id(collection.id)
        .execution_id(&execution.id)
        .transaction_id(&transaction.id)
        .table_id(table.table_id)
        .producer_logical_function_run_id(Some(producer_lfr.id.clone()))
        .function_param_pos(Some(TableFunctionParamPos::try_from(0)?))
        .triggered_on(physical_function_run.triggered_on)
        .triggered_by_id(transaction.triggered_by_id)
        .build()?;

    db.live().insert(&table_data_version_db).await?;
    db.live()
        .insert::<LogicalTableDataVersionDB>(&logical)
        .await?;
    let pointer = LogicalTableDataVersionPointerDB::build_event(
        logical.project_id.clone(),
        &logical.id,
        LogicalPointerStatus::Bound,
        Some(table_data_version_db.id.clone()),
        AtTime::try_from(physical_function_run.triggered_on)?,
        transaction.triggered_by_id,
        LogicalPointerReason::InitialMaterialization,
    )?;
    db.live()
        .insert::<LogicalTableDataVersionPointerDB>(&pointer)
        .await?;
    Ok(table_data_version_db)
}

#[cfg(test)]
mod tests {
    use crate::CurrentEntry;
    use crate::test_utils::SeedEntry;
    use ta_objects::Buildable;
    use ta_objects::and;
    use td_database::pool::TdPool;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::dxo::table::TableDB;
    use td_objects::types::basic::{
        BundleId, CollectionName, Decorator, LogicalFunctionRunStatus, TableName, TableNameDto,
        TransactionKey, UserId,
    };
    use td_security::ENCODED_ID_SYSTEM;

    #[td_test::test(db)]
    async fn test_seed_physical_table_data_version(db: TdPool) {
        let collection = db
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

        let function_version = db.seed().function(&collection, &create).await.unwrap();

        let execution = db.seed().execution(&function_version).await.unwrap();

        let transaction_key = TransactionKey::try_from("ANY").unwrap();
        let transaction = db
            .seed()
            .transaction(&execution, &transaction_key)
            .await
            .unwrap();

        let physical_function_run = db
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
            .current()
            .fetch_one::<TableDB>(and!(
                &collection.id,
                TableName::try_from(table_name).unwrap()
            ))
            .await
            .unwrap();

        let physical_table_data_version = db
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

        assert_eq!(physical_table_data_version.collection_id, collection.id);
        assert_eq!(
            physical_table_data_version.table_version_id,
            table_version.id
        );
        assert_eq!(
            physical_table_data_version.function_version_id,
            function_version.id
        );
        assert_eq!(physical_table_data_version.has_data, None);
        assert_eq!(physical_table_data_version.execution_id, execution.id);
        assert_eq!(physical_table_data_version.transaction_id, transaction.id);
        assert_eq!(
            physical_table_data_version.physical_function_run_id,
            physical_function_run.id
        );
    }
}
