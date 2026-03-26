//
// Copyright 2025 Tabs Data Inc.
//

use crate::BasicQueries;
use ta_objects::Buildable;
use td_database::sea_query::executor::SeaExecutor;
use td_error::TdError;
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::execution::ExecutionDB;
use td_objects::dxo::function_run::FunctionRunDB;
use td_objects::dxo::table::TableDB;
use td_objects::dxo::table_data_version::TableDataVersionDB;
use td_objects::dxo::transaction::TransactionDB;
use td_objects::types::basic::{HasData, TableFunctionParamPos};

pub(crate) async fn seed_table_data_version_with_data(
    db: impl SeaExecutor + Send,
    collection: &CollectionDB,
    execution: &ExecutionDB,
    transaction: &TransactionDB,
    function_run: &FunctionRunDB,
    table: &TableDB,
) -> Result<TableDataVersionDB, TdError> {
    _seed_table_data_version(
        db,
        collection,
        execution,
        transaction,
        function_run,
        table,
        Some(&HasData::from(true)),
    )
    .await
}

pub(crate) async fn seed_table_data_version(
    db: impl SeaExecutor + Send,
    collection: &CollectionDB,
    execution: &ExecutionDB,
    transaction: &TransactionDB,
    function_run: &FunctionRunDB,
    table: &TableDB,
) -> Result<TableDataVersionDB, TdError> {
    _seed_table_data_version(
        db,
        collection,
        execution,
        transaction,
        function_run,
        table,
        None,
    )
    .await
}

pub async fn _seed_table_data_version(
    db: impl SeaExecutor + Send,
    collection: &CollectionDB,
    execution: &ExecutionDB,
    transaction: &TransactionDB,
    function_run: &FunctionRunDB,
    table: &TableDB,
    has_data: Option<&HasData>,
) -> Result<TableDataVersionDB, TdError> {
    let table_data_version_db = TableDataVersionDB::builder()
        .collection_id(collection.id)
        .table_id(table.table_id)
        .name(table.name.clone())
        .table_version_id(table.id)
        .function_version_id(&table.function_version_id)
        .has_data(has_data.cloned())
        .execution_id(&execution.id)
        .transaction_id(&transaction.id)
        .function_run_id(function_run.id)
        .function_param_pos(TableFunctionParamPos::try_from(0)?)
        .triggered_on(function_run.triggered_on)
        .build()?;

    db.insert(&table_data_version_db).await?;
    Ok(table_data_version_db)
}

#[cfg(test)]
mod tests {
    use crate::VersionedQueries;
    use crate::test_utils::SeedQueries;
    use ta_objects::Buildable;
    use ta_objects::and;
    use td_database::pool::TdPool;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::dxo::table::TableDB;
    use td_objects::types::basic::{
        AtTime, BundleId, CollectionName, Decorator, FunctionRunStatus, TableName, TableNameDto,
        TransactionKey, UserId,
    };
    use td_security::ENCODED_ID_SYSTEM;

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_seed_table_data_version(db: TdPool) {
        let collection = db
            .seed_collection(
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

        let function_version = db.seed_function(&collection, &create).await.unwrap();

        let execution = db.seed_execution(&function_version).await.unwrap();

        let transaction_key = TransactionKey::try_from("ANY").unwrap();
        let transaction = db
            .seed_transaction(&execution, &transaction_key)
            .await
            .unwrap();

        let function_run = db
            .seed_function_run(
                &collection,
                &function_version,
                &execution,
                &transaction,
                &FunctionRunStatus::Scheduled,
            )
            .await
            .unwrap();

        let table_version = db
            .fetch_one_at::<{ TableDB::All }, TableDB>(
                and!(&collection.id, TableName::try_from(table_name).unwrap()),
                &AtTime::now(),
            )
            .await
            .unwrap();

        let table_data_version = db
            .seed_table_data_version(
                &collection,
                &execution,
                &transaction,
                &function_run,
                &table_version,
            )
            .await
            .unwrap();

        assert_eq!(table_data_version.collection_id, collection.id);
        assert_eq!(table_data_version.table_version_id, table_version.id);
        assert_eq!(table_data_version.function_version_id, function_version.id);
        assert_eq!(table_data_version.has_data, None);
        assert_eq!(table_data_version.execution_id, execution.id);
        assert_eq!(table_data_version.transaction_id, transaction.id);
        assert_eq!(table_data_version.function_run_id, function_run.id);
    }
}
