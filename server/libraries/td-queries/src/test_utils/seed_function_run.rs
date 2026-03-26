//
// Copyright 2025 Tabs Data Inc.
//

use crate::BasicQueries;
use ta_objects::Buildable;
use td_database::sea_query::executor::SeaExecutor;
use td_error::TdError;
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::execution::ExecutionDB;
use td_objects::dxo::function::FunctionDB;
use td_objects::dxo::function_run::FunctionRunDB;
use td_objects::dxo::transaction::TransactionDB;
use td_objects::types::basic::{FunctionRunStatus, Trigger};

pub(crate) async fn seed_function_run(
    db: impl SeaExecutor + Send,
    collection: &CollectionDB,
    function_version: &FunctionDB,
    execution: &ExecutionDB,
    transaction: &TransactionDB,
    status: &FunctionRunStatus,
) -> Result<FunctionRunDB, TdError> {
    let function_db = FunctionRunDB::builder()
        .collection_id(collection.id)
        .function_id(function_version.function_id)
        .function_version_id(&function_version.id)
        .execution_id(&execution.id)
        .transaction_id(&transaction.id)
        .triggered_on(transaction.triggered_on)
        .triggered_by_id(transaction.triggered_by_id)
        .trigger(Trigger::Manual)
        .status(status.clone())
        .build()?;

    db.insert::<FunctionRunDB>(&function_db).await?;
    Ok(function_db)
}

#[cfg(test)]
mod tests {
    use crate::test_utils::SeedQueries;
    use ta_objects::Buildable;
    use td_database::pool::TdPool;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::types::basic::{
        BundleId, CollectionName, Decorator, FunctionRunStatus, TransactionKey, Trigger, UserId,
    };
    use td_security::ENCODED_ID_SYSTEM;

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_seed_function_run(db: TdPool) {
        let collection = db
            .seed_collection(
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

        assert_eq!(function_run.collection_id, collection.id);
        assert_eq!(function_run.function_version_id, function_version.id);
        assert_eq!(function_run.execution_id, execution.id);
        assert_eq!(function_run.transaction_id, transaction.id);
        assert_eq!(function_run.triggered_on, transaction.triggered_on);
        assert_eq!(function_run.trigger, Trigger::Manual);
        assert_eq!(function_run.status, FunctionRunStatus::Scheduled);
    }
}
