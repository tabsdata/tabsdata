//
// Copyright 2025 Tabsdata Inc.
//

use crate::Queries;
use crate::Writes;
use td_database::access::InProject;
use td_database::domain::Apiserver;
use td_error::TdError;
use td_objects::dxo::collection::ActiveCollectionDB;
use td_objects::dxo::execution::ExecutionDB;
use td_objects::dxo::transaction::TransactionDB;
use td_objects::dxo::transaction::TransactionInsertDB;
use td_objects::types::basic::{TransactionId, TransactionKey};

pub(crate) async fn seed_transaction(
    mut executor: impl Writes<Domain = Apiserver, Scope = InProject>,
    execution: &ExecutionDB,
    transaction_key: &TransactionKey,
) -> Result<TransactionDB, TdError> {
    // A transaction's group is a snapshot of its collection's group at mint —
    // resolve it from the collection rather than assuming the global default.
    let collection: ActiveCollectionDB = executor
        .reuse()
        .fetch_one::<ActiveCollectionDB>(&execution.base.collection_id)
        .await?;
    let transaction_db = TransactionInsertDB {
        id: TransactionId::new(),
        collection_id: execution.base.collection_id,
        execution_id: execution.base.id.clone(),
        transaction_by: "ANY".try_into()?,
        transaction_key: transaction_key.clone(),
        triggered_on: execution.base.triggered_on,
        triggered_by_id: execution.base.triggered_by_id,
        group_id: collection.group_id,
    };

    // Field-less row; runs on a project-scoped connection so `project_id`
    // defaults from the scope.
    executor.reuse().insert(&transaction_db).await?;
    Ok(executor.reuse().fetch_one(&transaction_db.id).await?)
}

#[cfg(test)]
mod tests {
    use crate::test_utils::SeedEntry;
    use td_database::pool::TdPool;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::types::basic::ReuseFrozen;
    use td_objects::types::basic::{BundleId, CollectionName, Decorator, TransactionKey, UserId};
    use td_security::ENCODED_ID_SYSTEM;

    #[td_test::test(db)]
    async fn test_seed_transaction(db: TdPool) {
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

        assert_eq!(transaction.base.collection_id, execution.base.collection_id);
        assert_eq!(transaction.base.execution_id, execution.base.id);
        assert_eq!(transaction.base.transaction_key, transaction_key);
        assert_eq!(transaction.base.triggered_on, execution.base.triggered_on);
        assert_eq!(
            transaction.base.triggered_by_id,
            execution.base.triggered_by_id
        );
    }
}
