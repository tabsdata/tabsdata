//
// Copyright 2025 Tabs Data Inc.
//

use crate::BasicQueries;
use ta_objects::Buildable;
use td_database::sea_query::executor::SeaExecutor;
use td_error::TdError;
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::execution::ExecutionDB;
use td_objects::dxo::function_requirement::FunctionRequirementDB;
use td_objects::dxo::function_run::FunctionRunDB;
use td_objects::dxo::table::TableDB;
use td_objects::dxo::table_data_version::TableDataVersionDB;
use td_objects::dxo::transaction::TransactionDB;
use td_objects::types::basic::{DependencyPos, VersionPos};

#[allow(clippy::too_many_arguments)]
pub(crate) async fn seed_function_requirement(
    db: impl SeaExecutor + Send,
    collection: &CollectionDB,
    execution: &ExecutionDB,
    transaction: &TransactionDB,
    function_run: &FunctionRunDB,
    requirement_table: &TableDB,
    requirement_function_run: Option<&FunctionRunDB>,
    requirement_table_data_version: Option<&TableDataVersionDB>,
    requirement_dependency_pos: Option<&DependencyPos>,
    requirement_version_pos: &VersionPos,
) -> Result<FunctionRequirementDB, TdError> {
    let function_requirement_db = FunctionRequirementDB::builder()
        .collection_id(collection.id)
        .execution_id(&execution.id)
        .transaction_id(&transaction.id)
        .function_run_id(function_run.id)
        .function_version_id(&function_run.function_version_id)
        .requirement_table_id(requirement_table.table_id)
        .requirement_function_version_id(&requirement_table.function_version_id)
        .requirement_table_version_id(requirement_table.id)
        .requirement_function_run_id(requirement_function_run.map(|f| f.id))
        .requirement_table_data_version_id(requirement_table_data_version.map(|tdv| tdv.id.clone()))
        .requirement_dependency_pos(requirement_dependency_pos.cloned())
        .requirement_version_pos(requirement_version_pos)
        .build()?;

    db.insert::<FunctionRequirementDB>(&function_requirement_db)
        .await?;
    Ok(function_requirement_db)
}

#[cfg(test)]
mod tests {
    use crate::VersionedQueries;
    use crate::test_utils::SeedQueries;
    use ta_objects::Buildable;
    use td_database::pool::TdPool;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::dxo::table::TableDB;
    use td_objects::types::basic::{
        AtTime, BundleId, CollectionName, Decorator, DependencyPos, FunctionRunStatus,
        TableNameDto, TransactionKey, UserId, VersionPos,
    };
    use td_security::ENCODED_ID_SYSTEM;

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_seed_function_requirement(db: TdPool) {
        let collection = db
            .seed_collection(
                &CollectionName::try_from("collection").unwrap(),
                &UserId::try_from(ENCODED_ID_SYSTEM).unwrap(),
            )
            .await
            .unwrap();

        let create = FunctionRegister::builder()
            .try_name("joaquin")
            .unwrap()
            .try_description("function_foo description")
            .unwrap()
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")
            .unwrap()
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(vec![TableNameDto::try_from("table_1").unwrap()])
            .try_runtime_values("mock runtime values")
            .unwrap()
            .reuse_frozen_tables(false)
            .build()
            .unwrap();

        let function = db.seed_function(&collection, &create).await.unwrap();

        let execution = db.seed_execution(&function).await.unwrap();

        let transaction_key = TransactionKey::try_from("ANY").unwrap();
        let transaction = db
            .seed_transaction(&execution, &transaction_key)
            .await
            .unwrap();

        let function_run = db
            .seed_function_run(
                &collection,
                &function,
                &execution,
                &transaction,
                &FunctionRunStatus::Scheduled,
            )
            .await
            .unwrap();

        let requirement_tables = db
            .fetch_all_at::<{ TableDB::All }, TableDB>(&function.id, &AtTime::now())
            .await
            .unwrap();
        assert_eq!(requirement_tables.len(), 1);
        let requirement_table = &requirement_tables[0];

        let requirement_table_data_version = db
            .seed_table_data_version(
                &collection,
                &execution,
                &transaction,
                &function_run,
                requirement_table,
            )
            .await
            .unwrap();

        let create = FunctionRegister::builder()
            .try_name("joaquin_2")
            .unwrap()
            .try_description("function_foo description")
            .unwrap()
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")
            .unwrap()
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(None)
            .try_runtime_values("mock runtime values")
            .unwrap()
            .reuse_frozen_tables(false)
            .build()
            .unwrap();

        let function = db.seed_function(&collection, &create).await.unwrap();

        let execution = db.seed_execution(&function).await.unwrap();

        let transaction_key = TransactionKey::try_from("ANY").unwrap();
        let transaction = db
            .seed_transaction(&execution, &transaction_key)
            .await
            .unwrap();

        let requirement_function_run = db
            .seed_function_run(
                &collection,
                &function,
                &execution,
                &transaction,
                &FunctionRunStatus::Scheduled,
            )
            .await
            .unwrap();

        let requirement = db
            .seed_function_requirement(
                &collection,
                &execution,
                &transaction,
                &function_run,
                requirement_table,
                Some(&requirement_function_run),
                Some(&requirement_table_data_version),
                Some(&DependencyPos::try_from(0).unwrap()),
                &VersionPos::try_from(0).unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(requirement.collection_id, collection.id);
        assert_eq!(requirement.execution_id, execution.id);
        assert_eq!(requirement.transaction_id, transaction.id);
        assert_eq!(requirement.function_run_id, function_run.id);
        assert_eq!(requirement.requirement_table_id, requirement_table.table_id);
        assert_eq!(
            requirement.requirement_function_version_id,
            requirement_table.function_version_id
        );
        assert_eq!(
            requirement.requirement_table_version_id,
            requirement_table.id
        );
        assert_eq!(
            requirement.requirement_function_run_id,
            Some(requirement_function_run.id)
        );
        assert_eq!(
            requirement.requirement_table_data_version_id,
            Some(requirement_table_data_version.id)
        );
        assert_eq!(
            requirement.requirement_dependency_pos,
            Some(DependencyPos::try_from(0).unwrap())
        );
        assert_eq!(
            requirement.requirement_version_pos,
            VersionPos::try_from(0).unwrap()
        );
    }
}
