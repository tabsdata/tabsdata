//
// Copyright 2025 Tabsdata Inc.
//

use crate::Queries;
use crate::Writes;
use td_database::access::InProject;
use td_database::domain::Apiserver;
use td_error::TdError;
use td_objects::and;
use td_objects::dxo::collection::ActiveCollectionDB;
use td_objects::dxo::execution::ExecutionDB;
use td_objects::dxo::logical::{
    LogicalFunctionRunDB, LogicalFunctionRunInputDB, LogicalFunctionRunInputInsertDB,
};
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionDB;
use td_objects::dxo::transaction::TransactionDB;
use td_objects::types::basic::{DependencyPos, LogicalFunctionRunInputId, VersionPos};

/// Seed an LFR_input row mirroring a `(consumer FR, upstream TDV)`
/// dependency edge into the logical layer. The requirement is the logical
/// version the upstream materialization is of, so cascade walks see a
/// complete chain. Runs on the caller's executor: if it is
/// project-scoped (`db.scoped(p)`), the field-less `LogicalFunctionRunInputDB`
/// defaults its `project_id` from the scope.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn seed_logical_input(
    mut executor: impl Writes<Domain = Apiserver, Scope = InProject>,
    collection: &ActiveCollectionDB,
    execution: &ExecutionDB,
    transaction: &TransactionDB,
    physical_function_run: &PhysicalFunctionRunDB,
    requirement_table_data_version: Option<&PhysicalTableDataVersionDB>,
    requirement_dependency_pos: Option<&DependencyPos>,
    requirement_version_pos: &VersionPos,
) -> Result<LogicalFunctionRunInputDB, TdError> {
    let consumer_lfr: LogicalFunctionRunDB = executor
        .reuse()
        .fetch_one(and!(
            &physical_function_run.base.execution_id,
            &physical_function_run.base.function_id
        ))
        .await?;
    let consumer_lfr_id = consumer_lfr.base.id.clone();
    // The requirement is the logical version the given materialization is
    // of — it names it, so there is nothing to mint or bind here.
    let requirement_ltdv_id =
        requirement_table_data_version.map(|tdv| tdv.base.logical_table_data_version_id.clone());

    let ffr_input = LogicalFunctionRunInputInsertDB {
        id: LogicalFunctionRunInputId::default(),
        collection_id: collection.id,
        execution_id: execution.base.id.clone(),
        transaction_id: transaction.base.id.clone(),
        logical_function_run_id: consumer_lfr_id,
        requirement_logical_table_data_version_id: requirement_ltdv_id,
        requirement_input_idx: None,
        requirement_dependency_pos: requirement_dependency_pos.cloned(),
        requirement_version_pos: *requirement_version_pos,
    };
    executor.reuse().insert(&ffr_input).await?;
    Ok(executor.reuse().fetch_one(&ffr_input.id).await?)
}

#[cfg(test)]
mod tests {
    use super::LogicalFunctionRunDB;
    use super::and;
    use crate::Queries;
    use crate::test_utils::SeedEntry;
    use td_database::pool::TdPool;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::dxo::table::TableDB;
    use td_objects::types::basic::ReuseFrozen;
    use td_objects::types::basic::{
        BundleId, CollectionName, Decorator, DependencyPos, LogicalFunctionRunStatus, TableNameDto,
        TransactionKey, UserId, VersionPos,
    };
    use td_security::ENCODED_ID_SYSTEM;

    #[td_test::test(db)]
    async fn test_seed_logical_input(db: TdPool) {
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
            dependencies: None,
            triggers: None,
            tables: vec![TableNameDto::try_from("table_1").unwrap()].into(),
            table_types: None,
            parent_tables: None,
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
        let execution = db.scoped(p.id).seed().execution(&function).await.unwrap();
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
                &function,
                &execution,
                &transaction,
                &LogicalFunctionRunStatus::Scheduled,
            )
            .await
            .unwrap();

        let requirement_tables = db
            .scd2()
            .fetch_all::<TableDB>(&function.base.id)
            .await
            .unwrap();
        assert_eq!(requirement_tables.len(), 1);
        let requirement_table = &requirement_tables[0];

        let requirement_table_data_version = db
            .scoped(p.id)
            .seed()
            .physical_table_data_version(
                &collection,
                &execution,
                &transaction,
                &physical_function_run,
                requirement_table,
            )
            .await
            .unwrap();

        let create = FunctionRegister {
            child_functions: None,
            name: "joaquin_2".try_into().unwrap(),
            description: "function_foo description".try_into().unwrap(),
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into().unwrap(),
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: None,
            table_types: None,
            parent_tables: None,
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
        let execution = db.scoped(p.id).seed().execution(&function).await.unwrap();
        let transaction = db
            .scoped(p.id)
            .seed()
            .transaction(&execution, &transaction_key)
            .await
            .unwrap();

        let _requirement_function_run = db
            .scoped(p.id)
            .seed()
            .physical_function_run(
                &collection,
                &function,
                &execution,
                &transaction,
                &LogicalFunctionRunStatus::Scheduled,
            )
            .await
            .unwrap();

        let ffri = db
            .scoped(p.id)
            .seed()
            .logical_input(
                &collection,
                &execution,
                &transaction,
                &physical_function_run,
                Some(&requirement_table_data_version),
                Some(&DependencyPos::try_from(0).unwrap()),
                &VersionPos::try_from(0).unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(ffri.base.collection_id, collection.id);
        assert_eq!(ffri.base.execution_id, execution.base.id);
        let consumer_lfr: LogicalFunctionRunDB = db
            .fetch_one::<LogicalFunctionRunDB>(and!(
                &physical_function_run.base.execution_id,
                &physical_function_run.base.function_id
            ))
            .await
            .unwrap();
        assert_eq!(ffri.base.logical_function_run_id, consumer_lfr.base.id);
        assert!(
            ffri.base
                .requirement_logical_table_data_version_id
                .is_some()
        );
        assert_eq!(
            ffri.base.requirement_dependency_pos,
            Some(DependencyPos::try_from(0).unwrap())
        );
        assert_eq!(
            ffri.base.requirement_version_pos,
            VersionPos::try_from(0).unwrap()
        );
    }
}
