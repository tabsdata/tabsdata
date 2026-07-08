//
// Copyright 2025 Tabs Data Inc.
//

use crate::LiveEntry;
use crate::QueryExecutor;
use ta_objects::Buildable;
use td_database::domain::{Apiserver, Domained};
use td_error::TdError;
use td_objects::and;
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::execution::ExecutionDB;
use td_objects::dxo::logical::{
    LogicalFunctionRunDB, LogicalFunctionRunInputDB, LogicalPointerDao, LogicalTableDataVersionDB,
    LogicalTableDataVersionPointerDB,
};
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionDB;
use td_objects::dxo::table::TableDB;
use td_objects::dxo::transaction::TransactionDB;
use td_objects::types::basic::{
    AtTime, DependencyPos, LogicalPointerReason, LogicalPointerStatus, VersionPos,
};

/// Seed an LFR_input row mirroring a `(consumer FR, upstream FR + TDV)`
/// dependency edge into the logical layer. When upstream is non-NULL
/// also seeds the LTDV identity row + a Bound pointer event so cascade
/// walks see a complete chain. Runs on the caller's executor: if it is
/// project-scoped (`db.scoped(p)`), the field-less `LogicalFunctionRunInputDB`
/// defaults its `project_id` from the scope; the LTDV / pointer rows carry it
/// explicitly.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn seed_logical_input(
    mut executor: impl QueryExecutor + Domained<Domain = Apiserver>,
    collection: &CollectionDB,
    execution: &ExecutionDB,
    transaction: &TransactionDB,
    physical_function_run: &PhysicalFunctionRunDB,
    requirement_table: &TableDB,
    requirement_function_run: Option<&PhysicalFunctionRunDB>,
    requirement_table_data_version: Option<&PhysicalTableDataVersionDB>,
    requirement_dependency_pos: Option<&DependencyPos>,
    requirement_version_pos: &VersionPos,
) -> Result<LogicalFunctionRunInputDB, TdError> {
    let consumer_lfr: LogicalFunctionRunDB = executor
        .reuse()
        .live()
        .fetch_one(and!(
            &physical_function_run.execution_id,
            &physical_function_run.function_id
        ))
        .await?;
    let consumer_lfr_id = consumer_lfr.id.clone();
    let mut input_builder = LogicalFunctionRunInputDB::builder();
    input_builder
        .collection_id(collection.id)
        .execution_id(&execution.id)
        .transaction_id(&transaction.id)
        .logical_function_run_id(consumer_lfr_id)
        .requirement_input_idx(None)
        .requirement_dependency_pos(requirement_dependency_pos.cloned())
        .requirement_version_pos(requirement_version_pos);

    if let (Some(req_fr), Some(req_tdv)) =
        (requirement_function_run, requirement_table_data_version)
    {
        let producer_lfr: LogicalFunctionRunDB = executor
            .reuse()
            .live()
            .fetch_one(and!(&req_fr.execution_id, &req_fr.function_id))
            .await?;
        let producer_lfr_id = producer_lfr.id.clone();
        let ltdv = LogicalTableDataVersionDB::builder()
            .project_id(producer_lfr.project_id)
            .group_id(producer_lfr.group_id)
            .collection_id(collection.id)
            .execution_id(&execution.id)
            .transaction_id(&transaction.id)
            .table_id(requirement_table.table_id)
            .producer_logical_function_run_id(Some(producer_lfr_id))
            .private(requirement_table.private)
            .triggered_on(transaction.triggered_on)
            .triggered_by_id(transaction.triggered_by_id)
            .build()?;
        executor
            .reuse()
            .live()
            .insert::<LogicalTableDataVersionDB>(&ltdv)
            .await?;

        let pointer = LogicalTableDataVersionPointerDB::build_event(
            ltdv.project_id,
            &ltdv.id,
            LogicalPointerStatus::Bound,
            Some(req_tdv.id.clone()),
            AtTime::try_from(transaction.triggered_on)?,
            transaction.triggered_by_id,
            LogicalPointerReason::InitialDispatch,
        )?;
        executor
            .reuse()
            .live()
            .insert::<LogicalTableDataVersionPointerDB>(&pointer)
            .await?;

        input_builder.requirement_logical_table_data_version_id(Some(ltdv.id));
    }

    let ffr_input = input_builder.build()?;
    executor
        .reuse()
        .live()
        .insert::<LogicalFunctionRunInputDB>(&ffr_input)
        .await?;
    Ok(ffr_input)
}

#[cfg(test)]
mod tests {
    use super::LogicalFunctionRunDB;
    use super::and;
    use crate::test_utils::SeedEntry;
    use crate::{CurrentEntry, LiveEntry};
    use ta_objects::Buildable;
    use td_database::pool::TdPool;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::dxo::table::TableDB;
    use td_objects::types::basic::{
        BundleId, CollectionName, Decorator, DependencyPos, LogicalFunctionRunStatus, ProjectId,
        TableNameDto, TransactionKey, UserId, VersionPos,
    };
    use td_security::ENCODED_ID_SYSTEM;

    #[td_test::test(db)]
    async fn test_seed_logical_input(db: TdPool) {
        let collection = db
            .scoped(ProjectId::sandbox())
            .seed()
            .collection(
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

        let function = db
            .scoped(ProjectId::sandbox())
            .seed()
            .function(&collection, &create)
            .await
            .unwrap();
        let execution = db
            .scoped(ProjectId::sandbox())
            .seed()
            .execution(&function)
            .await
            .unwrap();
        let transaction_key = TransactionKey::try_from("ANY").unwrap();
        let transaction = db
            .scoped(ProjectId::sandbox())
            .seed()
            .transaction(&execution, &transaction_key)
            .await
            .unwrap();

        let physical_function_run = db
            .scoped(ProjectId::sandbox())
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
            .current()
            .fetch_all::<TableDB>(&function.id)
            .await
            .unwrap();
        assert_eq!(requirement_tables.len(), 1);
        let requirement_table = &requirement_tables[0];

        let requirement_table_data_version = db
            .scoped(ProjectId::sandbox())
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

        let function = db
            .scoped(ProjectId::sandbox())
            .seed()
            .function(&collection, &create)
            .await
            .unwrap();
        let execution = db
            .scoped(ProjectId::sandbox())
            .seed()
            .execution(&function)
            .await
            .unwrap();
        let transaction = db
            .scoped(ProjectId::sandbox())
            .seed()
            .transaction(&execution, &transaction_key)
            .await
            .unwrap();

        let requirement_function_run = db
            .scoped(ProjectId::sandbox())
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
            .scoped(ProjectId::sandbox())
            .seed()
            .logical_input(
                &collection,
                &execution,
                &transaction,
                &physical_function_run,
                requirement_table,
                Some(&requirement_function_run),
                Some(&requirement_table_data_version),
                Some(&DependencyPos::try_from(0).unwrap()),
                &VersionPos::try_from(0).unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(ffri.collection_id, collection.id);
        assert_eq!(ffri.execution_id, execution.id);
        let consumer_lfr: LogicalFunctionRunDB = db
            .live()
            .fetch_one::<LogicalFunctionRunDB>(and!(
                &physical_function_run.execution_id,
                &physical_function_run.function_id
            ))
            .await
            .unwrap();
        assert_eq!(ffri.logical_function_run_id, consumer_lfr.id);
        assert!(ffri.requirement_logical_table_data_version_id.is_some());
        assert_eq!(
            ffri.requirement_dependency_pos,
            Some(DependencyPos::try_from(0).unwrap())
        );
        assert_eq!(
            ffri.requirement_version_pos,
            VersionPos::try_from(0).unwrap()
        );
    }
}
