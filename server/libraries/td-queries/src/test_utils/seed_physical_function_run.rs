//
// Copyright 2025 Tabsdata Inc.
//

use crate::QueryExecutor;
use crate::{CurrentEntry, LiveEntry};
use ta_objects::and;
use td_database::domain::{Apiserver, Domained};
use td_error::TdError;
use td_objects::dxo::collection::ActiveCollectionDB;
use td_objects::dxo::execution::ExecutionDB;
use td_objects::dxo::function::FunctionDB;
use td_objects::dxo::logical::{
    LogicalFunctionRunDB, LogicalFunctionRunPointerDB, LogicalFunctionRunUpdateDB,
    LogicalPointerDao, LogicalTableDataVersionDB, LogicalTableDataVersionPointerDB,
};
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
use td_objects::dxo::table::TableDB;
use td_objects::dxo::transaction::TransactionDB;
use td_objects::types::basic::{
    AtTime, LogicalFunctionRunId, LogicalFunctionRunStatus, LogicalPointerReason,
    LogicalPointerStatus, LogicalTableDataVersionId, PhysicalFunctionRunId,
    PhysicalFunctionRunStatus, Trigger,
};

pub(crate) async fn seed_physical_function_run(
    mut executor: impl QueryExecutor + Domained<Domain = Apiserver>,
    collection: &ActiveCollectionDB,
    function_version: &FunctionDB,
    execution: &ExecutionDB,
    transaction: &TransactionDB,
    status: &LogicalFunctionRunStatus,
) -> Result<PhysicalFunctionRunDB, TdError> {
    // LFR identity is unique per (execution_id, function_id). For a
    // repeat call with the same pair (e.g. seeding a 2nd or 3rd attempt
    // on an existing LFR), reuse the existing LFR and align its status
    // with the requested `status` so the caller's intent isn't silently
    // ignored. Aligning hits the sticky-terminals trigger when the
    // existing LFR is in a sticky state and the new `status` differs —
    // which is the right failure mode for a test trying to violate the
    // invariant.
    let existing: Option<LogicalFunctionRunDB> = executor
        .reuse()
        .live()
        .fetch_optional(and!(&execution.id, &function_version.function_id))
        .await?;
    let logical = match existing {
        Some(existing) => {
            if &existing.status != status {
                let update = LogicalFunctionRunUpdateDB { status: *status };
                executor
                    .reuse()
                    .live()
                    .update::<_, LogicalFunctionRunDB>(&update, &existing.id)
                    .await?;
            }
            LogicalFunctionRunDB {
                status: *status,
                ..existing
            }
        }
        None => {
            let lfr = LogicalFunctionRunDB {
                id: LogicalFunctionRunId::new(),
                project_id: collection.project_id,
                collection_id: collection.id,
                execution_id: execution.id.clone(),
                transaction_id: transaction.id.clone(),
                function_id: function_version.function_id,
                function_version_id: function_version.id.clone(),
                trigger: Trigger::Manual,
                triggered_on: transaction.triggered_on,
                triggered_by_id: transaction.triggered_by_id,
                status: *status,
                group_id: collection.group_id,
            };
            executor
                .reuse()
                .live()
                .insert::<LogicalFunctionRunDB>(&lfr)
                .await?;
            lfr
        }
    };

    let function_db = PhysicalFunctionRunDB {
        id: PhysicalFunctionRunId::new(),
        project_id: collection.project_id,
        collection_id: collection.id,
        function_id: function_version.function_id,
        function_version_id: function_version.id.clone(),
        execution_id: execution.id.clone(),
        transaction_id: transaction.id.clone(),
        triggered_on: transaction.triggered_on,
        triggered_by_id: transaction.triggered_by_id,
        trigger: Trigger::Manual,
        base_path: function_version
            .base_path
            .clone()
            .unwrap_or_else(|| collection.effective_base_path.clone()),
        dispatched_on: None,
        started_on: None,
        ended_on: None,
        status: PhysicalFunctionRunStatus::from(status),
        function_version_patch_id: None,
        ended_on_hour_ms: None,
        ended_on_day_ms: None,
        ended_on_week_ms: None,
        ended_on_month_ms: None,
        metric_table_versions: None,
        metric_rows: None,
        metric_data_bytes: None,
    };

    executor
        .reuse()
        .live()
        .insert::<PhysicalFunctionRunDB>(&function_db)
        .await?;
    let pointer = LogicalFunctionRunPointerDB::build_event(
        logical.project_id,
        &logical.id,
        LogicalPointerStatus::Bound,
        Some(function_db.id.clone()),
        AtTime::try_from(transaction.triggered_on)?,
        transaction.triggered_by_id,
        LogicalPointerReason::InitialDispatch,
    )?;
    executor
        .reuse()
        .live()
        .insert::<LogicalFunctionRunPointerDB>(&pointer)
        .await?;

    // Seed an LTDV + a Bound pointer event for each of the function's
    // output tables — mirrors plan-build, which mints LTDV rows for
    // every output table at the same time it mints the LFR. Without
    // these rows, downstream layers that walk LFR → LTDV (replay
    // append-Requested, callback redirect, etc.) trip over missing
    // identity rows.
    let tables: Vec<TableDB> = executor
        .reuse()
        .current()
        .fetch_all::<TableDB>(&function_version.id)
        .await?;
    for t in &tables {
        let ltdv = LogicalTableDataVersionDB {
            id: LogicalTableDataVersionId::new(),
            project_id: logical.project_id,
            collection_id: collection.id,
            execution_id: execution.id.clone(),
            transaction_id: transaction.id.clone(),
            table_id: t.table_id,
            producer_logical_function_run_id: Some(logical.id.clone()),
            function_param_pos: t.function_param_pos,
            private: t.private,
            triggered_on: transaction.triggered_on,
            triggered_by_id: transaction.triggered_by_id,
            group_id: logical.group_id,
        };
        executor
            .reuse()
            .live()
            .insert::<LogicalTableDataVersionDB>(&ltdv)
            .await?;
        let ltdv_pointer = LogicalTableDataVersionPointerDB::build_event(
            ltdv.project_id,
            &ltdv.id,
            LogicalPointerStatus::Pending,
            None,
            AtTime::try_from(transaction.triggered_on)?,
            transaction.triggered_by_id,
            LogicalPointerReason::InitialMint,
        )?;
        executor
            .reuse()
            .live()
            .insert::<LogicalTableDataVersionPointerDB>(&ltdv_pointer)
            .await?;
    }

    Ok(function_db)
}

#[cfg(test)]
mod tests {
    use crate::test_utils::SeedEntry;
    use td_database::pool::TdPool;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::types::basic::ReuseFrozen;
    use td_objects::types::basic::{
        BundleId, CollectionName, Decorator, LogicalFunctionRunStatus, TransactionKey, Trigger,
        UserId,
    };
    use td_security::ENCODED_ID_SYSTEM;

    #[td_test::test(db)]
    async fn test_seed_function_run(db: TdPool) {
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

        assert_eq!(physical_function_run.collection_id, collection.id);
        assert_eq!(
            physical_function_run.function_version_id,
            function_version.id
        );
        assert_eq!(physical_function_run.execution_id, execution.id);
        assert_eq!(physical_function_run.transaction_id, transaction.id);
        assert_eq!(physical_function_run.triggered_on, transaction.triggered_on);
        assert_eq!(physical_function_run.trigger, Trigger::Manual);
        // Status lives on the LFR; PhysicalFunctionRunDB no longer has it.
        let _ = LogicalFunctionRunStatus::Scheduled;
    }
}
