//
// Copyright 2026 Tabsdata Inc.
//

use crate::Queries;
use crate::QueryExecutor;
use ta_objects::and;
use td_database::access::InProject;
use td_database::domain::{Apiserver, Domained};
use td_error::TdError;
use td_objects::dxo::collection::ActiveCollectionDB;
use td_objects::dxo::execution::ExecutionDB;
use td_objects::dxo::function::FunctionDB;
use td_objects::dxo::logical::{
    LogicalFunctionRunDB, LogicalFunctionRunInsertDB, LogicalFunctionRunStatusUpdateDB,
    LogicalTableDataVersionInsertDB,
};
use td_objects::dxo::table::TableDB;
use td_objects::dxo::transaction::TransactionDB;
use td_objects::types::basic::{
    LogicalFunctionRunId, LogicalFunctionRunStatus, LogicalTableDataVersionId, Trigger,
};

/// What plan-build writes for one run: the LFR identity row plus an LTDV per
/// output table, with no attempt yet.
///
/// LFR identity is unique per `(execution_id, function_id)`. A repeat call
/// with the same pair reuses the existing LFR and aligns its status with the
/// requested one, so the caller's intent isn't silently ignored. Aligning
/// hits the sticky-terminals trigger when the existing LFR is sticky and the
/// new `status` differs — the right failure mode for a test trying to
/// violate the invariant.
pub(crate) async fn seed_logical_function_run(
    mut executor: impl QueryExecutor + Domained<Domain = Apiserver, Scope = InProject>,
    collection: &ActiveCollectionDB,
    function_version: &FunctionDB,
    execution: &ExecutionDB,
    transaction: &TransactionDB,
    status: &LogicalFunctionRunStatus,
) -> Result<LogicalFunctionRunDB, TdError> {
    let existing: Option<LogicalFunctionRunDB> = executor
        .reuse()
        .fetch_optional(and!(&execution.base.id, &function_version.base.function_id))
        .await?;
    let logical_id = match existing {
        Some(existing) => {
            if &existing.base.status != status {
                let update = LogicalFunctionRunStatusUpdateDB { status: *status };
                executor.reuse().update(&update, &existing.base.id).await?;
            }
            existing.base.id
        }
        None => {
            let lfr = LogicalFunctionRunInsertDB {
                id: LogicalFunctionRunId::new(),
                collection_id: collection.id,
                execution_id: execution.base.id.clone(),
                transaction_id: transaction.base.id.clone(),
                function_id: function_version.base.function_id,
                function_version_id: function_version.base.id.clone(),
                trigger: Trigger::Manual,
                triggered_on: transaction.base.triggered_on,
                triggered_by_id: transaction.base.triggered_by_id,
                status: *status,
                group_id: collection.group_id,
            };
            executor.reuse().insert(&lfr).await?;
            lfr.id
        }
    };
    let logical: LogicalFunctionRunDB = executor.reuse().fetch_one(&logical_id).await?;

    let tables: Vec<TableDB> = executor
        .reuse()
        .latest()
        .fetch_all::<TableDB>(&function_version.base.id)
        .await?;
    for t in &tables {
        let ltdv = LogicalTableDataVersionInsertDB {
            id: LogicalTableDataVersionId::new(),
            collection_id: collection.id,
            execution_id: execution.base.id.clone(),
            transaction_id: transaction.base.id.clone(),
            table_id: t.base.table_id,
            producer_logical_function_run_id: Some(logical.base.id.clone()),
            function_param_pos: t.base.function_param_pos,
            private: t.base.private,
            triggered_on: transaction.base.triggered_on,
            triggered_by_id: transaction.base.triggered_by_id,
            group_id: logical.base.group_id,
        };
        executor.reuse().insert(&ltdv).await?;
    }

    Ok(logical)
}
