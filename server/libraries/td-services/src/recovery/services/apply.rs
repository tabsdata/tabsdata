//
// Copyright 2026 Tabsdata Inc.
//

use crate::execution::layers::update_status::{
    abandon_requested_physical_runs, update_function_run_status,
};
use crate::recovery::layers::{
    RecoveryActions, fail_lfrs_from_actions, fail_recovered_attempts,
    fail_running_lfrs_for_recover, failed_worker_ids, failed_worker_update,
    finalize_recovery_actions, recover_lfrs_from_actions,
};
use crate::scheduler::layers::request::dispatch_at_time;
use ta_services::factory::service_factory;
use td_objects::dxo::logical::UpdateLogicalFunctionRunDB;
use td_objects::dxo::worker::{UpdateWorkerDB, WorkerDB};
use td_objects::types::basic::WorkerId;
use td_queries::By;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

/// One short write transaction for the surviving function actions: finalize
/// re-reads run state (a concurrently-terminalized attempt drops out — the
/// callback wins), then worker rows → Failed, attempts → Failed, and the
/// LFRs recover or fail.
#[service_factory(
    name = RecoveryApplyService,
    request = RecoveryActions,
    response = (),
    connection = TransactionProvider,
    scope = "Global",
)]
fn service() {
    layers!(
        from_fn(dispatch_at_time),
        from_fn(finalize_recovery_actions),
        // Worker rows → Failed (informational record).
        from_fn(failed_worker_update),
        from_fn(failed_worker_ids),
        from_fn(By::<Vec<WorkerId>>::update_chunked::<UpdateWorkerDB, WorkerDB>),
        // Attempts → Failed; late callbacks drop at the in-flight filter.
        from_fn(fail_recovered_attempts),
        // Running LFRs with a recover verdict → Failed first; ReScheduled is
        // only reachable from Failed/OnHold/Requested.
        from_fn(fail_running_lfrs_for_recover),
        from_fn(UpdateLogicalFunctionRunDB::failed),
        from_fn(update_function_run_status),
        // Requested and just-failed LFRs with a recover verdict → ReScheduled.
        from_fn(recover_lfrs_from_actions),
        from_fn(abandon_requested_physical_runs),
        from_fn(UpdateLogicalFunctionRunDB::recover),
        from_fn(update_function_run_status),
        // The rest → Failed, with the downstream on-hold cascade.
        from_fn(fail_lfrs_from_actions),
        from_fn(UpdateLogicalFunctionRunDB::failed),
        from_fn(update_function_run_status),
    )
}

#[cfg(all(test, feature = "test_tower_metadata"))]
mod tests {
    use super::*;

    #[td_test::test(db)]
    async fn test_tower_metadata_recovery_apply(
        db: td_database::pool::TdPool,
    ) -> Result<(), td_error::TdError> {
        use ta_services::service::TdService;
        use td_tower::metadata::type_of_val;

        RecoveryApplyService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<RecoveryActions, ()>(&[
                type_of_val(&dispatch_at_time),
                type_of_val(&finalize_recovery_actions),
                type_of_val(&failed_worker_update),
                type_of_val(&failed_worker_ids),
                type_of_val(&By::<Vec<WorkerId>>::update_chunked::<UpdateWorkerDB, WorkerDB>),
                type_of_val(&fail_recovered_attempts),
                type_of_val(&fail_running_lfrs_for_recover),
                type_of_val(&UpdateLogicalFunctionRunDB::failed),
                type_of_val(&update_function_run_status),
                type_of_val(&recover_lfrs_from_actions),
                type_of_val(&abandon_requested_physical_runs),
                type_of_val(&UpdateLogicalFunctionRunDB::recover),
                type_of_val(&update_function_run_status),
                type_of_val(&fail_lfrs_from_actions),
                type_of_val(&UpdateLogicalFunctionRunDB::failed),
                type_of_val(&update_function_run_status),
            ]);
        Ok(())
    }
}
