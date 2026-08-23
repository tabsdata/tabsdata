//
// Copyright 2026 Tabsdata Inc.
//

use crate::execution::layers::update_status::{
    abandon_requested_physical_runs, update_function_run_status,
};
use crate::recovery::layers::{
    fail_stale_abandoned_attempts, fetch_stale_dispatched_runs, stuck_lfrs_from_stale,
};
use crate::scheduler::layers::request::dispatch_at_time;
use ta_services::factory::service_factory;
use td_execution::RecoveryConfig;
use td_objects::dxo::logical::UpdateLogicalFunctionRunDB;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

/// Time-based stale sweep — the only recovery when workloads are
/// unobservable: stale `Dispatched` PFRs → `Failed`; those under a
/// `Requested` LFR additionally get the operator-recover flow.
#[service_factory(
    name = RecoverySweepService,
    request = (),
    response = (),
    connection = TransactionProvider,
    scope = "Global",
    context = RecoveryConfig,
)]
fn service() {
    layers!(
        from_fn(dispatch_at_time),
        from_fn(fetch_stale_dispatched_runs),
        from_fn(fail_stale_abandoned_attempts),
        from_fn(stuck_lfrs_from_stale),
        from_fn(abandon_requested_physical_runs),
        from_fn(UpdateLogicalFunctionRunDB::recover),
        from_fn(update_function_run_status),
    )
}

#[cfg(all(test, feature = "test_tower_metadata"))]
mod tests {
    use super::*;

    #[td_test::test(db)]
    async fn test_tower_metadata_recovery_sweep(
        db: td_database::pool::TdPool,
    ) -> Result<(), td_error::TdError> {
        use ta_services::service::TdService;
        use td_tower::metadata::type_of_val;

        RecoverySweepService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<(), ()>(&[
                type_of_val(&dispatch_at_time),
                type_of_val(&fetch_stale_dispatched_runs),
                type_of_val(&fail_stale_abandoned_attempts),
                type_of_val(&stuck_lfrs_from_stale),
                type_of_val(&abandon_requested_physical_runs),
                type_of_val(&UpdateLogicalFunctionRunDB::recover),
                type_of_val(&update_function_run_status),
            ]);
        Ok(())
    }
}
