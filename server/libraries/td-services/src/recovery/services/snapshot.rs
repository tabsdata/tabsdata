//
// Copyright 2026 Tabsdata Inc.
//

use crate::recovery::layers::{
    RecoverySnapshot, build_recovery_snapshot, cron_candidates, current_trigger_versions,
    fn_candidates, stage_candidates, stage_trigger_functions,
};
use crate::scheduler::layers::request::dispatch_at_time;
use ta_services::factory::service_factory;
use td_objects::dxo::worker::WorkerDBWithNames;
use td_queries::By;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

/// One read-only pass over the DB: reconciliation candidates plus the
/// stage function/bundle locations the recreates need, so the orchestrator
/// runs every Kubernetes call with no connection held.
#[service_factory(
    name = RecoverySnapshotService,
    request = (),
    response = RecoverySnapshot,
    connection = ConnectionProvider,
    scope = "Global",
)]
fn service() {
    layers!(
        from_fn(dispatch_at_time),
        from_fn(
            By::<()>::fetch_all_in::<{ WorkerDBWithNames::CronTriggersInFlight }, WorkerDBWithNames>
        ),
        from_fn(cron_candidates),
        from_fn(
            By::<()>::fetch_all_in::<{ WorkerDBWithNames::StageTriggersInFlight }, WorkerDBWithNames>
        ),
        from_fn(stage_candidates),
        from_fn(current_trigger_versions),
        from_fn(fn_candidates),
        from_fn(stage_trigger_functions),
        from_fn(build_recovery_snapshot),
    )
}

#[cfg(all(test, feature = "test_tower_metadata"))]
mod tests {
    use super::*;

    #[td_test::test(db)]
    async fn test_tower_metadata_recovery_snapshot(
        db: td_database::pool::TdPool,
    ) -> Result<(), td_error::TdError> {
        use ta_services::service::TdService;
        use td_tower::metadata::type_of_val;

        RecoverySnapshotService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<(), RecoverySnapshot>(&[
                type_of_val(&dispatch_at_time),
                type_of_val(
                    &By::<()>::fetch_all_in::<
                        { WorkerDBWithNames::CronTriggersInFlight },
                        WorkerDBWithNames,
                    >,
                ),
                type_of_val(&cron_candidates),
                type_of_val(
                    &By::<()>::fetch_all_in::<
                        { WorkerDBWithNames::StageTriggersInFlight },
                        WorkerDBWithNames,
                    >,
                ),
                type_of_val(&stage_candidates),
                type_of_val(&current_trigger_versions),
                type_of_val(&fn_candidates),
                type_of_val(&stage_trigger_functions),
                type_of_val(&build_recovery_snapshot),
            ]);
        Ok(())
    }
}
