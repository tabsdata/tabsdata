//
// Copyright 2025 Tabsdata Inc.
//

#[ta_objects::dxo]
mod definitions {
    use crate::dxo::execution::ExecutionDB;
    use crate::dxo::worker::UpdateWorkerExecution;
    use crate::types::basic::{
        AtTime, BasePath, BundleId, ByteCount, CollectionId, CollectionName, ConnectionId,
        ConnectionKind, Decorator, EpochMs, ExecutionId, ExecutionName, FunctionId, FunctionName,
        FunctionVersionId, PhysicalFunctionRunId, PhysicalFunctionRunStatus, ProjectId,
        ProjectName, RowCount, StageId, StorageVersion, TableVersionCount, TransactionId, Trigger,
        TriggeredOn, UserId, UserName,
    };
    use chrono::{DateTime, Utc};
    use td_common::bucket::BucketFloors;
    use td_error::TdError;
    use td_tower::extractors::Ref;

    #[ta_objects::Dao]
    #[dao(
        sql_table = "physical_function_runs",
        order_by(col = "triggered_on", dir = "desc")
    )]
    #[dxo(builder(try_from = ExecutionDB, skip_all))]
    pub struct PhysicalFunctionRunDB {
        #[builder(default = "PhysicalFunctionRunId::new()")]
        pub id: PhysicalFunctionRunId,
        /// The project this run belongs to. Inherited from the parent LFR
        /// at mint; required at construction — there is no default project
        /// fallback because that would mask a builder forgetting to thread the
        /// project from upstream.
        #[dxo(extractor)]
        pub project_id: ProjectId,
        pub collection_id: CollectionId,
        #[dxo(extractor)]
        pub function_id: FunctionId,
        // The fvid of the function being run — distinct from
        // `ExecutionDB.function_version_id`, which is the trigger's fvid.
        #[dxo(extractor)]
        pub function_version_id: FunctionVersionId,
        #[dxo(extractor, builder(field = "id"))]
        pub execution_id: ExecutionId,
        #[dxo(extractor)]
        pub transaction_id: TransactionId,
        #[dxo(builder(include))]
        pub triggered_on: TriggeredOn,
        #[dxo(builder(include))]
        pub triggered_by_id: UserId,
        pub trigger: Trigger,
        // Stamped at dispatch; basis for the stuck-run sweep.
        #[builder(default)]
        pub dispatched_on: Option<AtTime>,
        #[builder(default)]
        pub started_on: Option<AtTime>,
        #[builder(default)]
        pub ended_on: Option<AtTime>,

        // Per-attempt audit status. Canonical flow-control status lives
        // on the LFR (sticky-Committed).
        #[builder(default = PhysicalFunctionRunStatus::Requested)]
        pub status: PhysicalFunctionRunStatus,
        // Patch resolved at mint, if any. NULL → head ran verbatim.
        #[builder(default)]
        pub function_version_patch_id: Option<FunctionVersionId>,

        // UTC epoch-ms bucket floors derived from `ended_on` at
        // terminalization. Written atomically with `status` + `ended_on`
        // by the worker callback. NULL until the attempt terminates.
        #[builder(default)]
        pub ended_on_hour_ms: Option<EpochMs>,
        #[builder(default)]
        pub ended_on_day_ms: Option<EpochMs>,
        #[builder(default)]
        pub ended_on_week_ms: Option<EpochMs>,
        #[builder(default)]
        pub ended_on_month_ms: Option<EpochMs>,

        // Aggregated per-attempt outcome from the worker callback's
        // `RunMetrics`. Populated only on `Done` terminalization;
        // NULL for `Error` / `Failed` and for not-yet-terminal rows.
        #[builder(default)]
        pub metric_table_versions: Option<TableVersionCount>,
        #[builder(default)]
        pub metric_rows: Option<RowCount>,
        #[builder(default)]
        pub metric_data_bytes: Option<ByteCount>,
    }

    #[ta_objects::Dao]
    #[dao(sql_table = "physical_function_runs__with_names")]
    #[inherits(PhysicalFunctionRunDB)]
    pub struct PhysicalFunctionRunDBWithNames {
        pub name: FunctionName,
        pub collection: CollectionName,
        pub project: Option<ProjectName>,
        pub connection: Option<ConnectionKind>,
        pub execution: Option<ExecutionName>,
        pub execution_function_version_id: FunctionVersionId,
        pub triggered_by: UserName,
    }

    #[ta_objects::Dlo]
    #[derive(sqlx::FromRow)] // not a Dao, just a logical layer built from combining them
    #[inherits(PhysicalFunctionRunDBWithNames)]
    pub struct PhysicalFunctionRunToExecute {
        #[dxo(extractor)]
        pub id: PhysicalFunctionRunId,

        pub storage_version: StorageVersion,
        pub bundle_id: BundleId,
        pub defined_on: AtTime,
        pub decorator: Decorator,
        // The collection's connection (NULL for non-source/destination),
        // joined in function_run_to_execute_select for the dispatch request.
        pub connection_id: Option<ConnectionId>,
        // Joined base paths (see function_run_to_execute_select): the effective
        // bundle's stamped prefix, the function version's stage identity + stamped
        // stage prefix, and the collection's live prefix (for run-content).
        pub bundle_base_path: BasePath,
        pub stage_id: StageId,
        pub stage_base_path: BasePath,
        pub collection_base_path: BasePath,
    }

    #[ta_objects::Dto]
    #[dto(list(on = PhysicalFunctionRunDBWithNames))]
    pub struct PhysicalFunctionRun {
        #[dto(list(pagination_by = "+", filter))]
        pub id: PhysicalFunctionRunId,
        #[dto(list(filter, order_by))]
        pub collection_id: CollectionId,
        #[dto(list(filter, order_by))]
        pub function_version_id: FunctionVersionId,
        #[dto(list(filter, filter_like, order_by))]
        pub execution_id: ExecutionId,
        #[dto(list(filter, filter_like, order_by))]
        pub transaction_id: TransactionId,
        #[dto(list(filter, order_by))]
        pub triggered_on: TriggeredOn,
        pub trigger: Trigger,
        #[dto(list(filter))]
        pub started_on: Option<AtTime>,
        #[dto(list(filter))]
        pub ended_on: Option<AtTime>,
        #[dto(list(filter, order_by))]
        pub status: PhysicalFunctionRunStatus,

        #[dto(list(filter, filter_like, order_by))]
        pub name: FunctionName,
        #[dto(list(filter, filter_like, order_by))]
        pub collection: CollectionName,
        #[dto(list(filter, filter_like))]
        pub execution: Option<ExecutionName>,
        #[dto(list(filter, order_by))]
        pub execution_function_version_id: FunctionVersionId,
        pub triggered_by: UserName,
        // TODO exception info
        // pub kind: Option<String>,
        // pub message: Option<String>,
        // pub error_code: Option<String>,
        // pub exit_status: i32,
    }

    // Per-attempt timing + status on the concrete FR. The worker callback
    // writes both via UpdateWorkerExecution → builder. The LFR's
    // canonical status is updated separately via UpdateLogicalFunctionRunDB
    // (sticky-Committed gate applies there, not here).
    //
    // At terminalization, the same UPDATE also writes the four
    // `ended_on_*_ms` bucket floors (computed in Rust from `ended_on`
    // via `td_common::bucket::BucketFloors::from_utc`) and — when the
    // attempt produced output — the three `metric_*` aggregates from
    // the callback's `RunMetrics`. Single atomic write.
    #[ta_objects::Dao]
    #[dao(update, sql_table = "physical_function_runs")]
    #[dxo(builder(try_from = UpdateWorkerExecution))]
    pub struct UpdatePhysicalFunctionRunDB {
        #[dxo(builder(skip))]
        #[builder(default)]
        pub dispatched_on: Option<AtTime>,
        #[dao(immutable)]
        #[builder(default)]
        pub started_on: Option<AtTime>,
        #[builder(default)]
        pub ended_on: Option<AtTime>,
        pub status: PhysicalFunctionRunStatus,

        #[builder(default)]
        pub ended_on_hour_ms: Option<EpochMs>,
        #[builder(default)]
        pub ended_on_day_ms: Option<EpochMs>,
        #[builder(default)]
        pub ended_on_week_ms: Option<EpochMs>,
        #[builder(default)]
        pub ended_on_month_ms: Option<EpochMs>,

        #[builder(default)]
        pub metric_table_versions: Option<TableVersionCount>,
        #[builder(default)]
        pub metric_rows: Option<RowCount>,
        #[builder(default)]
        pub metric_data_bytes: Option<ByteCount>,
    }

    impl UpdatePhysicalFunctionRunDB {
        /// Status transition for the request-layer's dispatch step:
        /// `Requested → Dispatched`, stamped at `dispatched_on`. The same
        /// tx creates the WorkerDB row; the pair (PFR.status=Dispatched,
        /// WorkerDB exists) makes the PFR ineligible for the readiness query.
        pub async fn dispatched(at: Ref<AtTime>) -> Result<Self, td_error::TdError> {
            Ok(Self {
                dispatched_on: Some(*at),
                started_on: None,
                ended_on: None,
                status: PhysicalFunctionRunStatus::Dispatched,
                ended_on_hour_ms: None,
                ended_on_day_ms: None,
                ended_on_week_ms: None,
                ended_on_month_ms: None,
                metric_table_versions: None,
                metric_rows: None,
                metric_data_bytes: None,
            })
        }

        /// Terminalization without a worker callback: recover of a stuck
        /// `Requested` LFR abandons its in-flight attempt as `Failed` at
        /// `ended_on`, so late callbacks from it drop at the in-flight
        /// filter. No metrics (the attempt produced no output).
        pub fn failed_at(ended_on: &AtTime) -> Result<Self, TdError> {
            let dt: DateTime<Utc> = ***ended_on;
            let floors = BucketFloors::from_utc(dt);
            Ok(Self {
                dispatched_on: None,
                started_on: None,
                ended_on: Some(*ended_on),
                status: PhysicalFunctionRunStatus::Failed,
                ended_on_hour_ms: Some(EpochMs::try_from(floors.hour_ms)?),
                ended_on_day_ms: Some(EpochMs::try_from(floors.day_ms)?),
                ended_on_week_ms: Some(EpochMs::try_from(floors.week_ms)?),
                ended_on_month_ms: Some(EpochMs::try_from(floors.month_ms)?),
                metric_table_versions: None,
                metric_rows: None,
                metric_data_bytes: None,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::basic::{AtTime, PhysicalFunctionRunStatus};
    use td_tower::extractors::Ref;

    #[tokio::test]
    async fn test_dispatched_helper_sets_status_and_dispatched_on() {
        let at = AtTime::now();
        let update = UpdatePhysicalFunctionRunDB::dispatched(Ref::new(at))
            .await
            .unwrap();
        assert_eq!(update.status, PhysicalFunctionRunStatus::Dispatched);
        assert_eq!(update.dispatched_on, Some(at));
        assert!(update.started_on.is_none());
        assert!(update.ended_on.is_none());
    }
}
