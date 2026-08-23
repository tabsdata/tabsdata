//
// Copyright 2025 Tabsdata Inc.
//

#[ta_objects::dxo]
mod definitions {
    use crate::dxo::function::FunctionDB;
    use crate::dxo::run_metrics::RunMetrics;
    use crate::dxo::yaml::WorkerOutput;
    use crate::dxo::yaml::function::WrittenTable;
    use crate::types::basic::{
        AtTime, ByteCount, CollectionId, CollectionName, ConnectionId, ConnectionKind, CronMask,
        Enabled, EpochMs, ExecutionId, ExecutionName, FunctionName, FunctionVersionId,
        LogicalFunctionRunId, PhysicalFunctionRunId, ProjectId, ProjectName, RowCount,
        SizeThreshold, TableVersionCount, TimeoutThresholdMin, TransactionId, WorkerConfig,
        WorkerId, WorkerMessageStatus, WorkerStatus, WorkerType,
    };
    use chrono::{DateTime, Utc};
    use ta_objects::Buildable;
    use ta_objects::{and, or};
    use td_common::bucket::BucketFloors;
    use td_common::datetime::Timestamp;
    use td_common::execution_status::WorkerCallbackStatus;
    use td_common::worker::ResponseMessagePayload;
    use td_error::TdError;

    #[ta_objects::Dao]
    #[dao(sql_table = "workers")]
    #[dxo(builder(try_from = FunctionDB, skip_all))]
    pub struct WorkerDB {
        #[builder(default = "WorkerId::new()")]
        #[dxo(extractor)]
        pub id: WorkerId,
        /// The project this worker belongs to. Workers always belong to the
        /// same project as the function-run they process. Set explicitly (not
        /// mapped from `FunctionDB`, which no longer carries `project_id`): the
        /// scheduler stamps it from the physical run, the register path from the
        /// request scope. No builder default: a missing setter is a programmer
        /// error, not a silent project fallback.
        #[dxo(extractor, setter)]
        pub project_id: ProjectId,
        #[dxo(builder(include), extractor)]
        pub collection_id: CollectionId,
        #[builder(default)]
        pub execution_id: Option<ExecutionId>,
        #[builder(default)]
        pub transaction_id: Option<TransactionId>,
        #[builder(default)]
        pub physical_function_run_id: Option<PhysicalFunctionRunId>,
        #[dxo(builder(include, field = "id"))]
        pub function_version_id: FunctionVersionId,
        #[builder(default = "WorkerMessageStatus::None")]
        pub message_status: WorkerMessageStatus,
        #[builder(default)]
        pub started_on: Option<AtTime>,
        #[builder(default)]
        pub ended_on: Option<AtTime>,
        #[builder(default = "WorkerStatus::Scheduled")]
        pub status: WorkerStatus,

        pub worker_type: WorkerType, // added in v4
        #[dxo(builder(include, field = "enabled"), extractor)]
        pub worker_enabled: Enabled, // added in v4
        pub worker_config: Option<WorkerConfig>, // added in v4
    }

    #[ta_objects::Dao]
    #[dao(sql_table = "workers__with_names",
        states(
            EnabledTriggers = and!(Enabled::TRUE, or!(WorkerType::CronTrigger, WorkerType::StageTrigger)),
            CronTriggersToSchedule = and!(Enabled::TRUE, WorkerType::CronTrigger, WorkerStatus::Scheduled),
            CronTriggersToStop = and!(Enabled::TRUE, WorkerType::CronTrigger, WorkerStatus::StopScheduled),
            // In flight = started and not stopping. Cron rows can park at
            // Done/Failed via fire-side reports and recovery must keep
            // owning them; a stage worker's own Failed callback is terminal
            // (re-armed by enable/update), so Failed is excluded there.
            CronTriggersInFlight = and!(Enabled::TRUE, WorkerType::CronTrigger, or!(WorkerStatus::Requested, WorkerStatus::Running, WorkerStatus::Done, WorkerStatus::Failed)),
            StageTriggersToSchedule = and!(Enabled::TRUE, WorkerType::StageTrigger, WorkerStatus::Scheduled),
            StageTriggersToStop = and!(Enabled::TRUE, WorkerType::StageTrigger, WorkerStatus::StopScheduled),
            StageTriggersInFlight = and!(Enabled::TRUE, WorkerType::StageTrigger, or!(WorkerStatus::Requested, WorkerStatus::Running, WorkerStatus::Done)),
        )
    )]
    #[inherits(WorkerDB)]
    pub struct WorkerDBWithNames {
        #[dxo(extractor)]
        pub id: WorkerId,

        pub logical_function_run_id: Option<LogicalFunctionRunId>,

        pub collection: CollectionName,
        // The collection's connection (NULL for non-source/destination),
        // joined from the collections table for the stage/cron-trigger requests.
        pub connection_id: Option<ConnectionId>,
        pub connection: Option<ConnectionKind>,
        pub execution: Option<ExecutionName>,
        pub function: FunctionName,
        pub enabled: Enabled,
        pub worker_type: WorkerType,
        // Patch the attempt's PFR ran (NULL = head), from physical_function_runs.
        pub function_version_patch_id: Option<FunctionVersionId>,
        pub project: Option<ProjectName>,
    }

    #[ta_objects::Dto]
    #[dto(list(on = WorkerDBWithNames))]
    pub struct Worker {
        #[dto(list(pagination_by = "+", filter))]
        pub id: WorkerId,
        #[dto(list(filter))]
        pub project_id: ProjectId,
        #[dto(list(filter, order_by))]
        pub collection_id: CollectionId,
        #[dto(list(filter, filter_like, order_by))]
        pub execution_id: Option<ExecutionId>,
        #[dto(list(filter, filter_like, order_by))]
        pub transaction_id: Option<TransactionId>,
        #[dto(list(filter, order_by))]
        pub physical_function_run_id: Option<PhysicalFunctionRunId>,
        #[dto(list(filter, filter_like, order_by))]
        pub function_version_id: FunctionVersionId,
        #[dto(list(filter, order_by))]
        pub message_status: WorkerMessageStatus,
        #[dto(list(filter, order_by))]
        pub started_on: Option<AtTime>,
        #[dto(list(filter, order_by))]
        pub ended_on: Option<AtTime>,
        #[dto(list(filter, order_by))]
        pub status: WorkerStatus,

        #[dto(list(filter, order_by))]
        pub logical_function_run_id: Option<LogicalFunctionRunId>,

        #[dto(list(filter, filter_like, order_by))]
        pub collection: CollectionName,
        #[dto(list(filter, filter_like))]
        pub execution: Option<ExecutionName>,
        #[dto(list(filter, filter_like, order_by))]
        pub function: FunctionName,
        #[dto(list(filter, order_by))]
        pub worker_type: WorkerType,
        #[dto(list(filter, order_by))]
        pub function_version_patch_id: Option<FunctionVersionId>,
        #[dto(list(filter, filter_like, order_by))]
        pub project: Option<ProjectName>,
    }

    pub type CallbackRequest = ResponseMessagePayload<WorkerOutput>;

    #[ta_objects::Dlo]
    pub struct UpdateWorkerExecution {
        pub started_on: Option<AtTime>,
        pub ended_on: Option<AtTime>,
        pub status: WorkerCallbackStatus,
        // Bucket floors derived from `ended_on`. NULL while still
        // running (no `ended_on` yet); populated at terminalization
        // via td_common::bucket::BucketFloors::from_utc.
        #[builder(default)]
        pub ended_on_hour_ms: Option<EpochMs>,
        #[builder(default)]
        pub ended_on_day_ms: Option<EpochMs>,
        #[builder(default)]
        pub ended_on_week_ms: Option<EpochMs>,
        #[builder(default)]
        pub ended_on_month_ms: Option<EpochMs>,
        // Per-attempt metrics aggregated from the worker callback's
        // Function output. `metric_data_bytes` stays None when the
        // worker yaml omits `data_bytes` on every written table.
        #[builder(default)]
        pub metric_table_versions: Option<TableVersionCount>,
        #[builder(default)]
        pub metric_rows: Option<RowCount>,
        #[builder(default)]
        pub metric_data_bytes: Option<ByteCount>,
    }

    impl TryFrom<&CallbackRequest> for UpdateWorkerExecution {
        type Error = TdError;

        fn try_from(value: &CallbackRequest) -> Result<Self, Self::Error> {
            let ended_on = value
                .end
                .map(|v| {
                    let ts = Timestamp::try_from(v)?;
                    AtTime::try_from(ts)
                })
                .transpose()?;

            // Compute bucket floors from `ended_on` if present. Single
            // atomic site for derivation: every PFR row whose `ended_on`
            // is set also gets its four bucket-floor columns set.
            let floors = ended_on.as_ref().map(|t| {
                let dt: DateTime<Utc> = ***t;
                BucketFloors::from_utc(dt)
            });

            // Aggregate per-attempt metrics from the worker's Function
            // output into a single RunMetrics. `None` = no output payload
            // (E/F/X/Y/U); `Some(_)` = output, with `data_bytes` itself
            // optional for the older-worker-yaml case.
            let metrics: Option<RunMetrics> = match &value.context {
                Some(WorkerOutput::Function(o)) => {
                    let mut tables: i64 = 0;
                    let mut rows: i64 = 0;
                    let mut bytes: Option<i64> = None;
                    for w in &o.output {
                        let info = match w {
                            WrittenTable::Data { info, .. }
                            | WrittenTable::Partitions { info, .. } => info,
                            WrittenTable::NoData { .. } => continue,
                        };
                        tables += 1;
                        rows += *info.row_count;
                        if let Some(b) = &info.data_bytes {
                            bytes = Some(bytes.unwrap_or(0) + **b);
                        }
                    }
                    Some(RunMetrics {
                        table_versions: TableVersionCount::try_from(tables)?,
                        rows: RowCount::try_from(rows)?,
                        data_bytes: bytes.map(ByteCount::try_from).transpose()?,
                    })
                }
                _ => None,
            };

            // Flatten RunMetrics into the three nullable DAO fields. The
            // SQL boundary is column-per-metric, so we expand here at
            // the in-memory → DAO seam (rather than carrying an
            // `Option<RunMetrics>` field into a struct sea-query maps
            // 1:1 to columns).
            let metric_table_versions = metrics.as_ref().map(|m| m.table_versions);
            let metric_rows = metrics.as_ref().map(|m| m.rows);
            let metric_data_bytes = metrics.as_ref().and_then(|m| m.data_bytes);

            Ok(UpdateWorkerExecution::builder()
                .started_on(
                    value
                        .start
                        .map(|v| {
                            let ts = Timestamp::try_from(v)?;
                            AtTime::try_from(ts)
                        })
                        .transpose()?,
                )
                .ended_on(ended_on)
                .status(value.status.clone())
                .ended_on_hour_ms(floors.map(|f| EpochMs::try_from(f.hour_ms)).transpose()?)
                .ended_on_day_ms(floors.map(|f| EpochMs::try_from(f.day_ms)).transpose()?)
                .ended_on_week_ms(floors.map(|f| EpochMs::try_from(f.week_ms)).transpose()?)
                .ended_on_month_ms(floors.map(|f| EpochMs::try_from(f.month_ms)).transpose()?)
                .metric_table_versions(metric_table_versions)
                .metric_rows(metric_rows)
                .metric_data_bytes(metric_data_bytes)
                .build()?)
        }
    }

    #[ta_objects::Dao]
    #[dao(update, sql_table = "workers")]
    #[dxo(builder(try_from = UpdateWorkerExecution))]
    pub struct UpdateWorkerDB {
        #[dao(immutable)]
        pub started_on: Option<AtTime>,
        pub ended_on: Option<AtTime>,
        pub status: WorkerStatus,
    }

    impl UpdateWorkerDB {
        pub async fn requested() -> Result<Self, TdError> {
            Ok(Self {
                started_on: None,
                ended_on: None,
                status: WorkerStatus::Requested,
            })
        }

        pub async fn stop_scheduled() -> Result<Self, TdError> {
            Ok(Self {
                started_on: None,
                ended_on: None,
                status: WorkerStatus::StopScheduled,
            })
        }

        pub async fn stop_requested() -> Result<Self, TdError> {
            Ok(Self {
                started_on: None,
                ended_on: None,
                status: WorkerStatus::StopRequested,
            })
        }

        pub async fn stopped(at: &AtTime) -> Result<Self, TdError> {
            Ok(Self {
                started_on: None,
                ended_on: Some(*at),
                status: WorkerStatus::Stopped,
            })
        }
    }

    #[ta_objects::Dao]
    #[dao(update, sql_table = "workers")]
    pub struct UpdateWorkerMessageStatusDB {
        message_status: WorkerMessageStatus,
    }

    impl UpdateWorkerMessageStatusDB {
        pub async fn locked() -> Result<Self, TdError> {
            Ok(Self {
                message_status: WorkerMessageStatus::Locked,
            })
        }

        pub async fn unlocked() -> Result<Self, TdError> {
            Ok(Self {
                message_status: WorkerMessageStatus::Unlocked,
            })
        }
    }

    #[ta_objects::Dto]
    pub struct CronTriggerConfig {
        pub start: Option<DateTime<Utc>>,
        pub end: Option<DateTime<Utc>>,
        pub mask: CronMask,
    }

    #[ta_objects::Dto]
    pub struct StageTriggerConfig {
        pub timeout_threshold_min: TimeoutThresholdMin,
        pub size_threshold: SizeThreshold,
        pub start: Option<DateTime<Utc>>,
        pub end: Option<DateTime<Utc>>,
    }
}
