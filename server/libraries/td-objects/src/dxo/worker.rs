//
// Copyright 2025 Tabsdata Inc.
//

use crate::dxo::run_metrics::RunMetrics;
use crate::dxo::yaml::WorkerOutput;
use crate::dxo::yaml::function::WrittenTable;
use crate::types::basic::{
    AtTime, ByteCount, CollectionId, CollectionName, ConnectionId, ConnectionKind, CronMask,
    Enabled, EpochMs, ExecutionId, ExecutionName, FunctionName, FunctionVersionId,
    LogicalFunctionRunId, LogsCastNumber, PhysicalFunctionRunId, ProjectId, ProjectName, RowCount,
    SizeThreshold, TableVersionCount, TimeoutThresholdMin, TransactionId, WorkerConfig, WorkerId,
    WorkerMessageStatus, WorkerStatus, WorkerType,
};
use chrono::{DateTime, Utc};
use ta_objects::{FromDao, StateFilter, TypedEnum, TypedOpenApi, TypedSerde, TypedSqlx, and, or};
use td_common::bucket::BucketFloors;
use td_common::datetime::Timestamp;
use td_common::execution_status::WorkerCallbackStatus;
use td_common::logging::LOG_EXTENSION;
use td_common::worker::{
    ERR_LOG_FILE_NAME, FN_LOG_FILE_NAME, OUT_LOG_FILE_NAME, ResponseMessagePayload,
    TD_LOG_FILE_NAME,
};
use td_error::TdError;

#[ta_objects::Dao]
#[dao(sql_table = "workers")]
pub struct WorkerDB {
    pub id: WorkerId,
    /// The project this worker belongs to. Workers always belong to the
    /// same project as the function-run they process. Set explicitly (not
    /// mapped from `FunctionDB`, which no longer carries `project_id`): the
    /// scheduler stamps it from the physical run, the register path from the
    /// request scope. No builder default: a missing setter is a programmer
    /// error, not a silent project fallback.
    pub project_id: ProjectId,
    pub collection_id: CollectionId,
    pub execution_id: Option<ExecutionId>,
    pub transaction_id: Option<TransactionId>,
    pub physical_function_run_id: Option<PhysicalFunctionRunId>,
    pub function_version_id: FunctionVersionId,
    pub message_status: WorkerMessageStatus,
    pub started_on: Option<AtTime>,
    pub ended_on: Option<AtTime>,
    pub status: WorkerStatus,

    pub worker_type: WorkerType,
    pub worker_enabled: Enabled,
    pub worker_config: Option<WorkerConfig>,
}

impl WorkerDB {
    /// On the base table so the readiness read can be claimed —
    /// `workers__with_names` is a view, not lockable.
    pub fn triggers_to_schedule(worker_type: WorkerType) -> StateFilter<Self> {
        StateFilter::of(and!(Enabled::TRUE, worker_type, WorkerStatus::Scheduled))
    }

    /// Trigger workers awaiting a stop. See [`triggers_to_schedule`](Self::triggers_to_schedule).
    pub fn triggers_to_stop(worker_type: WorkerType) -> StateFilter<Self> {
        StateFilter::of(and!(
            Enabled::TRUE,
            worker_type,
            WorkerStatus::StopScheduled
        ))
    }
}

#[ta_objects::Dao]
#[dao(sql_table = "workers__with_names")]
pub struct WorkerDBWithNames {
    #[dao(flatten)]
    pub base: WorkerDB,
    pub logical_function_run_id: Option<LogicalFunctionRunId>,

    pub collection: CollectionName,
    // The collection's connection (NULL for non-source/destination),
    // joined from the collections table for the stage/cron-trigger requests.
    pub connection_id: Option<ConnectionId>,
    pub connection: Option<ConnectionKind>,
    pub execution: Option<ExecutionName>,
    pub function: FunctionName,
    pub enabled: Enabled,
    // Patch the attempt's PFR ran (NULL = head), from physical_function_runs.
    pub function_version_patch_id: Option<FunctionVersionId>,
    pub project: Option<ProjectName>,
}

impl WorkerDBWithNames {
    /// Enabled cron/stage trigger workers.
    pub fn enabled_triggers() -> StateFilter<Self> {
        StateFilter::of(and!(
            Enabled::TRUE,
            or!(WorkerType::CronTrigger, WorkerType::StageTrigger)
        ))
    }

    /// In flight = started and not stopping. Cron rows can park at
    /// Done/Failed via fire-side reports and recovery must keep owning them.
    pub fn cron_triggers_in_flight() -> StateFilter<Self> {
        StateFilter::of(and!(
            Enabled::TRUE,
            WorkerType::CronTrigger,
            or!(
                WorkerStatus::Requested,
                WorkerStatus::Running,
                WorkerStatus::Done,
                WorkerStatus::Failed
            )
        ))
    }

    /// A stage worker's own Failed callback is terminal (re-armed by
    /// enable/update), so Failed is excluded here.
    pub fn stage_triggers_in_flight() -> StateFilter<Self> {
        StateFilter::of(and!(
            Enabled::TRUE,
            WorkerType::StageTrigger,
            or!(
                WorkerStatus::Requested,
                WorkerStatus::Running,
                WorkerStatus::Done
            )
        ))
    }
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

impl FromDao for Worker {
    type Dao = WorkerDBWithNames;

    fn from_dao(dao: WorkerDBWithNames) -> Result<Self, TdError> {
        Ok(Self {
            id: dao.base.id,
            project_id: dao.base.project_id,
            collection_id: dao.base.collection_id,
            execution_id: dao.base.execution_id,
            transaction_id: dao.base.transaction_id,
            physical_function_run_id: dao.base.physical_function_run_id,
            function_version_id: dao.base.function_version_id,
            message_status: dao.base.message_status,
            started_on: dao.base.started_on,
            ended_on: dao.base.ended_on,
            status: dao.base.status,
            logical_function_run_id: dao.logical_function_run_id,
            collection: dao.collection,
            execution: dao.execution,
            function: dao.function,
            worker_type: dao.base.worker_type,
            function_version_patch_id: dao.function_version_patch_id,
            project: dao.project,
        })
    }
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
    pub ended_on_hour_ms: Option<EpochMs>,
    pub ended_on_day_ms: Option<EpochMs>,
    pub ended_on_week_ms: Option<EpochMs>,
    pub ended_on_month_ms: Option<EpochMs>,
    // Per-attempt metrics aggregated from the worker callback's
    // Function output. `metric_data_bytes` stays None when the
    // worker yaml omits `data_bytes` on every written table.
    pub metric_table_versions: Option<TableVersionCount>,
    pub metric_rows: Option<RowCount>,
    pub metric_data_bytes: Option<ByteCount>,
}

impl TryFrom<&CallbackRequest> for UpdateWorkerExecution {
    type Error = TdError;

    fn try_from(value: &CallbackRequest) -> Result<Self, Self::Error> {
        let ended_on = value
            .end
            .map(|v| {
                let ts = Timestamp::try_from(v)?;
                Ok::<_, TdError>(AtTime::from(ts))
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
                        WrittenTable::Data { info, .. } | WrittenTable::Partitions { info, .. } => {
                            info
                        }
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

        Ok(UpdateWorkerExecution {
            started_on: value
                .start
                .map(|v| {
                    let ts = Timestamp::try_from(v)?;
                    Ok::<_, TdError>(AtTime::from(ts))
                })
                .transpose()?,
            ended_on,
            status: value.status.clone(),
            ended_on_hour_ms: floors.map(|f| EpochMs::try_from(f.hour_ms)).transpose()?,
            ended_on_day_ms: floors.map(|f| EpochMs::try_from(f.day_ms)).transpose()?,
            ended_on_week_ms: floors.map(|f| EpochMs::try_from(f.week_ms)).transpose()?,
            ended_on_month_ms: floors.map(|f| EpochMs::try_from(f.month_ms)).transpose()?,
            metric_table_versions,
            metric_rows,
            metric_data_bytes,
        })
    }
}

#[ta_objects::Dao]
#[dao(update, sql_table = "workers")]
pub struct UpdateWorkerDB {
    #[dao(immutable)]
    pub started_on: Option<AtTime>,
    pub ended_on: Option<AtTime>,
    pub status: WorkerStatus,
}

impl UpdateWorkerDB {
    pub fn requested() -> Self {
        Self {
            started_on: None,
            ended_on: None,
            status: WorkerStatus::Requested,
        }
    }

    pub fn stop_scheduled() -> Self {
        Self {
            started_on: None,
            ended_on: None,
            status: WorkerStatus::StopScheduled,
        }
    }

    pub fn stop_requested() -> Self {
        Self {
            started_on: None,
            ended_on: None,
            status: WorkerStatus::StopRequested,
        }
    }

    pub fn stopped(at: &AtTime) -> Self {
        Self {
            started_on: None,
            ended_on: Some(*at),
            status: WorkerStatus::Stopped,
        }
    }
}

#[ta_objects::Dao]
#[dao(update, sql_table = "workers")]
pub struct UpdateWorkerMessageStatusDB {
    pub message_status: WorkerMessageStatus,
}

impl UpdateWorkerMessageStatusDB {
    pub fn locked() -> Self {
        Self {
            message_status: WorkerMessageStatus::Locked,
        }
    }

    pub fn unlocked() -> Self {
        Self {
            message_status: WorkerMessageStatus::Unlocked,
        }
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
impl TryFrom<&UpdateWorkerExecution> for UpdateWorkerDB {
    type Error = TdError;
    fn try_from(src: &UpdateWorkerExecution) -> Result<Self, Self::Error> {
        Ok(UpdateWorkerDB {
            started_on: src.started_on,
            ended_on: src.ended_on,
            status: src.status.clone().into(),
        })
    }
}

#[ta_objects::UrlParam]
pub struct WorkerParam {
    pub worker: WorkerId,
}

#[ta_objects::UrlParam]
pub struct WorkerIdParam {
    pub worker_id: WorkerId,
}

#[derive(TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi, Default)]
pub enum LogsExtension {
    #[default]
    #[typed_enum(rename = "all")]
    All,
    #[typed_enum(rename = "err")]
    Err,
    #[typed_enum(rename = "fn")]
    Fn,
    #[typed_enum(rename = "out")]
    Out,
    #[typed_enum(rename = "td")]
    Td,
}

impl LogsExtension {
    pub fn glob_pattern(&self) -> String {
        match self {
            LogsExtension::All => format!("*.{LOG_EXTENSION}"), // Note that it might get more than the "known" log files.
            LogsExtension::Err => format!("{ERR_LOG_FILE_NAME}*.{LOG_EXTENSION}"),
            LogsExtension::Fn => format!("{FN_LOG_FILE_NAME}*.{LOG_EXTENSION}"),
            LogsExtension::Out => format!("{OUT_LOG_FILE_NAME}*.{LOG_EXTENSION}"),
            LogsExtension::Td => format!("{TD_LOG_FILE_NAME}*.{LOG_EXTENSION}"),
        }
    }

    #[cfg(feature = "test-utils")]
    pub fn files(&self, rotations: usize) -> Vec<std::path::PathBuf> {
        fn log_file(name: &str, rotation: usize) -> std::path::PathBuf {
            let ext = if rotation == 1 {
                format!(".{LOG_EXTENSION}")
            } else {
                format!("_{}.{}", rotation - 1, LOG_EXTENSION)
            };
            format!("{name}{ext}").into()
        }

        let mut log_files = Vec::new();
        for rotation in 1..=rotations {
            match self {
                LogsExtension::All => {
                    log_files.push(log_file(ERR_LOG_FILE_NAME, rotation));
                    log_files.push(log_file(FN_LOG_FILE_NAME, rotation));
                    log_files.push(log_file(OUT_LOG_FILE_NAME, rotation));
                    log_files.push(log_file(TD_LOG_FILE_NAME, rotation));
                }
                LogsExtension::Err => log_files.push(log_file(ERR_LOG_FILE_NAME, rotation)),
                LogsExtension::Fn => log_files.push(log_file(FN_LOG_FILE_NAME, rotation)),
                LogsExtension::Out => log_files.push(log_file(OUT_LOG_FILE_NAME, rotation)),
                LogsExtension::Td => log_files.push(log_file(TD_LOG_FILE_NAME, rotation)),
            }
        }

        log_files
    }
}

#[ta_objects::QueryParam]
pub struct LogsExtensionParam {
    #[serde(default = "extension_default")]
    pub extension: Vec<LogsExtension>,
}

fn extension_default() -> Vec<LogsExtension> {
    vec![LogsExtension::All]
}

/// Empty means all retries available.
#[ta_objects::QueryParam]
pub struct LogsRetryParam {
    #[serde(default)]
    pub retry: Vec<LogsCastNumber>,
}
