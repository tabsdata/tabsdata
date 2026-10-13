//
// Copyright 2025 Tabsdata Inc.
//

use crate::dxo::yaml::WorkerOutput;
use crate::types::basic::{
    AtTime, CollectionId, CollectionName, ConnectionId, ConnectionKind, CronMask, DestinationWrite,
    Enabled, Ended, ExecutionId, ExecutionName, FunctionId, FunctionName, FunctionVersionId,
    LogicalFunctionRunId, LogsCastNumber, OfFunction, Patch, PhysicalFunctionRunId, ProjectId,
    ProjectName, Retry, SizeThreshold, Started, TimeoutThresholdMin, TransactionId, WorkerConfig,
    WorkerFailures, WorkerId, WorkerStatus, WorkerType,
};
use chrono::{DateTime, Utc};
use ta_objects::sql_condition::SqlCondition;
use ta_objects::{FromDao, TypedEnum, TypedOpenApi, TypedSerde, TypedSqlx, and, lte, or};
use td_common::datetime::Timestamp;
use td_common::execution_status::WorkerCallbackStatus;
use td_common::logging::LOG_EXTENSION;
use td_common::worker::{
    ERR_LOG_FILE_NAME, FN_LOG_FILE_NAME, OUT_LOG_FILE_NAME, ResponseMessagePayload,
    TD_LOG_FILE_NAME,
};
use td_error::TdError;

#[ta_objects::Dao]
#[dao(table = "workers", ops(insert), project_scoped)]
pub struct WorkerInsertDB {
    #[dao(pk)]
    pub id: WorkerId,
    pub collection_id: CollectionId,
    pub execution_id: Option<ExecutionId>,
    pub transaction_id: Option<TransactionId>,
    pub physical_function_run_id: Option<PhysicalFunctionRunId>,
    pub function_version_id: FunctionVersionId,
    pub message_status: WorkerMessageStatus,
    pub started_on: Option<Started<AtTime>>,
    pub ended_on: Option<Ended<AtTime>>,
    pub status: WorkerStatus,

    pub worker_type: WorkerType,
    pub worker_enabled: Enabled,
    pub worker_config: Option<WorkerConfig>,
}

/// The row as the database holds it: `project_id` is the `project_isolation`
/// policy's, defaulted from the connection the insert ran on.
#[ta_objects::Dao]
#[dao(table = "workers", ops(select), project_scoped)]
pub struct WorkerDB {
    #[dao(flatten)]
    pub base: WorkerInsertDB,
    pub project_id: ProjectId,
    /// A failed stream's failures in a row, and when its next restart is due:
    /// none means it is not restarted. Unused by other workers.
    pub failures: WorkerFailures,
    pub retry_at: Option<Retry<AtTime>>,
    /// When the attempt began a destination write a re-run would repeat.
    pub destination_write_on: Option<DestinationWrite<AtTime>>,
}

impl WorkerDB {
    /// On the base table so the readiness read can be claimed —
    /// `workers__with_names` is a view, not lockable.
    pub fn triggers_to_schedule(worker_type: WorkerType) -> impl SqlCondition<Self> {
        and!(Enabled::TRUE, worker_type, WorkerStatus::Scheduled)
    }

    /// See [`triggers_to_schedule`](Self::triggers_to_schedule) for why this
    /// is on the base table.
    pub fn triggers_to_stop(worker_type: WorkerType) -> impl SqlCondition<Self> {
        and!(Enabled::TRUE, worker_type, WorkerStatus::StopScheduled)
    }
}

#[ta_objects::Dao]
#[dao(view = "workers__with_names", project_scoped)]
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
    pub function_id: FunctionId,
    pub function: FunctionName,
    pub enabled: OfFunction<Enabled>,
    // Patch the attempt's PFR ran (NULL = head), from physical_function_runs.
    pub function_version_patch_id: Option<Patch<FunctionVersionId>>,
    pub project: Option<ProjectName>,
}

impl WorkerDBWithNames {
    pub fn enabled_triggers() -> impl SqlCondition<Self> {
        and!(
            Enabled::TRUE,
            or!(WorkerType::CronTrigger, WorkerType::StageTrigger)
        )
    }

    /// In flight = started and not stopping. Cron rows can park at
    /// Done/Failed via fire-side reports and recovery must keep owning them.
    pub fn cron_triggers_in_flight() -> impl SqlCondition<Self> {
        and!(
            Enabled::TRUE,
            WorkerType::CronTrigger,
            or!(
                WorkerStatus::Requested,
                WorkerStatus::Running,
                WorkerStatus::Done,
                WorkerStatus::Failed
            )
        )
    }

    /// A failed stage worker is restarted once its retry is due, so it is
    /// left out here and listed by [`stage_triggers_due`](Self::stage_triggers_due).
    pub fn stage_triggers_in_flight() -> impl SqlCondition<Self> {
        and!(
            Enabled::TRUE,
            WorkerType::StageTrigger,
            or!(
                WorkerStatus::Requested,
                WorkerStatus::Running,
                WorkerStatus::Done
            )
        )
    }

    /// Failed stage workers whose restart is due at `at`.
    pub fn stage_triggers_due(at: &AtTime) -> impl SqlCondition<Self> {
        and!(
            Enabled::TRUE,
            WorkerType::StageTrigger,
            WorkerStatus::Failed,
            lte!(Retry::from(*at))
        )
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
    pub started_on: Option<Started<AtTime>>,
    #[dto(list(filter, order_by))]
    pub ended_on: Option<Ended<AtTime>>,
    #[dto(list(filter, order_by))]
    pub status: WorkerStatus,
    pub failures: WorkerFailures,
    pub retry_at: Option<Retry<AtTime>>,

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
    pub function_version_patch_id: Option<Patch<FunctionVersionId>>,
    #[dto(list(filter, filter_like, order_by))]
    pub project: Option<ProjectName>,
}

impl FromDao for Worker {
    type Dao = WorkerDBWithNames;

    fn from_dao(dao: WorkerDBWithNames) -> Result<Self, TdError> {
        Ok(Self {
            id: dao.base.base.id,
            project_id: dao.base.project_id,
            collection_id: dao.base.base.collection_id,
            execution_id: dao.base.base.execution_id,
            transaction_id: dao.base.base.transaction_id,
            physical_function_run_id: dao.base.base.physical_function_run_id,
            function_version_id: dao.base.base.function_version_id,
            message_status: dao.base.base.message_status,
            started_on: dao.base.base.started_on,
            ended_on: dao.base.base.ended_on,
            status: dao.base.base.status,
            failures: dao.base.failures,
            retry_at: dao.base.retry_at,
            logical_function_run_id: dao.logical_function_run_id,
            collection: dao.collection,
            execution: dao.execution,
            function: dao.function,
            worker_type: dao.base.base.worker_type,
            function_version_patch_id: dao.function_version_patch_id,
            project: dao.project,
        })
    }
}

pub type CallbackRequest = ResponseMessagePayload<WorkerOutput>;

#[ta_objects::Dlo]
pub struct WorkerExecutionUpdate {
    pub started_on: Option<Started<AtTime>>,
    pub ended_on: Option<Ended<AtTime>>,
    pub status: WorkerCallbackStatus,
}

impl TryFrom<&CallbackRequest> for WorkerExecutionUpdate {
    type Error = TdError;

    fn try_from(value: &CallbackRequest) -> Result<Self, Self::Error> {
        let ended_on = value
            .end
            .map(|v| {
                let ts = Timestamp::try_from(v)?;
                Ok::<_, TdError>(Ended::from(AtTime::from(ts)))
            })
            .transpose()?;

        Ok(WorkerExecutionUpdate {
            started_on: value
                .start
                .map(|v| {
                    let ts = Timestamp::try_from(v)?;
                    Ok::<_, TdError>(Started::from(AtTime::from(ts)))
                })
                .transpose()?,
            ended_on,
            status: value.status.clone(),
        })
    }
}

#[ta_objects::Dao]
#[dao(table = "workers", ops(update(WorkerDB)), project_scoped)]
pub struct WorkerUpdateDB {
    #[dao(immutable)]
    pub started_on: Option<Started<AtTime>>,
    pub ended_on: Option<Ended<AtTime>>,
    pub status: WorkerStatus,
}

impl WorkerUpdateDB {
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
            ended_on: Some((*at).into()),
            status: WorkerStatus::Stopped,
        }
    }

    pub fn failed_at(at: &AtTime) -> Self {
        Self {
            started_on: None,
            ended_on: Some((*at).into()),
            status: WorkerStatus::Failed,
        }
    }
}

/// A cron or stage worker carried to its function's frozen version, which
/// does not fire.
#[ta_objects::Dao]
#[dao(table = "workers", ops(update(WorkerDB)), project_scoped)]
pub struct WorkerFreezeUpdateDB {
    pub function_version_id: FunctionVersionId,
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
#[ta_objects::Dao]
#[dao(table = "workers", ops(update(WorkerDB)), project_scoped)]
pub struct WorkerDestinationWriteUpdateDB {
    pub destination_write_on: DestinationWrite<AtTime>,
}

/// When a failed stream restarts.
#[ta_objects::Dao]
#[dao(table = "workers", ops(update(WorkerDB)), project_scoped)]
pub struct WorkerRetryUpdateDB {
    pub failures: WorkerFailures,
    pub retry_at: Retry<AtTime>,
}

impl TryFrom<&WorkerExecutionUpdate> for WorkerUpdateDB {
    type Error = TdError;
    fn try_from(src: &WorkerExecutionUpdate) -> Result<Self, Self::Error> {
        Ok(WorkerUpdateDB {
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
