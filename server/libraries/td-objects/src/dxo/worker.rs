//
// Copyright 2025 Tabs Data Inc.
//

#[ta_objects::dxo]
mod definitions {
    use crate::dxo::function::FunctionDB;
    use crate::dxo::yaml::WorkerOutput;
    use crate::types::basic::{
        AtTime, CollectionId, CollectionName, CronMask, Enabled, ExecutionId, ExecutionName,
        FunctionName, FunctionVersionId, PhysicalFunctionRunId, SizeThreshold, TimeoutThresholdMin,
        TransactionId, WorkerConfig, WorkerId, WorkerMessageStatus, WorkerStatus, WorkerType,
    };
    use chrono::{DateTime, Utc};
    use ta_objects::Buildable;
    use ta_objects::{and, or};
    use td_common::datetime::Timestamp;
    use td_common::execution_status::WorkerCallbackStatus;
    use td_common::server::ResponseMessagePayload;
    use td_error::TdError;

    #[ta_objects::Dao]
    #[dao(sql_table = "workers")]
    #[dxo(builder(try_from = FunctionDB, skip_all))]
    pub struct WorkerDB {
        #[builder(default)]
        #[dxo(extractor)]
        pub id: WorkerId,
        #[dxo(builder(include), extractor)]
        pub collection_id: CollectionId,
        #[builder(default)]
        pub execution_id: Option<ExecutionId>,
        #[builder(default)]
        pub transaction_id: Option<TransactionId>,
        #[builder(default)]
        pub function_run_id: Option<PhysicalFunctionRunId>,
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
            StageTriggersToSchedule = and!(Enabled::TRUE, WorkerType::StageTrigger, WorkerStatus::Scheduled),
            StageTriggersToStop = and!(Enabled::TRUE, WorkerType::StageTrigger, WorkerStatus::StopScheduled),
        )
    )]
    #[inherits(WorkerDB)]
    pub struct WorkerDBWithNames {
        #[dxo(extractor)]
        pub id: WorkerId,

        pub collection: CollectionName,
        pub execution: Option<ExecutionName>,
        pub function: FunctionName,
        pub enabled: Enabled,
        pub worker_type: WorkerType,
    }

    #[ta_objects::Dto]
    #[dto(list(on = WorkerDBWithNames))]
    pub struct Worker {
        #[dto(list(pagination_by = "+", filter))]
        pub id: WorkerId,
        #[dto(list(filter, order_by))]
        pub collection_id: CollectionId,
        #[dto(list(filter, filter_like, order_by))]
        pub execution_id: Option<ExecutionId>,
        #[dto(list(filter, filter_like, order_by))]
        pub transaction_id: Option<TransactionId>,
        #[dto(list(filter, order_by))]
        pub function_run_id: Option<PhysicalFunctionRunId>,
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

        #[dto(list(filter, filter_like, order_by))]
        pub collection: CollectionName,
        #[dto(list(filter, filter_like))]
        pub execution: Option<ExecutionName>,
        #[dto(list(filter, filter_like, order_by))]
        pub function: FunctionName,
        #[dto(list(filter, order_by))]
        pub worker_type: WorkerType,
    }

    pub type CallbackRequest = ResponseMessagePayload<WorkerOutput>;

    #[ta_objects::Dlo]
    pub struct UpdateWorkerExecution {
        pub started_on: AtTime,
        pub ended_on: Option<AtTime>,
        pub status: WorkerCallbackStatus,
    }

    impl TryFrom<&CallbackRequest> for UpdateWorkerExecution {
        type Error = TdError;

        fn try_from(value: &CallbackRequest) -> Result<Self, Self::Error> {
            Ok(UpdateWorkerExecution::builder()
                .try_started_on(Timestamp::try_from(value.start)?)?
                .ended_on(
                    value
                        .end
                        .map(|v| {
                            let ts = Timestamp::try_from(v)?;
                            AtTime::try_from(ts)
                        })
                        .transpose()?,
                )
                .status(value.status.clone())
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
