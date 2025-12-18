//
// Copyright 2025 Tabs Data Inc.
//

#[td_type::dxo]
mod definitions {
    use crate::dxo::function::FunctionDB;
    use crate::dxo::yaml::WorkerOutput;
    use crate::types::basic::{
        AtTime, CollectionId, CollectionName, CronMask, ExecutionId, ExecutionName, FunctionName,
        FunctionRunId, FunctionVersionId, SizeThreshold, TimeoutThresholdMin, TransactionId,
        WorkerConfig, WorkerId, WorkerMessageStatus, WorkerStatus, WorkerType, WorkerVersion,
    };
    use td_common::datetime::IntoDateTimeUtc;
    use td_common::execution_status::WorkerCallbackStatus;
    use td_common::server::ResponseMessagePayload;
    use td_error::TdError;

    #[td_type::Dao]
    #[dao(sql_table = "workers")]
    #[td_type(builder(try_from = FunctionDB, skip_all))]
    pub struct WorkerDB {
        #[builder(default)]
        #[td_type(extractor)]
        pub id: WorkerId,
        #[td_type(builder(include), extractor)]
        pub collection_id: CollectionId,
        #[builder(default)]
        pub execution_id: Option<ExecutionId>,
        #[builder(default)]
        pub transaction_id: Option<TransactionId>,
        #[builder(default)]
        pub function_run_id: Option<FunctionRunId>,
        #[td_type(builder(include, field = "id"))]
        pub function_version_id: FunctionVersionId,
        #[builder(default = "WorkerMessageStatus::None")]
        pub message_status: WorkerMessageStatus,
        #[builder(default)]
        pub started_on: Option<AtTime>,
        #[builder(default)]
        pub ended_on: Option<AtTime>,
        #[builder(default = "WorkerStatus::Scheduled")]
        pub status: WorkerStatus,

        pub worker_type: WorkerType,             // added in v4
        pub worker_config: Option<WorkerConfig>, // added in v4
    }

    impl WorkerDB {
        // TODO this can be converted to states if creating a stateful select without versioned
        pub async fn cron_to_schedule() -> Result<(WorkerType, WorkerStatus), TdError> {
            Ok((WorkerType::Cron, WorkerStatus::Scheduled))
        }

        pub async fn watchdog_to_schedule() -> Result<(WorkerType, WorkerStatus), TdError> {
            Ok((WorkerType::Watchdog, WorkerStatus::Scheduled))
        }
    }

    #[td_type::Dao]
    #[dao(sql_table = "workers__with_names")]
    #[inherits(WorkerDB)]
    pub struct WorkerDBWithNames {
        #[td_type(extractor)]
        pub id: WorkerId,

        pub collection: CollectionName,
        pub execution: Option<ExecutionName>,
        pub function: FunctionName,
    }

    #[td_type::Dto]
    #[td_type(builder(try_from = WorkerDBWithNames))]
    #[dto(list(on = WorkerDBWithNames))]
    pub struct Worker {
        #[dto(list(pagination_by = "+", filter, filter_like))]
        pub id: WorkerId,
        #[dto(list(filter, filter_like, order_by))]
        pub collection_id: CollectionId,
        #[dto(list(filter, filter_like, order_by))]
        pub execution_id: Option<ExecutionId>,
        #[dto(list(filter, filter_like, order_by))]
        pub transaction_id: Option<TransactionId>,
        #[dto(list(filter, filter_like, order_by))]
        pub function_run_id: Option<FunctionRunId>,
        #[dto(list(filter, filter_like, order_by))]
        pub function_version_id: FunctionVersionId,
        #[dto(list(filter, filter_like, order_by))]
        pub message_status: WorkerMessageStatus,
        #[dto(list(filter, filter_like, order_by))]
        pub started_on: Option<AtTime>,
        #[dto(list(filter, filter_like, order_by))]
        pub ended_on: Option<AtTime>,
        #[dto(list(filter, filter_like, order_by))]
        pub status: WorkerStatus,

        #[dto(list(filter, filter_like, order_by))]
        pub collection: CollectionName,
        #[dto(list(filter, filter_like))]
        pub execution: Option<ExecutionName>,
        #[dto(list(filter, filter_like, order_by))]
        pub function: FunctionName,
    }

    pub type CallbackRequest = ResponseMessagePayload<WorkerOutput>;

    #[td_type::Dlo]
    pub struct UpdateWorkerExecution {
        pub started_on: AtTime,
        pub ended_on: Option<AtTime>,
        pub status: WorkerCallbackStatus,
    }

    impl TryFrom<&CallbackRequest> for UpdateWorkerExecution {
        type Error = TdError;

        fn try_from(value: &CallbackRequest) -> Result<Self, Self::Error> {
            Ok(UpdateWorkerExecution::builder()
                .try_started_on(value.start.datetime_utc()?)?
                .ended_on(
                    value
                        .end
                        .map(|v| AtTime::try_from(v.datetime_utc()?))
                        .transpose()?,
                )
                .status(value.status.clone())
                .build()?)
        }
    }

    #[td_type::Dao]
    #[dao(sql_table = "workers")]
    #[td_type(builder(try_from = UpdateWorkerExecution))]
    pub struct UpdateWorkerDB {
        #[dao(immutable)]
        started_on: Option<AtTime>,
        ended_on: Option<AtTime>,
        status: WorkerStatus,
    }

    #[td_type::Dao]
    #[dao(sql_table = "workers")]
    pub struct UpdateWorkerMessageStatusDB {
        message_status: WorkerMessageStatus,
    }

    impl UpdateWorkerMessageStatusDB {
        pub fn unlocked() -> Result<Self, TdError> {
            Ok(Self {
                message_status: WorkerMessageStatus::Unlocked,
            })
        }
    }

    #[td_type::Dao]
    #[dao(sql_table = "workers")]
    pub struct UpdateWorkerStatusDB {
        status: WorkerStatus,
    }

    impl UpdateWorkerStatusDB {
        pub fn stopped() -> Result<Self, TdError> {
            Ok(Self {
                status: WorkerStatus::Stopped,
            })
        }

        pub async fn run_requested() -> Result<Self, TdError> {
            Ok(Self {
                status: WorkerStatus::RunRequested,
            })
        }
    }

    #[td_type::Dto]
    #[derive(Eq, PartialEq)]
    pub struct CronConfig {
        pub version: WorkerVersion,
        pub start: Option<AtTime>,
        pub end: Option<AtTime>,
        pub mask: CronMask,
    }

    #[td_type::Dto]
    #[derive(Eq, PartialEq)]
    pub struct WatchdogConfig {
        pub version: WorkerVersion,
        pub timeout_threshold_min: TimeoutThresholdMin,
        pub size_threshold: SizeThreshold,
        pub start: Option<AtTime>,
        pub end: Option<AtTime>,
    }
}
