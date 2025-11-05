//
// Copyright 2025 Tabs Data Inc.
//

#[td_type::dxo]
pub mod defs {
    use crate::types::id::{
        CollectionId, ExecutionId, FunctionRunId, FunctionVersionId, TransactionId, WorkerId,
    };
    use crate::types::string::{CollectionName, ExecutionName, FunctionName};
    use crate::types::timestamp::AtTime;
    use crate::types::typed_enum::{WorkerMessageStatus, WorkerStatus};
    use crate::types::worker::FunctionOutput;
    use td_common::datetime::IntoDateTimeUtc;
    use td_common::execution_status::WorkerCallbackStatus;
    use td_common::server::ResponseMessagePayload;
    use td_error::TdError;

    #[td_type::Dao]
    #[dao(sql_table = "workers")]
    pub struct WorkerDB {
        #[builder(default)]
        #[td_type(extractor)]
        pub id: WorkerId,
        #[td_type(extractor)]
        pub collection_id: CollectionId,
        pub execution_id: ExecutionId,
        pub transaction_id: TransactionId,
        pub function_run_id: FunctionRunId,
        pub function_version_id: FunctionVersionId,
        pub message_status: WorkerMessageStatus,
        #[builder(default)]
        pub started_on: Option<AtTime>,
        #[builder(default)]
        pub ended_on: Option<AtTime>,
        pub status: WorkerStatus,
    }

    #[td_type::Dao]
    #[dao(sql_table = "workers__with_names")]
    pub struct WorkerDBWithNames {
        pub id: WorkerId,
        pub collection_id: CollectionId,
        pub execution_id: ExecutionId,
        pub transaction_id: TransactionId,
        pub function_run_id: FunctionRunId,
        pub function_version_id: FunctionVersionId,
        pub message_status: WorkerMessageStatus,
        pub started_on: Option<AtTime>,
        pub ended_on: Option<AtTime>,
        pub status: WorkerStatus,

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
        pub execution_id: ExecutionId,
        #[dto(list(filter, filter_like, order_by))]
        pub transaction_id: TransactionId,
        #[dto(list(filter, filter_like, order_by))]
        pub function_run_id: FunctionRunId,
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

    // TODO: Value is a placeholder, we need to define the actual type
    pub type CallbackRequest = ResponseMessagePayload<FunctionOutput>;

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
            Ok(Self::builder()
                .message_status(WorkerMessageStatus::Unlocked)
                .build()?)
        }
    }
}
