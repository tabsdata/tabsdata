//
// Copyright 2025 Tabs Data Inc.
//

#[ta_objects::dxo]
mod definitions {
    use crate::dxo::execution::ExecutionDB;
    use crate::dxo::worker::UpdateWorkerExecution;
    use crate::types::basic::{
        AtTime, BundleId, CollectionId, CollectionName, DataLocation, ExecutionId, ExecutionName,
        FunctionId, FunctionName, FunctionRunId, FunctionRunStatus, FunctionVersionId,
        StorageVersion, TransactionId, Trigger, TriggeredOn, UserId, UserName,
    };
    use td_error::TdError;

    #[ta_objects::Dao]
    #[dao(
        sql_table = "function_runs",
        order_by(col = "triggered_on", dir = "desc")
    )]
    #[dxo(builder(try_from = ExecutionDB, skip_all))]
    pub struct FunctionRunDB {
        #[builder(default)]
        pub id: FunctionRunId,
        pub collection_id: CollectionId, // this is not the ExecutionDB function_version_id, as that's the trigger
        #[dxo(extractor)]
        pub function_id: FunctionId, // added in v4
        #[dxo(extractor)]
        pub function_version_id: FunctionVersionId, // this is not the ExecutionDB function_version_id, as that's the trigger
        #[dxo(extractor, builder(field = "id"))]
        pub execution_id: ExecutionId,
        #[dxo(extractor)]
        pub transaction_id: TransactionId,
        #[dxo(builder(include))]
        pub triggered_on: TriggeredOn,
        #[dxo(builder(include))]
        pub triggered_by_id: UserId,
        pub trigger: Trigger,
        #[builder(default)]
        pub started_on: Option<AtTime>,
        #[builder(default)]
        pub ended_on: Option<AtTime>,
        #[builder(default = FunctionRunStatus::Scheduled)]
        pub status: FunctionRunStatus,
    }

    #[ta_objects::Dao]
    #[dao(sql_table = "function_runs__with_names")]
    #[inherits(FunctionRunDB)]
    pub struct FunctionRunDBWithNames {
        pub name: FunctionName,
        pub collection: CollectionName,
        pub execution: Option<ExecutionName>,
        pub execution_function_version_id: FunctionVersionId,
        pub triggered_by: UserName,
    }

    #[ta_objects::Dao]
    #[dao(sql_table = "function_runs__to_execute")]
    #[inherits(FunctionRunDBWithNames)]
    pub struct FunctionRunToExecuteDB {
        #[dxo(extractor)]
        pub id: FunctionRunId,

        pub data_location: DataLocation,
        pub storage_version: StorageVersion,
        pub bundle_id: BundleId,
        pub defined_on: AtTime,
    }

    #[ta_objects::Dao]
    #[dao(sql_table = "function_runs__to_commit")]
    #[inherits(FunctionRunDB)]
    pub struct FunctionRunToCommitDB {}

    #[ta_objects::Dto]
    #[dto(list(on = FunctionRunDBWithNames))]
    pub struct FunctionRun {
        #[dto(list(filter, order_by))]
        pub id: FunctionRunId,
        #[dto(list(filter, order_by))]
        pub collection_id: CollectionId,
        #[dto(list(filter, order_by))]
        pub function_version_id: FunctionVersionId,
        #[dto(list(filter, filter_like, order_by))]
        pub execution_id: ExecutionId,
        #[dto(list(filter, filter_like, order_by))]
        pub transaction_id: TransactionId,
        #[dto(list(pagination_by = "+", filter))]
        pub triggered_on: TriggeredOn,
        pub trigger: Trigger,
        #[dto(list(filter))]
        pub started_on: Option<AtTime>,
        #[dto(list(filter))]
        pub ended_on: Option<AtTime>,
        #[dto(list(filter, order_by))]
        pub status: FunctionRunStatus,

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

    #[ta_objects::Dao]
    #[dao(update, sql_table = "function_runs")]
    #[dxo(builder(try_from = UpdateWorkerExecution))]
    pub struct UpdateFunctionRunDB {
        #[dao(immutable)]
        #[builder(default)]
        pub started_on: Option<AtTime>,
        #[builder(default)]
        pub ended_on: Option<AtTime>,
        pub status: FunctionRunStatus,
    }

    impl UpdateFunctionRunDB {
        pub fn scheduled() -> Result<Self, TdError> {
            Ok(Self {
                started_on: None,
                ended_on: None,
                status: FunctionRunStatus::Scheduled,
            })
        }

        pub async fn run_requested() -> Result<Self, TdError> {
            Ok(Self {
                started_on: None,
                ended_on: None,
                status: FunctionRunStatus::RunRequested,
            })
        }

        pub async fn recover() -> Result<Self, TdError> {
            Ok(Self {
                started_on: None,
                ended_on: None,
                status: FunctionRunStatus::ReScheduled,
            })
        }

        pub async fn cancel() -> Result<Self, TdError> {
            Ok(Self {
                started_on: None,
                ended_on: Some(AtTime::now()),
                status: FunctionRunStatus::Canceled,
            })
        }
    }

    #[ta_objects::Dao]
    #[dao(sql_table = "function_runs")]
    pub struct CommitFunctionRunDB {
        pub status: FunctionRunStatus,
    }

    impl Default for CommitFunctionRunDB {
        fn default() -> Self {
            Self {
                status: FunctionRunStatus::Committed,
            }
        }
    }
}
