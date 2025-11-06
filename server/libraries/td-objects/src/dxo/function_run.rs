//
// Copyright 2025 Tabs Data Inc.
//

#[td_type::dxo]
pub mod defs {
    use crate::dxo::execution::defs::ExecutionDB;
    use crate::dxo::worker::defs::UpdateWorkerExecution;
    use crate::types::id::{
        BundleId, CollectionId, ExecutionId, FunctionRunId, FunctionVersionId, TransactionId,
        UserId,
    };
    use crate::types::string::{
        CollectionName, DataLocation, ExecutionName, FunctionName, StorageVersion, UserName,
    };
    use crate::types::timestamp::{AtTime, TriggeredOn};
    use crate::types::typed_enum::{FunctionRunStatus, Trigger};
    use td_error::TdError;

    #[td_type::Dao]
    #[dao(
        sql_table = "function_runs",
        order_by = "triggered_on",
        versioned(order_by = "triggered_on", partition_by = "id")
    )]
    #[td_type(builder(try_from = ExecutionDB, skip_all))]
    pub struct FunctionRunDB {
        #[builder(default)]
        pub id: FunctionRunId,
        pub collection_id: CollectionId, // this is not the ExecutionDB function_version_id, as that's the trigger
        #[td_type(extractor)]
        pub function_version_id: FunctionVersionId, // this is not the ExecutionDB function_version_id, as that's the trigger
        #[td_type(extractor, builder(field = "id"))]
        pub execution_id: ExecutionId,
        #[td_type(extractor)]
        pub transaction_id: TransactionId,
        #[td_type(builder(include))]
        pub triggered_on: TriggeredOn,
        #[td_type(builder(include))]
        pub triggered_by_id: UserId,
        pub trigger: Trigger,
        #[builder(default)]
        pub started_on: Option<AtTime>,
        #[builder(default)]
        pub ended_on: Option<AtTime>,
        #[builder(default = FunctionRunStatus::Scheduled)]
        pub status: FunctionRunStatus,
    }

    #[td_type::Dao]
    #[dao(sql_table = "function_runs__with_names")]
    #[inherits(FunctionRunDB)]
    pub struct FunctionRunDBWithNames {
        pub name: FunctionName,
        pub collection: CollectionName,
        pub execution: Option<ExecutionName>,
        pub triggered_by: UserName,
    }

    #[td_type::Dao]
    #[dao(sql_table = "function_runs__to_execute")]
    #[inherits(FunctionRunDBWithNames)]
    pub struct FunctionRunToExecuteDB {
        #[td_type(extractor)]
        pub id: FunctionRunId,

        pub data_location: DataLocation,
        pub storage_version: StorageVersion,
        pub bundle_id: BundleId,
    }

    #[td_type::Dao]
    #[dao(sql_table = "function_runs__to_commit")]
    #[inherits(FunctionRunDB)]
    pub struct FunctionRunToCommitDB {}

    #[td_type::Dto]
    #[dto(list(on = FunctionRunDBWithNames))]
    #[td_type(builder(try_from = FunctionRunDBWithNames))]
    pub struct FunctionRun {
        #[dto(list(filter, filter_like, order_by))]
        pub id: FunctionRunId,
        #[dto(list(filter, filter_like, order_by))]
        pub collection_id: CollectionId,
        pub function_version_id: FunctionVersionId,
        #[dto(list(filter, filter_like, order_by))]
        pub execution_id: ExecutionId,
        #[dto(list(filter, filter_like, order_by))]
        pub transaction_id: TransactionId,
        #[dto(list(pagination_by = "+", filter, filter_like))]
        pub triggered_on: TriggeredOn,
        pub trigger: Trigger,
        #[dto(list(filter, filter_like))]
        pub started_on: Option<AtTime>,
        #[dto(list(filter, filter_like))]
        pub ended_on: Option<AtTime>,
        #[dto(list(filter, filter_like, order_by))]
        pub status: FunctionRunStatus,

        #[dto(list(filter, filter_like, order_by))]
        pub name: FunctionName,
        #[dto(list(filter, filter_like, order_by))]
        pub collection: CollectionName,
        #[dto(list(filter, filter_like))]
        pub execution: Option<ExecutionName>,
        pub triggered_by: UserName,
        // TODO exception info
        // pub kind: Option<String>,
        // pub message: Option<String>,
        // pub error_code: Option<String>,
        // pub exit_status: i32,
    }

    #[td_type::Dao]
    #[dao(sql_table = "function_runs")]
    #[td_type(builder(try_from = UpdateWorkerExecution))]
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

    #[td_type::Dao]
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
