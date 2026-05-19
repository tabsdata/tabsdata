//
// Copyright 2025 Tabs Data Inc.
//

#[ta_objects::dxo]
mod definitions {
    use crate::dxo::execution::ExecutionDB;
    use crate::dxo::worker::UpdateWorkerExecution;
    use crate::types::basic::{
        AtTime, BundleId, CollectionId, CollectionName, DataLocation, ExecutionId, ExecutionName,
        FunctionId, FunctionName, FunctionVersionId, PhysicalFunctionRunId,
        PhysicalFunctionRunStatus, StorageVersion, TransactionId, Trigger, TriggeredOn, UserId,
        UserName,
    };

    #[ta_objects::Dao]
    #[dao(
        sql_table = "physical_function_runs",
        order_by(col = "triggered_on", dir = "desc")
    )]
    #[dxo(builder(try_from = ExecutionDB, skip_all))]
    pub struct PhysicalFunctionRunDB {
        #[builder(default = "PhysicalFunctionRunId::new()")]
        pub id: PhysicalFunctionRunId,
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
    }

    #[ta_objects::Dao]
    #[dao(sql_table = "physical_function_runs__with_names")]
    #[inherits(PhysicalFunctionRunDB)]
    pub struct PhysicalFunctionRunDBWithNames {
        pub name: FunctionName,
        pub collection: CollectionName,
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

        pub data_location: DataLocation,
        pub storage_version: StorageVersion,
        pub bundle_id: BundleId,
        pub defined_on: AtTime,
    }

    #[ta_objects::Dto]
    #[dto(list(on = PhysicalFunctionRunDBWithNames))]
    pub struct PhysicalFunctionRun {
        #[dto(list(filter, order_by))]
        pub id: PhysicalFunctionRunId,
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
    #[ta_objects::Dao]
    #[dao(update, sql_table = "physical_function_runs")]
    #[dxo(builder(try_from = UpdateWorkerExecution))]
    pub struct UpdatePhysicalFunctionRunDB {
        #[dao(immutable)]
        #[builder(default)]
        pub started_on: Option<AtTime>,
        #[builder(default)]
        pub ended_on: Option<AtTime>,
        pub status: PhysicalFunctionRunStatus,
    }

    impl UpdatePhysicalFunctionRunDB {
        /// Status transition for the request-layer's dispatch step:
        /// `Requested → Dispatched`. The same tx creates the WorkerDB
        /// row; the pair (PFR.status=Dispatched, WorkerDB exists)
        /// makes the PFR ineligible for the readiness query.
        pub async fn dispatched() -> Result<Self, td_error::TdError> {
            Ok(Self {
                started_on: None,
                ended_on: None,
                status: PhysicalFunctionRunStatus::Dispatched,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::basic::PhysicalFunctionRunStatus;

    #[tokio::test]
    async fn test_dispatched_helper_sets_only_status() {
        let update = UpdatePhysicalFunctionRunDB::dispatched().await.unwrap();
        assert_eq!(update.status, PhysicalFunctionRunStatus::Dispatched);
        assert!(update.started_on.is_none());
        assert!(update.ended_on.is_none());
    }
}
