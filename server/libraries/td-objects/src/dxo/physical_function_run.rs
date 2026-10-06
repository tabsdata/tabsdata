//
// Copyright 2025 Tabsdata Inc.
//

use crate::dxo::scd2_interval::Scd2Interval;
use crate::dxo::worker::WorkerExecutionUpdate;
use crate::types::basic::Scd2ValidFrom;
use crate::types::basic::{
    AtTime, BasePath, BundleId, CollectionId, CollectionName, ConnectionId, ConnectionKind,
    Decorator, Dispatched, Ended, ExecutionId, ExecutionName, FunctionId, FunctionName,
    FunctionVersionId, InputIdx, LogicalFunctionRunId, OfExecution, Patch, PhysicalFunctionRunId,
    PhysicalFunctionRunStatus, ProjectId, ProjectName, StageId, Started, StorageVersion,
    TransactionId, Trigger, UserId, UserLabel,
};
use ta_objects::FromDao;
use td_error::TdError;

/// One attempt at a logical function run. Attempts are the run's SCD2
/// versions, ordered by `triggered_on`: the newest is the current one.
#[ta_objects::Dao]
#[dao(table = "physical_function_runs", ops(insert), scd2, project_scoped)]
pub struct PhysicalFunctionRunInsertDB {
    #[dao(pk)]
    pub id: PhysicalFunctionRunId,
    pub collection_id: CollectionId,
    pub function_id: FunctionId,
    // The fvid of the function being run — distinct from
    // `ExecutionDB.function_version_id`, which is the trigger's fvid.
    pub function_version_id: FunctionVersionId,
    pub execution_id: ExecutionId,
    pub transaction_id: TransactionId,
    #[dao(partition_by)]
    pub logical_function_run_id: LogicalFunctionRunId,
    pub triggered_by_id: UserId,
    pub trigger: Trigger,
    // The run's effective prefix (function-version override, else the
    // collection's), frozen at mint so the run's own content survives a
    // later base_path move. Same resolution its output TDVs stamp.
    pub base_path: BasePath,
    // Stamped at dispatch; basis for the stuck-run sweep.
    pub dispatched_on: Option<Dispatched<AtTime>>,
    pub started_on: Option<Started<AtTime>>,
    pub ended_on: Option<Ended<AtTime>>,

    // Per-attempt audit status. Canonical flow-control status lives
    // on the LFR (sticky-Committed).
    pub status: PhysicalFunctionRunStatus,
    // Patch resolved at mint, if any. NULL → head ran verbatim.
    pub function_version_patch_id: Option<Patch<FunctionVersionId>>,
}

#[ta_objects::Dao]
#[dao(table = "physical_function_runs", ops(select), scd2, project_scoped)]
pub struct PhysicalFunctionRunDB {
    #[dao(flatten)]
    pub base: PhysicalFunctionRunInsertDB,
    pub project_id: ProjectId,
    #[dao(flatten, order_by)]
    pub interval: Scd2Interval,
}

#[ta_objects::Dao]
#[dao(view = "physical_function_runs__with_names", scd2, project_scoped)]
pub struct PhysicalFunctionRunDBWithNames {
    #[dao(flatten)]
    pub base: PhysicalFunctionRunDB,
    pub name: FunctionName,
    pub collection: CollectionName,
    pub project: Option<ProjectName>,
    pub connection: Option<ConnectionKind>,
    pub execution: Option<ExecutionName>,
    pub execution_function_version_id: OfExecution<FunctionVersionId>,
    pub storage_version: StorageVersion,
    pub triggered_by: UserLabel,
}

#[ta_objects::Dlo]
#[derive(sqlx::FromRow)] // not a SelectDao, just a logical layer built from combining them
pub struct PhysicalFunctionRunToExecute {
    #[sqlx(flatten)]
    pub base: PhysicalFunctionRunDBWithNames,
    pub bundle_id: BundleId,
    pub defined_on: AtTime,
    pub decorator: Decorator,
    // The collection's connection (NULL for non-source/destination),
    // joined in function_run_to_execute_select for the dispatch request.
    pub connection_id: Option<ConnectionId>,
    // Joined base paths (see function_run_to_execute_select): the effective
    // bundle's stamped prefix, and the function version's stage identity +
    // stamped stage prefix. The run's own prefix is `base.base.base_path`.
    pub bundle_base_path: BasePath,
    pub stage_id: StageId,
    pub stage_base_path: BasePath,
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
    #[dto(list(filter, order_by, col = Scd2ValidFrom))]
    pub triggered_on: AtTime,
    pub trigger: Trigger,
    #[dto(list(filter))]
    pub started_on: Option<Started<AtTime>>,
    #[dto(list(filter))]
    pub ended_on: Option<Ended<AtTime>>,
    #[dto(list(filter, order_by))]
    pub status: PhysicalFunctionRunStatus,

    #[dto(list(filter, filter_like, order_by))]
    pub name: FunctionName,
    #[dto(list(filter, filter_like, order_by))]
    pub collection: CollectionName,
    #[dto(list(filter, filter_like))]
    pub execution: Option<ExecutionName>,
    #[dto(list(filter, order_by))]
    pub execution_function_version_id: OfExecution<FunctionVersionId>,
    pub triggered_by: UserLabel,
    // TODO exception info
    // pub kind: Option<String>,
    // pub message: Option<String>,
    // pub error_code: Option<String>,
    // pub exit_status: i32,
}

impl FromDao for PhysicalFunctionRun {
    type Dao = PhysicalFunctionRunDBWithNames;

    fn from_dao(dao: PhysicalFunctionRunDBWithNames) -> Result<Self, TdError> {
        Ok(Self {
            id: dao.base.base.id,
            collection_id: dao.base.base.collection_id,
            function_version_id: dao.base.base.function_version_id,
            execution_id: dao.base.base.execution_id,
            transaction_id: dao.base.base.transaction_id,
            triggered_on: dao.base.interval.valid_from.into(),
            trigger: dao.base.base.trigger,
            started_on: dao.base.base.started_on,
            ended_on: dao.base.base.ended_on,
            status: dao.base.base.status,
            name: dao.name,
            collection: dao.collection,
            execution: dao.execution,
            execution_function_version_id: dao.execution_function_version_id,
            triggered_by: dao.triggered_by,
        })
    }
}

// Per-attempt timing + status on the concrete FR. The worker callback
// writes both via WorkerExecutionUpdate → builder. The LFR's
// canonical status is updated separately via LogicalFunctionRunStatusUpdateDB
// (sticky-Committed gate applies there, not here).
#[ta_objects::Dao]
#[dao(
    table = "physical_function_runs",
    ops(update(PhysicalFunctionRunDB)),
    project_scoped
)]
pub struct PhysicalFunctionRunUpdateDB {
    pub dispatched_on: Option<Dispatched<AtTime>>,
    #[dao(immutable)]
    pub started_on: Option<Started<AtTime>>,
    pub ended_on: Option<Ended<AtTime>>,
    pub status: PhysicalFunctionRunStatus,
}

impl PhysicalFunctionRunUpdateDB {
    /// Status transition for the request-layer's dispatch step:
    /// `Requested → Dispatched`, stamped at `dispatched_on`. The same
    /// tx creates the WorkerDB row; the pair (PFR.status=Dispatched,
    /// WorkerDB exists) makes the PFR ineligible for the readiness query.
    pub fn dispatched(at: &AtTime) -> Self {
        Self {
            dispatched_on: Some((*at).into()),
            started_on: None,
            ended_on: None,
            status: PhysicalFunctionRunStatus::Dispatched,
        }
    }

    /// Terminalization without a worker callback: recover of a stuck
    /// `Requested` LFR abandons its in-flight attempt as `Failed` at
    /// `ended_on`, so late callbacks from it drop at the in-flight
    /// filter.
    pub fn failed_at(ended_on: &AtTime) -> Self {
        Self {
            dispatched_on: None,
            started_on: None,
            ended_on: Some((*ended_on).into()),
            status: PhysicalFunctionRunStatus::Failed,
        }
    }
}

impl TryFrom<&WorkerExecutionUpdate> for PhysicalFunctionRunUpdateDB {
    type Error = TdError;
    fn try_from(src: &WorkerExecutionUpdate) -> Result<Self, Self::Error> {
        Ok(PhysicalFunctionRunUpdateDB {
            dispatched_on: None,
            started_on: src.started_on,
            ended_on: src.ended_on,
            status: src.status.clone().try_into()?,
        })
    }
}

#[ta_objects::UrlParam]
pub struct FunctionRunParam {
    pub function_run: PhysicalFunctionRunId,
}

/// Names one stamped raw-data file of a run. The runner assigns the index as a
/// counter over every file it ingested, so it is not a parameter position.
#[ta_objects::UrlParam]
pub struct InputIdxParam {
    pub input_idx: InputIdx,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::basic::{AtTime, PhysicalFunctionRunStatus};

    #[tokio::test]
    async fn test_dispatched_helper_sets_status_and_dispatched_on() {
        let at = AtTime::now();
        let update = PhysicalFunctionRunUpdateDB::dispatched(&at);
        assert_eq!(update.status, PhysicalFunctionRunStatus::Dispatched);
        assert_eq!(update.dispatched_on, Some(Dispatched::from(at)));
        assert!(update.started_on.is_none());
        assert!(update.ended_on.is_none());
    }
}
