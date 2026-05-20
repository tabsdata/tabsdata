//
// Copyright 2025 Tabs Data Inc.
//

use td_common::execution_status::WorkerCallbackStatus;
use td_error::{ApiError, TdError, api_error};

#[ta_objects::typed_enum]
#[derive(Eq, PartialEq)]
pub enum Decorator {
    #[typed_enum(rename = "P")]
    Publisher,
    #[typed_enum(rename = "T")]
    Transformer,
    #[typed_enum(rename = "S")]
    Subscriber,
}

#[ta_objects::typed_enum]
#[derive(Eq, PartialEq)]
pub enum DependencyStatus {
    #[typed_enum(rename = "A")]
    Active,
    #[typed_enum(rename = "D")]
    Deleted,
}

/// Represents the status of an execution.
/// It is a summary of the statuses of all function runs within the execution.
#[ta_objects::typed_enum]
#[derive(Eq, PartialEq, Hash)]
pub enum ExecutionStatus {
    /// All function runs are scheduled.
    #[typed_enum(rename = "S")]
    Scheduled,
    /// At least one function run is still running (or able to do so).
    #[typed_enum(rename = "R")]
    Running,
    /// All functions are in a finished state, but at least one is Failed or OnHold.
    #[typed_enum(rename = "L")]
    Stalled,
    /// All function runs are finished (either successfully or with issues).
    #[typed_enum(rename = "F")]
    Finished,
    /// Unexpected status used as fallback.
    #[typed_enum(rename = "U")]
    Unexpected,
}

#[cfg_attr(doc, aquamarine::aquamarine)]
/// Represents the state of a function run.
///
/// ```mermaid
/// stateDiagram-v2
///     [*] --> Scheduled
///     Canceled --> [*]
///     Yanked --> [*]
///
///     Scheduled --> Requested
///     Requested --> Scheduled
///     Requested --> Running
///     Scheduled --> OnHold
///     Scheduled --> Canceled
///     Running --> Done
///     Running --> Error
///     Running --> Failed
///     Running --> Canceled
///     OnHold --> Canceled
///     OnHold --> Scheduled
///     Error --> Running
///     Error --> Canceled
///     Failed --> ReScheduled
///     Failed --> Canceled
///     Done --> Canceled
///     Done --> Committed
///     Committed --> Yanked
/// ```
#[ta_objects::typed_enum]
#[derive(Eq, PartialEq, Hash)]
pub enum LogicalFunctionRunStatus {
    #[typed_enum(rename = "S")]
    Scheduled,
    #[typed_enum(rename = "RR")]
    Requested,
    #[typed_enum(rename = "RS")]
    ReScheduled,
    #[typed_enum(rename = "R")]
    Running,
    #[typed_enum(rename = "D")]
    Done,
    #[typed_enum(rename = "E")]
    Error,
    #[typed_enum(rename = "F")]
    Failed,
    #[typed_enum(rename = "H")]
    OnHold,
    #[typed_enum(rename = "C")]
    Committed,
    #[typed_enum(rename = "X")]
    Canceled,
    #[typed_enum(rename = "Y")]
    Yanked,
}

impl TryFrom<WorkerCallbackStatus> for LogicalFunctionRunStatus {
    type Error = TdError;

    fn try_from(value: WorkerCallbackStatus) -> Result<Self, TdError> {
        let status = match value {
            WorkerCallbackStatus::Running => LogicalFunctionRunStatus::Running,
            WorkerCallbackStatus::Done => LogicalFunctionRunStatus::Done,
            WorkerCallbackStatus::Error => LogicalFunctionRunStatus::Error,
            WorkerCallbackStatus::Failed => LogicalFunctionRunStatus::Failed,
            WorkerCallbackStatus::Stopped => Err(api_error!(
                ApiError::InputError,
                "Cannot stop a function run worker via worker callback."
            ))?,
        };
        Ok(status)
    }
}

/// Status of a logical pointer (LFR or LTDV) at any moment.
#[ta_objects::typed_enum]
#[derive(Eq, PartialEq, Hash)]
pub enum LogicalPointerStatus {
    /// The logical row exists but no concrete is bound yet.
    #[typed_enum(rename = "P")]
    Pending,
    /// Bound to a concrete row.
    #[typed_enum(rename = "B")]
    Bound,
    /// Not bound; only re-bindable via an explicit operator redirect.
    #[typed_enum(rename = "V")]
    Voided,
}

/// Reason recorded on every pointer-changing event in the logical history.
#[ta_objects::typed_enum]
#[derive(Eq, PartialEq, Hash)]
pub enum LogicalPointerReason {
    /// Logical row created at plan-build time; status begins `Pending`.
    #[typed_enum(rename = "IM")]
    InitialMint,
    /// Auto-bind at dispatch: `Pending → Bound`.
    #[typed_enum(rename = "ID")]
    InitialDispatch,
    /// Retroactive lazy-mint of a V1-born concrete TDV; LTDV is created
    /// already `Bound` with no dispatch event.
    #[typed_enum(rename = "IT")]
    InitialMaterialization,
    /// Void path used by the cancel pipeline; only voids `Pending` rows.
    #[typed_enum(rename = "TC")]
    TransactionCancelManual,
    /// Void path triggered by upstream cancellation.
    #[typed_enum(rename = "UC")]
    UpstreamCancellationManual,
    /// Explicit operator-driven repointing. The only path to
    /// `Voided → Bound`, `Bound → Bound(different concrete)`, or any
    /// rebind the auto path refuses. Always carries the operator's
    /// actual user id in `set_by_id`, never `UserId::admin`.
    #[typed_enum(rename = "OR")]
    OperatorRedirect,
    /// Operator force-void of a `Bound` (or `Pending`) row — deliberately
    /// abandons a binding without choosing a replacement. Distinct from
    /// `TransactionCancelManual`, which only voids `Pending` rows.
    #[typed_enum(rename = "OV")]
    OperatorForceVoid,
    /// Worker-finish redirect on a replay attempt's success. LFR pointer
    /// was `Bound(PFR_old)` while the replay PFR ran; on its `Done`
    /// callback `redirect_at_finish` appends `Bound(PFR_new)` with this
    /// reason. Paired with `Bound` status.
    #[typed_enum(rename = "RR")]
    ReplayRedirect,
}

/// Per-attempt status on `physical_function_runs`. Independent of
/// [`LogicalFunctionRunStatus`] (LFR/LTDV-canonical, sticky-Committed).
///
/// ```text
///   Requested ─► Dispatched ─► Running ─► Done | Error | Failed
///   Failed    ─► Requested                       (operator recover)
/// ```
#[ta_objects::typed_enum]
#[derive(Eq, PartialEq, Hash)]
pub enum PhysicalFunctionRunStatus {
    #[typed_enum(rename = "RR")]
    Requested,
    #[typed_enum(rename = "DP")]
    Dispatched,
    #[typed_enum(rename = "R")]
    Running,
    #[typed_enum(rename = "D")]
    Done,
    #[typed_enum(rename = "E")]
    Error,
    #[typed_enum(rename = "F")]
    Failed,
}

impl TryFrom<WorkerCallbackStatus> for PhysicalFunctionRunStatus {
    type Error = TdError;

    fn try_from(value: WorkerCallbackStatus) -> Result<Self, TdError> {
        let status = match value {
            WorkerCallbackStatus::Running => PhysicalFunctionRunStatus::Running,
            WorkerCallbackStatus::Done => PhysicalFunctionRunStatus::Done,
            WorkerCallbackStatus::Error => PhysicalFunctionRunStatus::Error,
            WorkerCallbackStatus::Failed => PhysicalFunctionRunStatus::Failed,
            WorkerCallbackStatus::Stopped => Err(api_error!(
                ApiError::InputError,
                "Cannot stop a function run worker via worker callback."
            ))?,
        };
        Ok(status)
    }
}

/// Project an LFR-side status onto a "natural" PFR-side status. Used by
/// test seeds and any read-side view that surfaces a unified status
/// field. Production code sets the two statuses independently — LFR
/// transitions via the canonical state machine, PFR via worker
/// callbacks + the dispatch transition.
///
/// Mapping rationale: shared variants pass through; LFR-only sticky
/// terminals collapse to the PFR terminal that implied the underlying
/// attempt outcome (Committed/Yanked → Done, Canceled → Failed); LFR
/// pre-flight states map to Requested.
impl From<LogicalFunctionRunStatus> for PhysicalFunctionRunStatus {
    fn from(s: LogicalFunctionRunStatus) -> Self {
        match s {
            LogicalFunctionRunStatus::Running => PhysicalFunctionRunStatus::Running,
            LogicalFunctionRunStatus::Done
            | LogicalFunctionRunStatus::Committed
            | LogicalFunctionRunStatus::Yanked => PhysicalFunctionRunStatus::Done,
            LogicalFunctionRunStatus::Error => PhysicalFunctionRunStatus::Error,
            LogicalFunctionRunStatus::Failed | LogicalFunctionRunStatus::Canceled => {
                PhysicalFunctionRunStatus::Failed
            }
            LogicalFunctionRunStatus::Scheduled
            | LogicalFunctionRunStatus::Requested
            | LogicalFunctionRunStatus::ReScheduled
            | LogicalFunctionRunStatus::OnHold => PhysicalFunctionRunStatus::Requested,
        }
    }
}

impl From<&LogicalFunctionRunStatus> for PhysicalFunctionRunStatus {
    fn from(s: &LogicalFunctionRunStatus) -> Self {
        s.clone().into()
    }
}

#[ta_objects::typed_enum]
#[derive(Eq, PartialEq)]
pub enum FunctionStatus {
    #[typed_enum(rename = "A")]
    Active,
    #[typed_enum(rename = "F")]
    Frozen,
    #[typed_enum(rename = "D")]
    Deleted,
}

#[ta_objects::typed_enum]
pub enum GrantType {
    #[typed_enum(rename = "refresh_token")]
    RefreshToken,
}

#[ta_objects::typed_enum]
pub enum GlobalStatus {
    #[typed_enum(rename = "S")]
    Scheduled,
    #[typed_enum(rename = "R")]
    Running,
    #[typed_enum(rename = "L")]
    Stalled,
    #[typed_enum(rename = "F")]
    Finished,
    #[typed_enum(rename = "U")]
    Unknown,
}

#[ta_objects::typed_enum]
#[derive(Eq, PartialEq)]
pub enum PermissionEntityType {
    #[typed_enum(rename = "s")]
    System,
    #[typed_enum(rename = "c")]
    Collection,
}

#[ta_objects::typed_enum]
#[derive(Eq, PartialEq)]
pub enum PermissionType {
    #[typed_enum(rename = "sa")]
    SysAdmin,
    #[typed_enum(rename = "ss")]
    SecAdmin,
    #[typed_enum(rename = "ca")]
    CollectionAdmin,
    #[typed_enum(rename = "cd")]
    CollectionDev,
    #[typed_enum(rename = "cx")]
    CollectionExec,
    #[typed_enum(rename = "cr")]
    CollectionRead,
}

impl PermissionType {
    pub fn on_entity_type(&self) -> PermissionEntityType {
        if matches!(self, &Self::SysAdmin | &Self::SecAdmin) {
            PermissionEntityType::System
        } else {
            PermissionEntityType::Collection
        }
    }
}

/// Kind of replay-audit row written by the replay / delete-replay
/// endpoints to `logical_function_run_replays`.
#[ta_objects::typed_enum]
#[derive(Eq, PartialEq, Hash)]
pub enum ReplayKind {
    /// Operator created a fresh `PhysicalFunctionRunDB` and redirected the LFR
    /// pointer to it.
    #[typed_enum(rename = "R")]
    Replay,
    /// Operator undid the most-recent replay; LFR rebound to the prior
    /// concrete FR.
    #[typed_enum(rename = "B")]
    Rollback,
    /// Operator undid the LFR's only binding; LFR transitioned to Voided.
    #[typed_enum(rename = "V")]
    Void,
}

#[ta_objects::typed_enum]
#[derive(Eq, PartialEq)]
pub enum SessionStatus {
    #[typed_enum(rename = "a")]
    Active,
    #[typed_enum(rename = "i_pc")]
    InvalidPasswordChange,
    #[typed_enum(rename = "i_nt")]
    InvalidNewToken,
    #[typed_enum(rename = "i_rc")]
    InvalidRoleChange,
    #[typed_enum(rename = "i_l")]
    InvalidLogout,
    #[typed_enum(rename = "i_ud")]
    InvalidUserDisabled,
}

/// Kept 1:1 with `td_storage::location::StorageLocation`.
#[ta_objects::typed_enum]
#[derive(Default, Eq, PartialEq, Hash)]
pub enum StorageVersion {
    #[default]
    V1,
}

#[ta_objects::typed_enum]
#[derive(Eq, PartialEq)]
pub enum TableStatus {
    #[typed_enum(rename = "A")]
    Active,
    #[typed_enum(rename = "F")]
    Frozen,
    #[typed_enum(rename = "D")]
    Deleted,
}

#[ta_objects::typed_enum]
#[derive(Default)]
pub enum TableType {
    #[default]
    #[typed_enum(rename = "R")]
    Regular,
    #[typed_enum(rename = "DQD")]
    DQData,
    #[typed_enum(rename = "DQS")]
    DQSummary,
}

/// Represents the status of a transaction. Note transactions are atomic status wise.
/// So final status (e.g., Committed, Canceled, Yanked) means all function runs within the transaction
/// do have the same status.
/// It is a summary of the statuses of all function runs within the transaction.
#[ta_objects::typed_enum]
#[derive(Eq, PartialEq, Hash)]
pub enum TransactionStatus {
    /// All function runs are scheduled.
    #[typed_enum(rename = "S")]
    Scheduled,
    /// At least one function run is still running (or able to do so).
    #[typed_enum(rename = "R")]
    Running,
    /// All functions are in a finished state, but at least one is Failed or OnHold.
    #[typed_enum(rename = "L")]
    Stalled,
    /// All function runs are Committed.
    #[typed_enum(rename = "C")]
    Committed,
    /// All function runs are Canceled.
    #[typed_enum(rename = "X")]
    Canceled,
    /// All function runs are Yanked.
    #[typed_enum(rename = "Y")]
    Yanked,
    /// Unexpected status used as fallback.
    #[typed_enum(rename = "U")]
    Unexpected,
}

#[ta_objects::typed_enum]
#[derive(Eq, PartialEq)]
pub enum Trigger {
    #[typed_enum(rename = "M")]
    Manual,
    #[typed_enum(rename = "D")]
    Dependency,
}

#[ta_objects::typed_enum]
#[derive(Default, Eq, PartialEq)]
pub enum TriggerStatus {
    #[default]
    #[typed_enum(rename = "A")]
    Active,
    #[typed_enum(rename = "F")]
    Frozen,
    #[typed_enum(rename = "D")]
    Deleted,
}

#[ta_objects::typed_enum]
#[derive(Eq, PartialEq)]
pub enum TriggerType {
    #[typed_enum(rename = "M")]
    Manual,
    #[typed_enum(rename = "CT")]
    CronTrigger,
    #[typed_enum(rename = "ST")]
    StageTrigger,
}

#[ta_objects::typed_enum]
#[derive(Eq, PartialEq)]
pub enum WorkerMessageStatus {
    #[typed_enum(rename = "N")]
    None,
    #[typed_enum(rename = "L")]
    Locked,
    #[typed_enum(rename = "U")]
    Unlocked,
}

#[ta_objects::typed_enum]
#[derive(Eq, PartialEq)]
pub enum WorkerStatus {
    #[typed_enum(rename = "S")]
    Scheduled,
    #[typed_enum(rename = "RR")]
    Requested,
    #[typed_enum(rename = "R")]
    Running,
    #[typed_enum(rename = "D")]
    Done,
    #[typed_enum(rename = "SPS")]
    StopScheduled,
    #[typed_enum(rename = "SPR")]
    StopRequested,
    #[typed_enum(rename = "SP")]
    Stopped,
    #[typed_enum(rename = "E")]
    Error,
    #[typed_enum(rename = "F")]
    Failed,
    #[typed_enum(rename = "X")]
    Canceled,
}

impl From<WorkerCallbackStatus> for WorkerStatus {
    fn from(value: WorkerCallbackStatus) -> Self {
        match value {
            WorkerCallbackStatus::Running => WorkerStatus::Running,
            WorkerCallbackStatus::Done => WorkerStatus::Done,
            WorkerCallbackStatus::Error => WorkerStatus::Error,
            WorkerCallbackStatus::Failed => WorkerStatus::Failed,
            WorkerCallbackStatus::Stopped => WorkerStatus::Stopped,
        }
    }
}

#[ta_objects::typed_enum]
#[derive(Eq, PartialEq, Hash)]
pub enum WorkerTypeDto {
    #[typed_enum(rename = "CT")]
    CronTrigger,
    #[typed_enum(rename = "ST")]
    StageTrigger,
}

#[ta_objects::typed_enum]
#[derive(Eq, PartialEq)]
pub enum WorkerType {
    #[typed_enum(rename = "F")]
    FunctionRunner,
    #[typed_enum(rename = "CT")]
    CronTrigger,
    #[typed_enum(rename = "ST")]
    StageTrigger,
}

impl From<WorkerTypeDto> for WorkerType {
    fn from(value: WorkerTypeDto) -> Self {
        match value {
            WorkerTypeDto::CronTrigger => WorkerType::CronTrigger,
            WorkerTypeDto::StageTrigger => WorkerType::StageTrigger,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pin the persisted codes of the sticky-terminal `FunctionRunStatus`
    /// variants. The td-schema `logical_function_runs__sticky_terminals`
    /// trigger hardcodes `&["C", "X", "Y"]`; td-schema can't import this
    /// enum (foundational crate ordering), so this test is the canary
    /// that catches a divergent rename.
    #[test]
    fn test_sticky_terminal_codes_pinned() {
        assert_eq!(LogicalFunctionRunStatus::Committed.to_string(), "C");
        assert_eq!(LogicalFunctionRunStatus::Canceled.to_string(), "X");
        assert_eq!(LogicalFunctionRunStatus::Yanked.to_string(), "Y");
    }
}
