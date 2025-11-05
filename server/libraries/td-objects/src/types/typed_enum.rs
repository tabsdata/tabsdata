//
// Copyright 2025 Tabs Data Inc.
//

use td_common::execution_status::WorkerCallbackStatus;

#[td_type::typed_enum]
pub enum Decorator {
    #[typed_enum(rename = "P")]
    Publisher,
    #[typed_enum(rename = "T")]
    Transformer,
    #[typed_enum(rename = "S")]
    Subscriber,
}

#[td_type::typed_enum]
pub enum DependencyStatus {
    #[typed_enum(rename = "A")]
    Active,
    #[typed_enum(rename = "D")]
    Deleted,
}

/// Represents the status of an execution.
/// It is a summary of the statuses of all function runs within the execution.
#[td_type::typed_enum]
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
///     Scheduled --> RunRequested
///     RunRequested --> Scheduled
///     RunRequested --> Running
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
#[td_type::typed_enum]
pub enum FunctionRunStatus {
    #[typed_enum(rename = "S")]
    Scheduled,
    #[typed_enum(rename = "RR")]
    RunRequested,
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

impl From<WorkerCallbackStatus> for FunctionRunStatus {
    fn from(value: WorkerCallbackStatus) -> Self {
        match value {
            WorkerCallbackStatus::Running => FunctionRunStatus::Running,
            WorkerCallbackStatus::Done => FunctionRunStatus::Done,
            WorkerCallbackStatus::Error => FunctionRunStatus::Error,
            WorkerCallbackStatus::Failed => FunctionRunStatus::Failed,
        }
    }
}

#[td_type::typed_enum]
pub enum FunctionStatus {
    #[typed_enum(rename = "A")]
    Active,
    #[typed_enum(rename = "F")]
    Frozen,
    #[typed_enum(rename = "D")]
    Deleted,
}

#[td_type::typed_enum]
pub enum GrantType {
    #[typed_enum(rename = "refresh_token")]
    RefreshToken,
}

#[td_type::typed_enum]
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

#[td_type::typed_enum]
pub enum PermissionEntityType {
    #[typed_enum(rename = "s")]
    System,
    #[typed_enum(rename = "c")]
    Collection,
}

#[td_type::typed_enum]
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

#[td_type::typed_enum]
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

#[td_type::typed_enum]
pub enum TableStatus {
    #[typed_enum(rename = "A")]
    Active,
    #[typed_enum(rename = "F")]
    Frozen,
    #[typed_enum(rename = "D")]
    Deleted,
}

/// Represents the status of a transaction. Note transactions are atomic status wise.
/// So final status (e.g., Committed, Canceled, Yanked) means all function runs within the transaction
/// do have the same status.
/// It is a summary of the statuses of all function runs within the transaction.
#[td_type::typed_enum]
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

#[td_type::typed_enum]
pub enum Trigger {
    #[typed_enum(rename = "M")]
    Manual,
    #[typed_enum(rename = "D")]
    Dependency,
}

#[td_type::typed_enum]
#[derive(Default)]
pub enum TriggerStatus {
    #[default]
    #[typed_enum(rename = "A")]
    Active,
    #[typed_enum(rename = "F")]
    Frozen,
    #[typed_enum(rename = "D")]
    Deleted,
}

#[td_type::typed_enum]
pub enum WorkerMessageStatus {
    #[typed_enum(rename = "L")]
    Locked,
    #[typed_enum(rename = "U")]
    Unlocked,
}

#[td_type::typed_enum]
pub enum WorkerStatus {
    #[typed_enum(rename = "RR")]
    RunRequested,
    #[typed_enum(rename = "R")]
    Running,
    #[typed_enum(rename = "D")]
    Done,
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
        }
    }
}
