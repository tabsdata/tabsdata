//
// Copyright 2024 Tabs Data Inc.
//

use crate::server::ResponseMessagePayload;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::str::FromStr;
use strum::ParseError;
use strum_macros::{Display, EnumString};

// TODO: Value is a placeholder, we need to define the actual type
pub type DataVersionUpdateRequest = ResponseMessagePayload<Value>;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum ExecutionUpdateStatus {
    Running,
    Done,
    Error,
    Failed,
}

impl From<ExecutionUpdateStatus> for DataVersionStatus {
    fn from(s: ExecutionUpdateStatus) -> Self {
        match s {
            ExecutionUpdateStatus::Running => DataVersionStatus::Running,
            ExecutionUpdateStatus::Done => DataVersionStatus::Done,
            ExecutionUpdateStatus::Error => DataVersionStatus::Error,
            ExecutionUpdateStatus::Failed => DataVersionStatus::Failed,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RecoverStatus {
    Cancel,
    Reschedule,
}

impl From<RecoverStatus> for DataVersionStatus {
    fn from(s: RecoverStatus) -> Self {
        match s {
            RecoverStatus::Cancel => DataVersionStatus::Canceled,
            RecoverStatus::Reschedule => DataVersionStatus::Scheduled,
        }
    }
}

#[cfg_attr(doc, aquamarine::aquamarine)]
/// Represents the state of an execution plan.
///
/// ```mermaid
/// stateDiagram-v2
///     [*] --> Scheduled
///     Complete --> [*]
///     Incomplete --> [*]
///
///     Scheduled --> Running
///     Running --> Complete
///     Running --> Incomplete
/// ```
#[derive(Debug, Clone, PartialEq, EnumString, Display, Serialize, Deserialize)]
pub enum ExecutionPlanStatus {
    #[strum(serialize = "S")]
    /// All transactions scheduled.
    Scheduled,
    #[strum(serialize = "R")]
    /// At least one transaction is not finished.
    Running,
    #[strum(serialize = "D")]
    /// All transactions finished in Published status.
    Done,
    #[strum(serialize = "I")]
    /// All transactions finished and at least one transaction finished in Canceled status.
    Incomplete,
}

impl TryFrom<String> for ExecutionPlanStatus {
    type Error = ParseError;

    fn try_from(s: String) -> Result<Self, ParseError> {
        ExecutionPlanStatus::from_str(s.as_str())
    }
}

#[cfg_attr(doc, aquamarine::aquamarine)]
/// Represents the state of a transaction.
///
/// ```mermaid
/// stateDiagram-v2
///     [*] --> Scheduled
///     Canceled --> [*]
///     Published --> [*]
///
///     Scheduled --> Running
///     Scheduled --> OnHold
///     Scheduled --> Canceled
///     Running --> Published
///     Running --> Failed
///     Running --> Canceled
///     OnHold --> Canceled
///     OnHold --> Scheduled
///     Failed --> Scheduled
///     Failed --> Canceled
/// ```
#[derive(Debug, Clone, PartialEq, EnumString, Display, Serialize, Deserialize)]
pub enum TransactionStatus {
    #[strum(serialize = "S")]
    /// Scheduled for execution.
    Scheduled,
    #[strum(serialize = "R")]
    /// Execution in progress.
    Running,

    #[strum(serialize = "F")]
    /// Execution completed with an error.
    Failed,

    #[strum(serialize = "H")]
    /// A direct parent is in failed state. External resolution is required.
    OnHold,

    #[strum(serialize = "C")]
    /// Execution canceled. Version is invalid. This state is final, and all dependants are also in this state.
    Canceled,
    #[strum(serialize = "P")]
    /// Execution finished. This state is final. Only if all the steps in the plan are in published state.
    Published,
}

impl TryFrom<String> for TransactionStatus {
    type Error = ParseError;

    fn try_from(s: String) -> Result<Self, ParseError> {
        TransactionStatus::from_str(s.as_str())
    }
}

#[cfg_attr(doc, aquamarine::aquamarine)]
/// Represents the state of a data version.
///
/// ```mermaid
/// stateDiagram-v2
///     [*] --> Scheduled
///     Canceled --> [*]
///     Published --> [*]
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
///     Failed --> Scheduled
///     Failed --> Canceled
///     Done --> Published
///     Done --> Canceled
/// ```
#[derive(Debug, Clone, PartialEq, EnumString, Display, Serialize, Deserialize)]
pub enum DataVersionStatus {
    #[strum(serialize = "S")]
    /// Scheduled for execution.
    Scheduled,
    #[strum(serialize = "Rr")]
    /// Execution requested.
    RunRequested,
    #[strum(serialize = "R")]
    /// Execution in progress.
    Running,
    #[strum(serialize = "D")]
    /// Execution completed successfully.
    Done,

    #[strum(serialize = "E")]
    /// Execution completed with an error, but still can retry.
    Error,
    #[strum(serialize = "F")]
    /// Execution completed with an error, and cannot retry.
    Failed,

    #[strum(serialize = "H")]
    /// A direct parent is in failed state. External resolution is required.
    OnHold,

    #[strum(serialize = "C")]
    /// Execution canceled. Version is invalid. This state is final, and all dependants are also in this state.
    Canceled,
    #[strum(serialize = "P")]
    /// Execution finished. This state is final. Only if all the steps in the plan are in published state.
    Published,
}

impl TryFrom<String> for DataVersionStatus {
    type Error = ParseError;

    fn try_from(s: String) -> Result<Self, ParseError> {
        DataVersionStatus::from_str(s.as_str())
    }
}
