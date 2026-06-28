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

impl Decorator {
    /// Map this DB decorator to the wire-level `td_common` decorator carried
    /// on worker messages. (A `From` impl can't live here — both types are
    /// foreign to a single crate under the orphan rule.)
    pub fn to_message(&self) -> td_common::worker::Decorator {
        match self {
            Decorator::Publisher => td_common::worker::Decorator::Publisher,
            Decorator::Transformer => td_common::worker::Decorator::Transformer,
            Decorator::Subscriber => td_common::worker::Decorator::Subscriber,
        }
    }
}

#[ta_objects::typed_enum]
#[derive(Eq, PartialEq)]
pub enum DependencyStatus {
    #[typed_enum(rename = "A")]
    Active,
    #[typed_enum(rename = "D")]
    Deleted,
}

/// Flavour of a collection, inferred from its group at creation time and then
/// immutable. `DataFactory` is the default; `Source`/`Destination` collections
/// carry connection info.
#[ta_objects::typed_enum]
#[derive(Eq, PartialEq)]
pub enum CollectionType {
    #[typed_enum(rename = "F")]
    DataFactory,
    #[typed_enum(rename = "S")]
    Source,
    #[typed_enum(rename = "D")]
    Destination,
}

impl CollectionType {
    /// Infer the flavour from the collection's group. The reserved `sources` and
    /// `destinations` groups map to `Source`/`Destination`; every other group
    /// (default or user-created) is a `DataFactory`. Keying on the group *id* is
    /// sound because the seeded system groups carry the same well-known id in
    /// every project (group ids are unique per project, not globally).
    pub fn from_group_id(group: &super::GroupId) -> Self {
        if *group == super::GroupId::source() {
            CollectionType::Source
        } else if *group == super::GroupId::destination() {
            CollectionType::Destination
        } else {
            CollectionType::DataFactory
        }
    }
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
#[derive(Eq, PartialEq, Hash, Copy)]
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

impl LogicalFunctionRunStatus {
    /// Statuses that imply `ended_on IS NOT NULL` on the bound PFR —
    /// the set `/metrics` aggregates over. `IN_FLIGHT` is the
    /// complement, the set `/inflight` returns.
    ///
    /// Note: "ended" is not the same as state-machine terminal. Per the
    /// transition diagram, `Done` can move to `Committed`/`Canceled`,
    /// `Error` back to `Running`, `Failed` to `ReScheduled` — so only
    /// `Committed`/`Canceled`/`Yanked` are truly no-transitions-out
    /// terminals.
    pub const ENDED: &'static [LogicalFunctionRunStatus] = &[
        LogicalFunctionRunStatus::Committed,
        LogicalFunctionRunStatus::Done,
        LogicalFunctionRunStatus::Error,
        LogicalFunctionRunStatus::Failed,
        LogicalFunctionRunStatus::Canceled,
        LogicalFunctionRunStatus::Yanked,
    ];

    pub const IN_FLIGHT: &'static [LogicalFunctionRunStatus] = &[
        LogicalFunctionRunStatus::Scheduled,
        LogicalFunctionRunStatus::Requested,
        LogicalFunctionRunStatus::ReScheduled,
        LogicalFunctionRunStatus::Running,
        LogicalFunctionRunStatus::OnHold,
    ];

    pub fn is_ended(&self) -> bool {
        Self::ENDED.contains(self)
    }
}

impl TryFrom<WorkerCallbackStatus> for LogicalFunctionRunStatus {
    type Error = TdError;

    fn try_from(value: WorkerCallbackStatus) -> Result<Self, TdError> {
        let status = match value {
            WorkerCallbackStatus::Running => LogicalFunctionRunStatus::Running,
            WorkerCallbackStatus::Done => LogicalFunctionRunStatus::Done,
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
#[derive(Eq, PartialEq, Hash, Copy)]
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

impl PhysicalFunctionRunStatus {
    /// Statuses where the attempt has finished and `ended_on` plus the
    /// bucket columns are populated.
    pub const ENDED: &'static [PhysicalFunctionRunStatus] = &[
        PhysicalFunctionRunStatus::Done,
        PhysicalFunctionRunStatus::Error,
        PhysicalFunctionRunStatus::Failed,
    ];

    pub const IN_FLIGHT: &'static [PhysicalFunctionRunStatus] = &[
        PhysicalFunctionRunStatus::Requested,
        PhysicalFunctionRunStatus::Dispatched,
        PhysicalFunctionRunStatus::Running,
    ];

    pub fn is_ended(&self) -> bool {
        Self::ENDED.contains(self)
    }
}

impl TryFrom<WorkerCallbackStatus> for PhysicalFunctionRunStatus {
    type Error = TdError;

    fn try_from(value: WorkerCallbackStatus) -> Result<Self, TdError> {
        let status = match value {
            WorkerCallbackStatus::Running => PhysicalFunctionRunStatus::Running,
            WorkerCallbackStatus::Done => PhysicalFunctionRunStatus::Done,
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
        (*s).into()
    }
}

/// Lifecycle marker for an uploaded function bundle. A bundle is `Pending`
/// from upload until a function register/update/patch claims it, at which
/// point it flips to `Registered`. Rows are never deleted, so `Pending` rows
/// that are never claimed are orphan-GC candidates.
#[ta_objects::typed_enum]
#[derive(Eq, PartialEq)]
pub enum BundleStatus {
    #[typed_enum(rename = "P")]
    Pending,
    #[typed_enum(rename = "R")]
    Registered,
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

/// The entity type a permission targets — the `entity_type` half of the
/// `(entity_type, entity_id)` pair.
///
/// An open string (not a closed enum): the built-in OS entity types are
/// `system` and `project`, but extensions register their own (e.g. `ai`) in the
/// authorization registry. The set of *valid* values is whatever the registry
/// has registered — validated at the grant-create boundary, not by this type.
#[ta_objects::typed(string)]
#[derive(Eq, PartialEq, Hash)]
pub struct PermissionEntityType;

impl PermissionEntityType {
    /// The built-in `system` entity type.
    pub fn system() -> Self {
        Self::try_from("system").expect("built-in entity type")
    }

    /// The built-in `project` entity type.
    pub fn project() -> Self {
        Self::try_from("project").expect("built-in entity type")
    }
}

/// A permission name, stored in `permissions.permission_type`. The stored value
/// is the exact user-facing symbol — no internal/external translation layer.
///
/// An open string (not a closed enum): the six built-in OS permissions are
/// listed below, but extensions register their own (e.g. `manage_ai`) in the
/// authorization registry. The set of *valid* `(entity_type, permission)` pairs
/// is whatever the registry has registered — validated at the grant-create
/// boundary, not by this type.
#[ta_objects::typed(string)]
#[derive(Eq, PartialEq, Hash)]
pub struct PermissionType;

impl PermissionType {
    // system permissions (entity `system:__system__`)
    pub fn manage_access() -> Self {
        Self::try_from("manage_access").expect("built-in permission")
    }
    pub fn create_projects() -> Self {
        Self::try_from("create_projects").expect("built-in permission")
    }
    pub fn manage_roles() -> Self {
        Self::try_from("manage_roles").expect("built-in permission")
    }
    // project permissions (entity `project:<id>` or `project:__all__`)
    pub fn view_project() -> Self {
        Self::try_from("view_project").expect("built-in permission")
    }
    pub fn develop_project() -> Self {
        Self::try_from("develop_project").expect("built-in permission")
    }
    pub fn manage_project() -> Self {
        Self::try_from("manage_project").expect("built-in permission")
    }

    /// The entity type a *built-in* permission targets, or `None` for a
    /// permission unknown to the OS core (e.g. an extension permission). The
    /// authoritative mapping for extension permissions lives in the
    /// authorization registry; this covers only the built-ins, which is all the
    /// OS-core create/translation paths handle.
    pub fn on_entity_type(&self) -> Option<PermissionEntityType> {
        match self.as_str() {
            "manage_access" | "create_projects" | "manage_roles" => {
                Some(PermissionEntityType::system())
            }
            "view_project" | "develop_project" | "manage_project" => {
                Some(PermissionEntityType::project())
            }
            _ => None,
        }
    }
}

/// The flat permission names exposed on the Simple Security Mode surface
///. These never hit a table directly: the facade in `td-services`
/// (`simple_permission`) translates each one into the RBAC role-membership /
/// `role_permission` reads it maps to.
///
/// Three are project-scoped (`view_project`/`develop_project`/`manage_project`,
/// always *on a project*) and two are system-scoped
/// (`create_projects`/`manage_system`, instance-wide). `manage_system` is full
/// system admin (`sys_admin`): user/role administration plus authority over all
/// projects. The org-wide read/develop wildcards
/// (`view_all_projects`/`develop_all_projects`) are intentionally absent — they
/// have no Simple-surface name and are reachable only in RBAC Mode View.
#[ta_objects::typed_enum]
#[derive(Eq, PartialEq, Hash)]
pub enum SimplePermissionType {
    // project-scoped — require a project
    #[typed_enum(rename = "view_project")]
    ViewProject,
    #[typed_enum(rename = "develop_project")]
    DevelopProject,
    #[typed_enum(rename = "manage_project")]
    ManageProject,
    // system-scoped — must not carry a project
    #[typed_enum(rename = "create_projects")]
    CreateProjects,
    #[typed_enum(rename = "manage_system")]
    ManageSystem,
    // org-wide read/develop wildcards: parseable so the facade can reject them
    // with a clear "use RBAC Mode View" error rather than a generic 400; they
    // have no faithful Simple-surface mapping.
    #[typed_enum(rename = "view_all_projects")]
    ViewAllProjects,
    #[typed_enum(rename = "develop_all_projects")]
    DevelopAllProjects,
}

impl SimplePermissionType {
    /// The org-wide read/develop wildcards have no Simple-surface mapping and
    /// are rejected by the facade (reachable only in RBAC Mode View).
    pub fn is_rbac_only_wildcard(&self) -> bool {
        matches!(self, Self::ViewAllProjects | Self::DevelopAllProjects)
    }

    /// Whether this permission is project-scoped (a project is required) or
    /// system-scoped (a project is forbidden). Only meaningful for the
    /// non-wildcard permissions; the RBAC-only wildcards are rejected before
    /// this is consulted.
    pub fn is_project_scoped(&self) -> bool {
        matches!(
            self,
            Self::ViewProject | Self::DevelopProject | Self::ManageProject
        )
    }

    /// For a project-scoped permission, the `<project>_<suffix>` per-project
    /// role suffix it maps to (`admin`/`dev`/`view`). `None` for system perms.
    pub fn project_role_suffix(&self) -> Option<&'static str> {
        match self {
            Self::ManageProject => Some("admin"),
            Self::DevelopProject => Some("dev"),
            Self::ViewProject => Some("view"),
            _ => None,
        }
    }
}

/// The kind of a role, stored in `roles.kind` (the user-visible role kinds
/// plus the API-created `custom` kind). The built-in system roles
/// (`sys_admin`/`default`/`project_creator`) have `project_id` NULL; the three
/// per-project kinds (`project_admin`/`project_dev`/`project_view`) carry a `project_id`.
/// `custom` is the kind of any role created through the RBAC `role create`
/// surface — its presence is what makes a configuration unrepresentable in
/// Simple Security Mode (condition 1).
#[ta_objects::typed_enum]
#[derive(Eq, PartialEq)]
pub enum RoleKind {
    #[typed_enum(rename = "sys_admin")]
    SysAdmin,
    #[typed_enum(rename = "default")]
    Default,
    #[typed_enum(rename = "project_creator")]
    ProjectCreator,
    #[typed_enum(rename = "project_admin")]
    ProjectAdmin,
    #[typed_enum(rename = "project_dev")]
    ProjectDev,
    #[typed_enum(rename = "project_view")]
    ProjectView,
    #[typed_enum(rename = "custom")]
    Custom,
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
    use crate::types::basic::GroupId;

    #[test]
    fn test_collection_type_from_group_id() {
        assert_eq!(
            CollectionType::from_group_id(&GroupId::source()),
            CollectionType::Source
        );
        assert_eq!(
            CollectionType::from_group_id(&GroupId::destination()),
            CollectionType::Destination
        );
        assert_eq!(
            CollectionType::from_group_id(&GroupId::default_group()),
            CollectionType::DataFactory
        );
        // Any fresh (user-created) group id is a DataFactory.
        assert_eq!(
            CollectionType::from_group_id(&GroupId::default()),
            CollectionType::DataFactory
        );
    }

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

    /// Every LFR variant must land in exactly one of ENDED / IN_FLIGHT.
    /// Catches the "added a variant but forgot to classify it" drift
    /// that a manual array enumeration would miss.
    #[test]
    fn lfr_ended_and_in_flight_partition_all_variants() {
        use strum::IntoEnumIterator;
        assert_eq!(LogicalFunctionRunStatus::ENDED.len(), 6);
        assert_eq!(LogicalFunctionRunStatus::IN_FLIGHT.len(), 5);
        for s in LogicalFunctionRunStatus::iter() {
            let ended = LogicalFunctionRunStatus::ENDED.contains(&s);
            let in_flight = LogicalFunctionRunStatus::IN_FLIGHT.contains(&s);
            assert!(
                ended ^ in_flight,
                "{s} must be in exactly one of ENDED / IN_FLIGHT"
            );
            assert_eq!(s.is_ended(), ended);
        }
    }

    #[test]
    fn pfr_ended_and_in_flight_partition_all_variants() {
        use strum::IntoEnumIterator;
        assert_eq!(PhysicalFunctionRunStatus::ENDED.len(), 3);
        assert_eq!(PhysicalFunctionRunStatus::IN_FLIGHT.len(), 3);
        for s in PhysicalFunctionRunStatus::iter() {
            let ended = PhysicalFunctionRunStatus::ENDED.contains(&s);
            let in_flight = PhysicalFunctionRunStatus::IN_FLIGHT.contains(&s);
            assert!(
                ended ^ in_flight,
                "{s} must be in exactly one of ENDED / IN_FLIGHT"
            );
            assert_eq!(s.is_ended(), ended);
        }
    }
}
