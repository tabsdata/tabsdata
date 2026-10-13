//
// Copyright 2025 Tabsdata Inc.
//

use ta_objects::{Typed, TypedEnum, TypedOpenApi, TypedSerde, TypedSqlx};
use td_common::execution_status::WorkerCallbackStatus;
use td_error::{ApiError, TdError, api_error};

#[derive(TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi, Default, Eq, PartialEq)]
pub enum FunctionType {
    #[default]
    #[typed_enum(rename = "R")]
    Regular,
    #[typed_enum(rename = "DQST")]
    DQSummaryTrends,
}

#[derive(TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
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

/// Which seeded group a row is, stored in `groups.kind`; `Custom` is any group
/// created through the API.
#[derive(TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
pub enum GroupKind {
    #[typed_enum(rename = "default")]
    Default,
    #[typed_enum(rename = "sources")]
    Sources,
    #[typed_enum(rename = "destinations")]
    Destinations,
    #[typed_enum(rename = "custom")]
    Custom,
}

/// Flavour of a collection, inferred from its group at creation time and then
/// immutable. `DataFactory` is the default; `Source`/`Destination` collections
/// carry connection info.
#[derive(TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
pub enum CollectionType {
    #[typed_enum(rename = "F")]
    DataFactory,
    #[typed_enum(rename = "S")]
    Source,
    #[typed_enum(rename = "D")]
    Destination,
}

impl CollectionType {
    /// Infer the flavour from the collection's group. The reserved `sources`
    /// and `destinations` groups map to `Source`/`Destination`; every other
    /// group (default or user-created) is a `DataFactory`.
    pub fn from_group_kind(kind: &GroupKind) -> Self {
        match kind {
            GroupKind::Sources => CollectionType::Source,
            GroupKind::Destinations => CollectionType::Destination,
            GroupKind::Default | GroupKind::Custom => CollectionType::DataFactory,
        }
    }
}

/// Represents the status of an execution.
/// It is a summary of the statuses of all function runs within the execution.
#[derive(TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Hash)]
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

/// Represents the state of a function run. The status changes it may make
/// are [`Self::TRANSITIONS`]; the database refuses any other.
#[derive(
    TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Ord, PartialOrd, Hash, Copy,
)]
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
    /// Every status change a run may make. Canceled and Yanked have no way
    /// out, and Committed only to Yanked.
    pub const TRANSITIONS: &'static [(Self, Self)] = &[
        (Self::Scheduled, Self::Requested),
        (Self::Scheduled, Self::OnHold),
        (Self::Scheduled, Self::ReScheduled),
        (Self::Scheduled, Self::Canceled),
        (Self::Requested, Self::Scheduled),
        (Self::Requested, Self::Running),
        (Self::Requested, Self::Error),
        (Self::Requested, Self::Failed),
        (Self::Requested, Self::OnHold),
        (Self::Requested, Self::ReScheduled),
        (Self::Requested, Self::Canceled),
        (Self::ReScheduled, Self::Requested),
        (Self::ReScheduled, Self::OnHold),
        (Self::ReScheduled, Self::Canceled),
        (Self::Running, Self::Done),
        (Self::Running, Self::Error),
        (Self::Running, Self::Failed),
        (Self::Running, Self::OnHold),
        (Self::Running, Self::ReScheduled),
        (Self::Running, Self::Canceled),
        (Self::Done, Self::Committed),
        (Self::Done, Self::OnHold),
        (Self::Done, Self::ReScheduled),
        (Self::Done, Self::Canceled),
        (Self::Committed, Self::Yanked),
        (Self::Error, Self::OnHold),
        (Self::Error, Self::ReScheduled),
        (Self::Error, Self::Canceled),
        (Self::Failed, Self::Done),
        (Self::Failed, Self::OnHold),
        (Self::Failed, Self::ReScheduled),
        (Self::Failed, Self::Canceled),
        (Self::OnHold, Self::ReScheduled),
        (Self::OnHold, Self::Canceled),
    ];

    /// Whether a run may be written from `self` to `to`. Writing the status a
    /// run already has is always allowed.
    pub fn can_move_to(&self, to: &Self) -> bool {
        self == to || Self::TRANSITIONS.contains(&(*self, *to))
    }

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

/// Per-attempt status on `physical_function_runs`. Independent of
/// [`LogicalFunctionRunStatus`] (LFR/LTDV-canonical, sticky-Committed). The
/// status changes it may make are [`Self::TRANSITIONS`]; the database refuses
/// any other.
#[derive(TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Hash, Copy)]
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

/// The kind of a `/metrics` edge row.
#[derive(
    TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Ord, PartialOrd, Hash, Copy,
)]
pub enum EdgeKind {
    /// A consumed version's producer dim → the consuming attempt's dim.
    #[typed_enum(rename = "R")]
    Real,
    /// Produced by an attempt with no inputs: data entering the system.
    #[typed_enum(rename = "S")]
    ExternalSource,
    /// Consumed by an attempt with no visible outputs: data leaving the system.
    #[typed_enum(rename = "E")]
    Egress,
}

/// Which event of an ended attempt a `/metrics` fact row records.
#[derive(
    TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Ord, PartialOrd, Hash, Copy,
)]
pub enum FactSide {
    /// The attempt itself.
    #[typed_enum(rename = "A")]
    Attempt,
    /// A version the attempt wrote.
    #[typed_enum(rename = "O")]
    Written,
    /// A version the attempt read.
    #[typed_enum(rename = "I")]
    Read,
    /// A version the attempt read and no visible output carries onward.
    #[typed_enum(rename = "E")]
    Egress,
}

impl PhysicalFunctionRunStatus {
    /// Every status change an attempt may make. Done, Error and Failed have no
    /// way out.
    pub const TRANSITIONS: &'static [(Self, Self)] = &[
        (Self::Requested, Self::Dispatched),
        (Self::Requested, Self::Failed),
        (Self::Dispatched, Self::Running),
        (Self::Dispatched, Self::Failed),
        (Self::Running, Self::Done),
        (Self::Running, Self::Failed),
    ];

    /// Whether an attempt may be written from `self` to `to`. Writing the
    /// status an attempt already has is always allowed.
    pub fn can_move_to(&self, to: &Self) -> bool {
        self == to || Self::TRANSITIONS.contains(&(*self, *to))
    }

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
/// attempt outcome (Committed/Yanked → Done, Canceled → Failed); a Requested
/// run's attempt was dispatched by the tick that requested it; and a run not
/// yet requested can only carry an attempt that already ended, since a new
/// attempt opens only once the previous one has.
impl From<LogicalFunctionRunStatus> for PhysicalFunctionRunStatus {
    fn from(s: LogicalFunctionRunStatus) -> Self {
        match s {
            LogicalFunctionRunStatus::Running => PhysicalFunctionRunStatus::Running,
            LogicalFunctionRunStatus::Done
            | LogicalFunctionRunStatus::Committed
            | LogicalFunctionRunStatus::Yanked => PhysicalFunctionRunStatus::Done,
            LogicalFunctionRunStatus::Error => PhysicalFunctionRunStatus::Error,
            LogicalFunctionRunStatus::Failed
            | LogicalFunctionRunStatus::Canceled
            | LogicalFunctionRunStatus::Scheduled
            | LogicalFunctionRunStatus::ReScheduled
            | LogicalFunctionRunStatus::OnHold => PhysicalFunctionRunStatus::Failed,
            LogicalFunctionRunStatus::Requested => PhysicalFunctionRunStatus::Dispatched,
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
#[derive(TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
pub enum BundleStatus {
    #[typed_enum(rename = "P")]
    Pending,
    #[typed_enum(rename = "R")]
    Registered,
}

#[derive(TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
pub enum FunctionStatus {
    #[typed_enum(rename = "A")]
    Active,
    #[typed_enum(rename = "F")]
    Frozen,
}

#[derive(TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi)]
pub enum GrantType {
    #[typed_enum(rename = "refresh_token")]
    RefreshToken,
}

#[derive(TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi)]
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
#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Hash)]
pub struct PermissionEntityType(String);

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
#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Hash)]
pub struct PermissionType(String);

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
#[derive(TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Hash)]
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

    /// For a project-scoped permission, the kind of the per-project role it
    /// maps to. `None` for system perms.
    pub fn project_role_kind(&self) -> Option<RoleKind> {
        match self {
            Self::ManageProject => Some(RoleKind::ProjectAdmin),
            Self::DevelopProject => Some(RoleKind::ProjectDev),
            Self::ViewProject => Some(RoleKind::ProjectView),
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
#[derive(TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
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
#[derive(TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Hash)]
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

/// What a service token carries of its owner's grants.
#[derive(TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Copy)]
pub enum TokenScope {
    /// Only the permissions a token may use: project view and develop.
    #[typed_enum(rename = "delegated")]
    Delegated,
    /// Every grant the owner holds. No API creates one.
    #[typed_enum(rename = "identity")]
    Identity,
}

#[derive(TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
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
#[derive(TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi, Default, Eq, PartialEq, Hash)]
pub enum StorageVersion {
    #[default]
    V1,
}

#[derive(TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
pub enum TableStatus {
    #[typed_enum(rename = "A")]
    Active,
    #[typed_enum(rename = "F")]
    Frozen,
}

#[derive(TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi, Default, Eq, PartialEq)]
pub enum TableType {
    #[default]
    #[typed_enum(rename = "R")]
    Regular,
    #[typed_enum(rename = "DQD")]
    DQData,
    #[typed_enum(rename = "DQS")]
    DQSummary,
    #[typed_enum(rename = "DQST")]
    DQSummaryTrends,
}

/// Represents the status of a transaction. Note transactions are atomic status wise.
/// So final status (e.g., Committed, Canceled, Yanked) means all function runs within the transaction
/// do have the same status.
/// It is a summary of the statuses of all function runs within the transaction.
#[derive(TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Hash)]
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

#[derive(TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
pub enum Trigger {
    #[typed_enum(rename = "M")]
    Manual,
    #[typed_enum(rename = "D")]
    Dependency,
}

#[derive(TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi, Default, Eq, PartialEq)]
pub enum TriggerStatus {
    #[default]
    #[typed_enum(rename = "A")]
    Active,
    #[typed_enum(rename = "F")]
    Frozen,
}

#[derive(TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
pub enum TriggerType {
    #[typed_enum(rename = "M")]
    Manual,
    #[typed_enum(rename = "CT")]
    CronTrigger,
    #[typed_enum(rename = "ST")]
    StageTrigger,
}

#[derive(TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
pub enum WorkerMessageStatus {
    #[typed_enum(rename = "N")]
    None,
    #[typed_enum(rename = "L")]
    Locked,
    #[typed_enum(rename = "U")]
    Unlocked,
}

#[derive(TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
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

impl WorkerStatus {
    const FUNCTION_RUNNER: &'static [(Self, Self)] = &[
        (Self::Requested, Self::Running),
        (Self::Requested, Self::Failed),
        (Self::Running, Self::Done),
        (Self::Running, Self::Failed),
    ];

    const CRON_TRIGGER: &'static [(Self, Self)] = &[
        (Self::Scheduled, Self::Requested),
        (Self::Requested, Self::Running),
        (Self::Requested, Self::Failed),
        (Self::Running, Self::Done),
        (Self::Running, Self::Failed),
        (Self::Done, Self::Running),
        (Self::Done, Self::Failed),
        (Self::Failed, Self::Running),
        (Self::Failed, Self::Done),
        (Self::Scheduled, Self::Stopped),
        (Self::Scheduled, Self::StopScheduled),
        (Self::Requested, Self::StopScheduled),
        (Self::Running, Self::StopScheduled),
        (Self::Done, Self::StopScheduled),
        (Self::Failed, Self::StopScheduled),
        (Self::StopScheduled, Self::StopRequested),
        (Self::StopScheduled, Self::Stopped),
        (Self::StopRequested, Self::Stopped),
    ];

    const STAGE_TRIGGER: &'static [(Self, Self)] = &[
        (Self::Scheduled, Self::Requested),
        (Self::Requested, Self::Running),
        (Self::Requested, Self::Failed),
        (Self::Running, Self::Done),
        (Self::Running, Self::Failed),
        (Self::Done, Self::Failed),
        (Self::Failed, Self::Running),
        (Self::Scheduled, Self::Stopped),
        (Self::Scheduled, Self::StopScheduled),
        (Self::Requested, Self::StopScheduled),
        (Self::Running, Self::StopScheduled),
        (Self::Done, Self::StopScheduled),
        (Self::Failed, Self::StopScheduled),
        (Self::StopScheduled, Self::StopRequested),
        (Self::StopScheduled, Self::Stopped),
        (Self::StopRequested, Self::Stopped),
    ];

    /// Every status change a worker of `worker_type` may make. A function
    /// runner runs once. A trigger stops from any state its workload may still
    /// exist in: a cron trigger parks at Done or Failed between fires, and a
    /// stage trigger restarts from Failed once its retry is due, or stops. A
    /// stage trigger that exits Done while it should run has failed. A trigger
    /// never started has no workload, so it stops at once.
    pub fn transitions(worker_type: &WorkerType) -> &'static [(Self, Self)] {
        match worker_type {
            WorkerType::FunctionRunner => Self::FUNCTION_RUNNER,
            WorkerType::CronTrigger => Self::CRON_TRIGGER,
            WorkerType::StageTrigger => Self::STAGE_TRIGGER,
        }
    }

    /// Whether a worker of `worker_type` may be written from `self` to `to`.
    /// Writing the status a worker already has is always allowed.
    pub fn can_move_to(&self, worker_type: &WorkerType, to: &Self) -> bool {
        self == to || Self::transitions(worker_type).contains(&(*self, *to))
    }
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

#[derive(TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Hash)]
pub enum WorkerTypeDto {
    #[typed_enum(rename = "CT")]
    CronTrigger,
    #[typed_enum(rename = "ST")]
    StageTrigger,
}

#[derive(TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
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

    #[test]
    fn test_collection_type_from_group_kind() {
        assert_eq!(
            CollectionType::from_group_kind(&GroupKind::Sources),
            CollectionType::Source
        );
        assert_eq!(
            CollectionType::from_group_kind(&GroupKind::Destinations),
            CollectionType::Destination
        );
        assert_eq!(
            CollectionType::from_group_kind(&GroupKind::Default),
            CollectionType::DataFactory
        );
        assert_eq!(
            CollectionType::from_group_kind(&GroupKind::Custom),
            CollectionType::DataFactory
        );
    }

    /// Pin the persisted codes of the terminal `FunctionRunStatus` variants.
    /// td-schema's status roll-ups hardcode `&["C", "X", "Y"]`; td-schema
    /// can't import this enum (foundational crate ordering), so this test is
    /// the canary that catches a divergent rename.
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
