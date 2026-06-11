//
// Copyright 2026 Tabs Data Inc.
//

//! DAO / DLO models for the logical-version layer.
//!
//! Five tables. Two pairs of identity-row + pointer-event-log, plus the
//! N:M dependency-binding rows:
//!
//! - `LogicalFunctionRunDB` — the LFR identity row, insert-once.
//! - `LogicalFunctionRunPointerDB` — append-only pointer-change event log.
//!   The latest row by `(set_on DESC, id DESC)` for a given
//!   `logical_function_run_id` is the current pointer.
//! - `LogicalTableDataVersionDB` — the LTDV identity row, insert-once.
//! - `LogicalTableDataVersionPointerDB` — append-only pointer-change event log.
//! - `LogicalFunctionRunInputDB` — V2 dependency-binding row (LFR → LTDV).
//!
//! Plan-build inserts an LFR / LTDV identity row plus a `Pending`
//! `InitialMint` pointer-event row. Dispatch appends a `Bound` event with
//! a freshly minted concrete `PhysicalFunctionRunId` / `PhysicalTableDataVersionId`.
//! Cancellation appends a `Voided` event. Operator redirect appends a
//! fresh `Bound` event with a different concrete id. Lazy-mint of a
//! V1-born concrete TDV inserts the LTDV identity row and a single
//! `Bound` `InitialMaterialization` event.

use crate::types::basic::{AtTime, LogicalPointerReason, LogicalPointerStatus, ProjectId, UserId};
use ta_objects::{Dao, SeaValue, VersionedDao};
use td_error::TdError;

/// Pointer-event-log DAO for a logical identity.
///
/// Implemented (via the `Dao` macro's `logical_pointer(...)` arg) by
/// the two pointer-event tables that track LFR / LTDV state:
/// `LogicalFunctionRunPointerDB` and `LogicalTableDataVersionPointerDB`.
/// Carries the schema metadata, row accessors, and event constructors —
/// no I/O. State-machine logic (bind / redirect / void) lives on
/// `td_queries::LogicalPointers<P, E>`, reached via
/// `conn.logical_pointers::<P>()`.
///
/// Each pointer-event table is `event_log` (multi-row per partition)
/// and `versioned`, partitioned by the logical identity and ordered
/// by `set_on: AtTime`. Those structural invariants are encoded in the
/// trait's `VersionedDao` super-bounds: `Partition = LogicalId` and
/// `VersionOrder = AtTime`. So `db.versioned(&at).fetch_one::<P>(logical_id)`
/// type-checks for any `P: LogicalPointerDao`.
pub trait LogicalPointerDao:
    VersionedDao<VersionOrder = AtTime, Partition = <Self as LogicalPointerDao>::LogicalId> + Sized
{
    /// The logical identity column's value type (e.g. `LogicalFunctionRunId`).
    type LogicalId: SeaValue + Clone + Send + Sync + std::fmt::Display;

    /// The concrete-binding column's value type when present (e.g.
    /// `PhysicalFunctionRunId` / `PhysicalTableDataVersionId`). The column itself is
    /// `Option<ConcreteId>` — Pending and Voided rows store `NULL`.
    type ConcreteId: SeaValue
        + Clone
        + PartialEq
        + Send
        + Sync
        + std::fmt::Debug
        + std::fmt::Display;

    /// The insert-once identity-row DAO this pointer-event row references.
    type IdentityDB: Dao;

    /// Human-readable kind tag for error messages ("LFR" or "LTDV").
    const KIND: &'static str;

    /// Iden for the logical-identity column (partition column).
    fn logical_id_col() -> Self::Iden;

    /// Iden for the concrete-binding column.
    fn concrete_id_col() -> Self::Iden;

    /// Iden for the `status` column.
    fn status_col() -> Self::Iden;

    /// Status of this event row.
    fn status(&self) -> LogicalPointerStatus;

    /// Logical-identity id of the partition this event belongs to.
    fn logical_id(&self) -> &Self::LogicalId;

    /// Concrete id at this event row (`None` for Pending / Voided).
    fn concrete_id(&self) -> Option<&Self::ConcreteId>;

    /// Build a fresh pointer-event row. `project_id` is the project of the
    /// logical identity this event belongs to — passed in (not derived from
    /// `logical_id`) so the scheduler's bypass dispatch tick can stamp it
    /// explicitly rather than leaning on the connection GUC default.
    fn build_event(
        project_id: ProjectId,
        logical_id: &Self::LogicalId,
        status: LogicalPointerStatus,
        concrete_id: Option<Self::ConcreteId>,
        set_on: AtTime,
        set_by_id: UserId,
        reason: LogicalPointerReason,
    ) -> Result<Self, TdError>;

    /// Build the `InitialMint` event that pairs an identity row at
    /// plan-build (Pending status, no concrete id, `set_on` /
    /// `set_by_id` derived from the identity's plan-time fields).
    fn initial_mint_for(identity: &Self::IdentityDB) -> Result<Self, TdError>;
}

#[ta_objects::dxo]
mod definitions {
    use crate::dxo::worker::UpdateWorkerExecution;
    use crate::types::basic::{
        AtTime, ByteCount, CollectionId, CollectionName, ColumnCount, DataChanged,
        DependencyPos, EpochMs, ExecutionId, ExecutionName, FunctionId, FunctionName,
        FunctionVersionId, GroupId, GroupName, HasData, InputIdx, LogicalFunctionRunId,
        LogicalFunctionRunInputId, LogicalFunctionRunPointerId, LogicalFunctionRunStatus,
        LogicalPointerReason, LogicalPointerStatus, LogicalTableDataVersionId,
        LogicalTableDataVersionPointerId, PhysicalFunctionRunId, PhysicalFunctionRunStatus,
        PhysicalTableDataVersionId, ProjectId, ProjectName, RowCount, SchemaChanged,
        SchemaDataChanged, SchemaHash, StorageVersion, TableFunctionParamPos, TableId, TableName,
        TableVersionCount, TableVersionId, TransactionId, Trigger, TriggeredOn, UserId, UserName,
        VersionPos,
    };
    use td_error::TdError;

    // -----------------------------------------------------------------------
    // Replay endpoint DTOs
    // -----------------------------------------------------------------------

    /// Response body for `POST .../logical-function-runs/{lfr}/replay`.
    /// `new_function_run_id` is the freshly-minted FR. `previous_function_run_id`
    /// is the FR the pointer currently points at (still bound until the
    /// new attempt finishes successfully).
    #[ta_objects::Dto]
    pub struct LogicalFunctionRunReplayResponse {
        pub logical_function_run_id: LogicalFunctionRunId,
        pub previous_function_run_id: PhysicalFunctionRunId,
        pub new_function_run_id: PhysicalFunctionRunId,
        pub function_version_id: FunctionVersionId,
    }

    // -----------------------------------------------------------------------
    // logical_function_runs (LFR) — per-run plan-time identity
    // -----------------------------------------------------------------------

    #[ta_objects::Dao]
    #[dao(
        sql_table = "logical_function_runs",
        order_by(col = "triggered_on", dir = "desc")
    )]
    pub struct LogicalFunctionRunDB {
        #[builder(default = "LogicalFunctionRunId::new()")]
        #[dxo(extractor)]
        pub id: LogicalFunctionRunId,
        /// The project this run belongs to. Inherited from the parent
        /// collection at LFR mint; threaded forward to the FR + worker rows
        /// the LFR spawns so the RLS GUC matches end-to-end.
        #[dxo(extractor)]
        pub project_id: ProjectId,
        pub collection_id: CollectionId,
        // (execution_id, function_id) is the natural key for an LFR —
        // one LFR per logical run within an execution. The FR → LFR
        // derivation (callback, redirect, replay, dispatch) uses these
        // two as composite filter; both are marked as extractors so
        // `fetch_one::<LogicalFunctionRunDB>(and!(&exec_id, &fn_id))`
        // resolves directly.
        #[dxo(extractor)]
        pub execution_id: ExecutionId,
        pub transaction_id: TransactionId,
        // Logical function identity. The *function version* (i.e. which
        // code) the LFR's concrete attempts run is on each concrete FR
        // row, not here — replays can attach a fresh concrete FR with a
        // different function_version_id under the same LFR identity.
        #[dxo(extractor)]
        pub function_id: FunctionId,
        // Plan-time fvid this LFR was minted against. Topology
        // reference for primitives (mint / bind): a target fvid's
        // head must equal `head_of(function_version_id)`.
        // Set once at plan-build; never updated.
        #[dxo(extractor)]
        pub function_version_id: FunctionVersionId,
        pub trigger: Trigger,
        pub triggered_on: TriggeredOn,
        pub triggered_by_id: UserId,
        // Canonical execution status. Sticky once Committed: no future
        // write moves it. Replay attempts still mint a new FR and
        // (on success) redirect the pointer, but never demote a
        // Committed LFR.
        #[builder(default = LogicalFunctionRunStatus::Scheduled)]
        pub status: LogicalFunctionRunStatus,
        // Group snapshot copied from collections.group_id at LFR mint;
        // immutable thereafter so a collection's later group reassignment
        // doesn't shift historical runs. No default: the group is
        // project-scoped, so it must be stamped explicitly from the run's
        // collection — a global fallback would mis-tag runs in any non-sandbox
        // project.
        pub group_id: GroupId,
    }

    // Names-joined view of the LFR identity row, merged with the
    // currently-bound PFR's per-attempt fields (started_on, ended_on,
    // function_version_patch_id, the PFR id itself). PFR-derived fields
    // are NULL when no PFR is currently bound (Pending/Voided).
    //
    // `status` is the canonical LFR-level (sticky-Committed) flow-control
    // status — not the PFR's per-attempt status.
    #[ta_objects::Dao]
    #[dao(sql_table = "function_runs__with_names")]
    #[inherits(LogicalFunctionRunDB)]
    pub struct FunctionRunDBWithNames {
        pub name: FunctionName,
        pub collection: CollectionName,
        pub execution: Option<ExecutionName>,
        pub triggered_by: UserName,
        pub group: Option<GroupName>,
        /// Display name resolved from `projects.name` via the view join.
        pub project: Option<ProjectName>,
        // PFR id currently bound to this LFR (resolved from the pointer
        // event log); NULL while Pending/Voided.
        pub physical_function_run_id: Option<PhysicalFunctionRunId>,
        pub started_on: Option<AtTime>,
        pub ended_on: Option<AtTime>,
        // Per-attempt audit status from the bound PFR. The canonical
        // sticky-Committed `status` field lives on the LFR side
        // (inherited via `LogicalFunctionRunDB`).
        pub physical_status: Option<PhysicalFunctionRunStatus>,
        pub function_version_patch_id: Option<FunctionVersionId>,
        // Bucket floors / aggregated metrics from the bound PFR. NULL
        // until the attempt terminates; metric_* additionally NULL for
        // non-output statuses (E/F/X/Y).
        pub ended_on_hour_ms: Option<EpochMs>,
        pub ended_on_day_ms: Option<EpochMs>,
        pub ended_on_week_ms: Option<EpochMs>,
        pub ended_on_month_ms: Option<EpochMs>,
        pub metric_table_versions: Option<TableVersionCount>,
        pub metric_rows: Option<RowCount>,
        pub metric_data_bytes: Option<ByteCount>,
    }

    /// Public DTO for a function run — one row per
    /// `(execution_id, function_id)`. Carries the LFR identity (canonical
    /// status, sticky-Committed semantics) merged with the per-attempt
    /// fields of the currently-bound PFR. Per-attempt drill-downs
    /// (workers, logs) go through `physical_function_run_id`.
    #[ta_objects::Dto]
    #[dto(list(on = FunctionRunDBWithNames))]
    pub struct FunctionRun {
        #[dto(list(filter, order_by))]
        pub id: LogicalFunctionRunId,
        #[dto(list(filter, order_by))]
        pub collection_id: CollectionId,
        #[dto(list(filter, filter_like, order_by))]
        pub execution_id: ExecutionId,
        #[dto(list(filter, filter_like, order_by))]
        pub transaction_id: TransactionId,
        #[dto(list(filter, order_by))]
        pub function_id: FunctionId,
        #[dto(list(filter, order_by))]
        pub function_version_id: FunctionVersionId,
        pub trigger: Trigger,
        #[dto(list(pagination_by = "+", filter))]
        pub triggered_on: TriggeredOn,
        #[dto(list(filter, order_by))]
        pub triggered_by_id: UserId,
        #[dto(list(filter, order_by))]
        pub status: LogicalFunctionRunStatus,

        #[dto(list(filter, filter_like, order_by))]
        pub name: FunctionName,
        #[dto(list(filter, filter_like, order_by))]
        pub collection: CollectionName,
        #[dto(list(filter, filter_like))]
        pub execution: Option<ExecutionName>,
        pub triggered_by: UserName,
        // Resolved per-attempt fields from the currently-bound PFR.
        // `physical_function_run_id` is the PFR id; NULL while
        // Pending/Voided. UI/CLI use this to drill into PFR-keyed
        // entities (workers, logs).
        #[dto(list(filter, order_by))]
        pub physical_function_run_id: Option<PhysicalFunctionRunId>,
        #[dto(list(filter, order_by))]
        pub started_on: Option<AtTime>,
        #[dto(list(filter, order_by))]
        pub ended_on: Option<AtTime>,
        // PFR-side per-attempt status (Running, Done, …). The canonical
        // LFR status — what callers use for committed-only filters —
        // lives in the `status` field above.
        #[dto(list(filter, order_by))]
        pub physical_status: Option<PhysicalFunctionRunStatus>,
        #[dto(list(filter, order_by))]
        pub function_version_patch_id: Option<FunctionVersionId>,
    }

    // Status update on the LFR identity row. Drives flow control
    // (readiness, dispatch, cancel, recover, commit). Per-attempt
    // wall-clock fields (started_on, ended_on) live on PhysicalFunctionRunDB.
    #[ta_objects::Dao]
    #[dao(update, sql_table = "logical_function_runs")]
    #[dxo(builder(try_from = UpdateWorkerExecution))]
    pub struct UpdateLogicalFunctionRunDB {
        pub status: LogicalFunctionRunStatus,
    }

    impl UpdateLogicalFunctionRunDB {
        pub async fn scheduled() -> Result<Self, TdError> {
            Ok(Self {
                status: LogicalFunctionRunStatus::Scheduled,
            })
        }

        pub async fn requested() -> Result<Self, TdError> {
            Ok(Self {
                status: LogicalFunctionRunStatus::Requested,
            })
        }

        pub async fn recover() -> Result<Self, TdError> {
            Ok(Self {
                status: LogicalFunctionRunStatus::ReScheduled,
            })
        }

        pub async fn cancel() -> Result<Self, TdError> {
            Ok(Self {
                status: LogicalFunctionRunStatus::Canceled,
            })
        }
    }

    #[ta_objects::Dao]
    #[dao(update, sql_table = "logical_function_runs")]
    pub struct CommitLogicalFunctionRunDB {
        pub status: LogicalFunctionRunStatus,
    }

    impl Default for CommitLogicalFunctionRunDB {
        fn default() -> Self {
            Self {
                status: LogicalFunctionRunStatus::Committed,
            }
        }
    }

    // -----------------------------------------------------------------------
    // logical_function_run_pointers — append-only pointer-event log
    // -----------------------------------------------------------------------

    #[ta_objects::Dao]
    #[dao(
        sql_table = "logical_function_run_pointers",
        order_by(col = "set_on", dir = "asc"),
        event_log,
        versioned(
            pk = "id",
            version_by = "set_on",
            partition_by = "logical_function_run_id"
        ),
        logical_pointer(
            logical_id_col = "logical_function_run_id",
            concrete_id_col = "physical_function_run_id",
            identity = LogicalFunctionRunDB,
            kind = "LFR"
        )
    )]
    pub struct LogicalFunctionRunPointerDB {
        #[builder(default)]
        pub id: LogicalFunctionRunPointerId,
        // Project of the LFR this event belongs to. Carried explicitly so the
        // scheduler's cross-project dispatch tick (bypass scope, no request
        // GUC) can append bind events without relying on the `app.project_id`
        // column default — fail-closed.
        pub project_id: ProjectId,
        pub logical_function_run_id: LogicalFunctionRunId,
        pub status: LogicalPointerStatus,
        // NULL on Pending and Voided rows; populated on Bound rows with the
        // concrete FR the LFR was bound to at this event.
        #[builder(default)]
        pub physical_function_run_id: Option<PhysicalFunctionRunId>,
        pub set_on: AtTime,
        pub set_by_id: UserId,
        pub reason: LogicalPointerReason,
    }

    // -----------------------------------------------------------------------
    // logical_table_data_versions (LTDV) — per-output plan-time identity
    // -----------------------------------------------------------------------

    #[ta_objects::Dao]
    #[dao(
        data_version,
        sql_table = "logical_table_data_versions",
        order_by(col = "triggered_on", dir = "desc"),
        versioned(pk = "id", version_by = "triggered_on", partition_by = "table_id")
    )]
    pub struct LogicalTableDataVersionDB {
        #[builder(default = "LogicalTableDataVersionId::new()")]
        #[dxo(extractor)]
        pub id: LogicalTableDataVersionId,
        /// The project this version belongs to. Inherited from the producing
        /// LFR at mint; threaded forward to the pointer-event rows so the RLS
        /// GUC matches end-to-end on the cross-project scheduler dispatch tick.
        pub project_id: ProjectId,
        pub collection_id: CollectionId,
        pub execution_id: ExecutionId,
        pub transaction_id: TransactionId,
        pub table_id: TableId,
        // NULL when the LTDV is a lazy-mint over a V1-born concrete TDV
        // whose producer pre-dates the logical layer.
        #[builder(default)]
        pub producer_logical_function_run_id: Option<LogicalFunctionRunId>,
        #[builder(default)]
        pub function_param_pos: Option<TableFunctionParamPos>,
        // Logical table identity is on `table_id` above. The *table version*
        // (i.e. which schema) the LTDV's concrete attempts produce lives
        // on each concrete TDV row, not here — replays can attach a
        // concrete TDV with a different table_version_id under the same
        // LTDV identity (e.g. when the function version evolves the
        // schema between attempts).
        pub triggered_on: TriggeredOn,
        pub triggered_by_id: UserId,
        // Equal by construction to the producing LFR's group_id (both
        // copied off the same FunctionNode at plan-build). No default: the
        // group is project-scoped and must be stamped explicitly.
        pub group_id: GroupId,
    }

    // LTDV identity rows whose producer LFR is not Canceled/Yanked — the
    // version resolver's "active" timeline.
    #[ta_objects::Dao]
    #[dao(sql_table = "logical_table_data_versions__active")]
    #[inherits(LogicalTableDataVersionDB)]
    pub struct ActiveLogicalTableDataVersionDB {}

    // Names-joined view of the LTDV identity row, merged with the
    // currently-bound PTDV's per-attempt fields (and the producer PFR's
    // started/ended/status reached through the PTDV). PTDV/PFR-derived
    // fields are NULL when no PTDV is currently bound (Pending/Voided).
    #[ta_objects::Dao]
    #[dao(sql_table = "table_data_versions__with_names")]
    #[inherits(LogicalTableDataVersionDB)]
    pub struct TableDataVersionDBWithNames {
        pub collection: CollectionName,
        pub execution: Option<ExecutionName>,
        pub triggered_by: UserName,
        // Resolved group display name from the joined groups row.
        pub group: Option<GroupName>,
        // PTDV id currently bound to this LTDV (resolved from the pointer
        // event log); NULL while Pending/Voided.
        pub physical_table_data_version_id: Option<PhysicalTableDataVersionId>,
        pub name: Option<TableName>,
        pub table_version_id: Option<TableVersionId>,
        pub function_version_id: Option<FunctionVersionId>,
        pub function_version_patch_id: Option<FunctionVersionId>,
        pub has_data: Option<HasData>,
        pub schema_changed: Option<SchemaChanged>,
        pub column_count: Option<ColumnCount>,
        pub row_count: Option<RowCount>,
        pub data_bytes: Option<ByteCount>,
        pub schema_hash: Option<SchemaHash>,
        pub with_data_table_data_version_id: Option<PhysicalTableDataVersionId>,
        pub physical_function_run_id: Option<PhysicalFunctionRunId>,
        pub started_on: Option<AtTime>,
        pub ended_on: Option<AtTime>,
        // Canonical (sticky-Committed) status from the LTDV's producer
        // LFR. Use this for committed-only filters.
        pub status: Option<LogicalFunctionRunStatus>,
        // PFR-side per-attempt status (Running, Done, …).
        pub physical_status: Option<PhysicalFunctionRunStatus>,
        pub function: Option<FunctionName>,
        pub storage_version: Option<StorageVersion>,
    }

    /// Public DTO for a table data version — one row per LTDV identity.
    /// Carries the logical identity merged with the per-attempt fields of
    /// the currently-bound PTDV (and that PTDV's producer PFR). The
    /// per-attempt drill (download, schema, sample) goes through
    /// `physical_table_data_version_id`.
    #[ta_objects::Dto]
    #[dto(list(on = TableDataVersionDBWithNames))]
    pub struct TableDataVersion {
        #[dto(list(pagination_by = "+", filter, filter_like))]
        pub id: LogicalTableDataVersionId,
        #[dto(list(filter, order_by))]
        pub collection_id: CollectionId,
        #[dto(list(filter, order_by))]
        pub table_id: TableId,
        #[dto(list(filter, filter_like, order_by))]
        pub execution_id: ExecutionId,
        #[dto(list(filter, filter_like, order_by))]
        pub transaction_id: TransactionId,
        #[dto(list(filter, order_by))]
        pub producer_logical_function_run_id: Option<LogicalFunctionRunId>,
        #[dto(list(filter, order_by))]
        pub function_param_pos: Option<TableFunctionParamPos>,
        #[dxo(builder(field = "triggered_on"))]
        #[dto(list(filter, order_by))]
        pub created_at: TriggeredOn,
        #[dto(list(filter, order_by))]
        pub triggered_by_id: UserId,

        #[dto(list(filter, filter_like, order_by))]
        pub collection: CollectionName,
        #[dto(list(filter, filter_like))]
        pub execution: Option<ExecutionName>,
        #[dxo(builder(field = "triggered_by"))]
        pub created_by: UserName,

        // Bound PTDV identity + derived per-attempt audit. NULL while
        // Pending/Voided.
        #[dto(list(filter, order_by))]
        pub physical_table_data_version_id: Option<PhysicalTableDataVersionId>,
        #[dto(list(filter, filter_like, order_by))]
        pub name: Option<TableName>,
        #[dto(list(filter, order_by))]
        pub table_version_id: Option<TableVersionId>,
        #[dto(list(filter, filter_like, order_by))]
        pub function_version_id: Option<FunctionVersionId>,
        #[dto(list(filter, order_by))]
        pub function_version_patch_id: Option<FunctionVersionId>,
        #[dxo(builder(field = "has_data"))]
        #[dto(list(filter))]
        pub data_changed: DataChanged,
        #[dxo(builder(field = "schema_changed"))]
        #[dto(list(filter))]
        pub schema_data_changed: SchemaDataChanged,
        pub column_count: Option<ColumnCount>,
        pub row_count: Option<RowCount>,
        pub data_bytes: Option<ByteCount>,
        #[dto(list(filter))]
        pub schema_hash: Option<SchemaHash>,
        pub with_data_table_data_version_id: Option<PhysicalTableDataVersionId>,
        #[dto(list(filter, order_by))]
        pub physical_function_run_id: Option<PhysicalFunctionRunId>,
        #[dto(list(filter, order_by))]
        pub started_on: Option<AtTime>,
        #[dto(list(filter, order_by))]
        pub ended_on: Option<AtTime>,
        // Canonical LFR-level status (sticky-Committed). Filter on this
        // for committed-only.
        #[dto(list(filter, order_by))]
        pub status: Option<LogicalFunctionRunStatus>,
        // PFR-side per-attempt status (Running, Done, …).
        #[dto(list(filter, order_by))]
        pub physical_status: Option<PhysicalFunctionRunStatus>,
        #[dto(list(filter, filter_like, order_by))]
        pub function: Option<FunctionName>,
        pub storage_version: Option<StorageVersion>,
    }

    // -----------------------------------------------------------------------
    // logical_table_data_version_pointers — append-only pointer-event log
    // -----------------------------------------------------------------------

    #[ta_objects::Dao]
    #[dao(
        sql_table = "logical_table_data_version_pointers",
        order_by(col = "set_on", dir = "asc"),
        event_log,
        versioned(
            pk = "id",
            version_by = "set_on",
            partition_by = "logical_table_data_version_id"
        ),
        logical_pointer(
            logical_id_col = "logical_table_data_version_id",
            concrete_id_col = "physical_table_data_version_id",
            identity = LogicalTableDataVersionDB,
            kind = "LTDV"
        )
    )]
    pub struct LogicalTableDataVersionPointerDB {
        #[builder(default)]
        pub id: LogicalTableDataVersionPointerId,
        // Project of the LTDV this event belongs to. Carried explicitly for
        // the same fail-closed reason as the LFR pointer above.
        pub project_id: ProjectId,
        pub logical_table_data_version_id: LogicalTableDataVersionId,
        pub status: LogicalPointerStatus,
        // NULL on Pending and Voided rows; populated on Bound rows with the
        // concrete TDV the LTDV was bound to at this event.
        #[builder(default)]
        pub physical_table_data_version_id: Option<PhysicalTableDataVersionId>,
        pub set_on: AtTime,
        pub set_by_id: UserId,
        pub reason: LogicalPointerReason,
    }

    // -----------------------------------------------------------------------
    // logical_function_run_inputs — V2 dependency-binding rows
    // -----------------------------------------------------------------------

    #[ta_objects::Dao]
    #[dao(sql_table = "logical_function_run_inputs")]
    pub struct LogicalFunctionRunInputDB {
        #[builder(default)]
        pub id: LogicalFunctionRunInputId,
        pub collection_id: CollectionId,
        pub execution_id: ExecutionId,
        pub transaction_id: TransactionId,
        pub logical_function_run_id: LogicalFunctionRunId,
        // NULL when the dep edge resolved to no LTDV at plan-build (e.g.
        // self-dep on first execution). The row is still emitted so the
        // worker yaml renders the input slot with table info derived from
        // `DependencyDB` at dispatch.
        #[builder(default)]
        pub requirement_logical_table_data_version_id: Option<LogicalTableDataVersionId>,
        // NULL on trigger-edge rows (mirrors V1 function_requirements).
        #[builder(default)]
        pub requirement_input_idx: Option<InputIdx>,
        #[builder(default)]
        pub requirement_dependency_pos: Option<DependencyPos>,
        pub requirement_version_pos: VersionPos,
    }
}
