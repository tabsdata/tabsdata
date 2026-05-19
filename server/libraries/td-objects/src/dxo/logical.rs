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

use crate::types::basic::{AtTime, LogicalPointerReason, LogicalPointerStatus, UserId};
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

    /// Build a fresh pointer-event row.
    fn build_event(
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
        AtTime, CollectionId, CollectionName, DependencyPos, ExecutionId, ExecutionName,
        FunctionId, FunctionName, FunctionRunStatus, FunctionVersionId, InputIdx,
        LogicalFunctionRunId, LogicalFunctionRunInputId, LogicalFunctionRunPointerId,
        LogicalPointerReason, LogicalPointerStatus, LogicalTableDataVersionId,
        LogicalTableDataVersionPointerId, PhysicalFunctionRunId, PhysicalTableDataVersionId,
        TableFunctionParamPos, TableId, TransactionId, Trigger, TriggeredOn, UserId, UserName,
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
        #[builder(default = FunctionRunStatus::Scheduled)]
        pub status: FunctionRunStatus,
    }

    // Names-joined view of the LFR identity row. Surfaces the
    // canonical (LFR-level) status for the listing endpoint that backs
    // the execution-plan graph.
    #[ta_objects::Dao]
    #[dao(sql_table = "logical_function_runs__with_names")]
    #[inherits(LogicalFunctionRunDB)]
    pub struct LogicalFunctionRunDBWithNames {
        pub name: FunctionName,
        pub collection: CollectionName,
        pub execution: Option<ExecutionName>,
        pub triggered_by: UserName,
    }

    /// Public DTO for a logical function run — one row per
    /// `(execution_id, function_id)`. Status reflects canonical
    /// flow-control state (sticky-Committed semantics apply); the
    /// per-attempt audit lives on `PhysicalFunctionRun`.
    #[ta_objects::Dto]
    #[dto(list(on = LogicalFunctionRunDBWithNames))]
    pub struct LogicalFunctionRun {
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
        pub status: FunctionRunStatus,

        #[dto(list(filter, filter_like, order_by))]
        pub name: FunctionName,
        #[dto(list(filter, filter_like, order_by))]
        pub collection: CollectionName,
        #[dto(list(filter, filter_like))]
        pub execution: Option<ExecutionName>,
        pub triggered_by: UserName,
    }

    // Status update on the LFR identity row. Drives flow control
    // (readiness, dispatch, cancel, recover, commit). Per-attempt
    // wall-clock fields (started_on, ended_on) live on PhysicalFunctionRunDB.
    #[ta_objects::Dao]
    #[dao(update, sql_table = "logical_function_runs")]
    #[dxo(builder(try_from = UpdateWorkerExecution))]
    pub struct UpdateLogicalFunctionRunDB {
        pub status: FunctionRunStatus,
    }

    impl UpdateLogicalFunctionRunDB {
        pub async fn scheduled() -> Result<Self, TdError> {
            Ok(Self {
                status: FunctionRunStatus::Scheduled,
            })
        }

        pub async fn requested() -> Result<Self, TdError> {
            Ok(Self {
                status: FunctionRunStatus::Requested,
            })
        }

        pub async fn recover() -> Result<Self, TdError> {
            Ok(Self {
                status: FunctionRunStatus::ReScheduled,
            })
        }

        pub async fn cancel() -> Result<Self, TdError> {
            Ok(Self {
                status: FunctionRunStatus::Canceled,
            })
        }
    }

    #[ta_objects::Dao]
    #[dao(update, sql_table = "logical_function_runs")]
    pub struct CommitLogicalFunctionRunDB {
        pub status: FunctionRunStatus,
    }

    impl Default for CommitLogicalFunctionRunDB {
        fn default() -> Self {
            Self {
                status: FunctionRunStatus::Committed,
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
