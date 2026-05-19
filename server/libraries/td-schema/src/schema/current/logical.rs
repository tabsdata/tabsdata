//
// Copyright 2026 Tabs Data Inc.
//

//! Logical-version layer.
//!
//! Five tables that together hold the immutable plan-time identities of
//! function runs and table data versions, plus the append-only event log of
//! every pointer change between a logical identity and its current
//! concrete materialization.
//!
//! - `logical_function_runs`            — per-run plan-time identity (immutable)
//! - `logical_function_run_pointers`    — append-only pointer event log (LFR)
//! - `logical_table_data_versions`      — per-output plan-time identity (immutable)
//! - `logical_table_data_version_pointers` — append-only pointer event log (LTDV)
//! - `logical_function_run_inputs`      — V2 dependency-binding rows
//!
//! The identity tables are insert-once at plan-build (and at lazy-mint over
//! V1-born concrete TDVs); they never update. The pointer-event tables are
//! pure INSERT — each mint / bind / redirect / void writes one row. The
//! "current" pointer for a logical id is the latest event row by
//! `(set_on DESC, id DESC)`. There is no separate "current" cache to keep
//! in sync — the event log is the source of truth.

use sea_query::{ColumnDef, ForeignKey, ForeignKeyAction, Iden, Table, TableCreateStatement};

use super::collections::Collections;
use super::functions::{Executions, Transactions};
use crate::schema::backend::{Backend, ImmutableTriggerDef, StickyTerminalsTriggerDef};

// ---------------------------------------------------------------------------
// Iden enums
// ---------------------------------------------------------------------------

#[derive(Iden)]
pub enum LogicalFunctionRuns {
    Table,
    Id,
    CollectionId,
    ExecutionId,
    TransactionId,
    FunctionId,
    FunctionVersionId,
    Trigger,
    TriggeredOn,
    TriggeredById,
    Status,
}

#[derive(Iden)]
pub enum LogicalFunctionRunPointers {
    Table,
    Id,
    LogicalFunctionRunId,
    Status,
    PhysicalFunctionRunId,
    SetOn,
    SetById,
    Reason,
}

#[derive(Iden)]
pub enum LogicalTableDataVersions {
    Table,
    Id,
    CollectionId,
    ExecutionId,
    TransactionId,
    TableId,
    ProducerLogicalFunctionRunId,
    FunctionParamPos,
    TriggeredOn,
    TriggeredById,
}

#[derive(Iden)]
pub enum LogicalTableDataVersionPointers {
    Table,
    Id,
    LogicalTableDataVersionId,
    Status,
    PhysicalTableDataVersionId,
    SetOn,
    SetById,
    Reason,
}

#[derive(Iden)]
pub enum LogicalFunctionRunInputs {
    Table,
    Id,
    CollectionId,
    ExecutionId,
    TransactionId,
    LogicalFunctionRunId,
    RequirementLogicalTableDataVersionId,
    RequirementInputIdx,
    RequirementDependencyPos,
    RequirementVersionPos,
}

// ---------------------------------------------------------------------------
// Table definitions
// ---------------------------------------------------------------------------

pub fn tables() -> Vec<TableCreateStatement> {
    vec![
        // logical_function_runs — identity row, insert-once.
        Table::create()
            .table(LogicalFunctionRuns::Table)
            .col(
                ColumnDef::new(LogicalFunctionRuns::Id)
                    .text()
                    .not_null()
                    .primary_key(),
            )
            .col(
                ColumnDef::new(LogicalFunctionRuns::CollectionId)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(LogicalFunctionRuns::ExecutionId)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(LogicalFunctionRuns::TransactionId)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(LogicalFunctionRuns::FunctionId)
                    .text()
                    .not_null(),
            )
            // Plan-time fvid the LFR was minted against; topology
            // reference for primitives. Set once at plan-build.
            .col(
                ColumnDef::new(LogicalFunctionRuns::FunctionVersionId)
                    .text()
                    .not_null()
                    .default(""),
            )
            .col(
                ColumnDef::new(LogicalFunctionRuns::Trigger)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(LogicalFunctionRuns::TriggeredOn)
                    .timestamp()
                    .not_null(),
            )
            .col(
                ColumnDef::new(LogicalFunctionRuns::TriggeredById)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(LogicalFunctionRuns::Status)
                    .text()
                    .not_null(),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(
                        LogicalFunctionRuns::Table,
                        LogicalFunctionRuns::CollectionId,
                    )
                    .to(Collections::Table, Collections::Id)
                    .on_delete(ForeignKeyAction::Restrict)
                    .on_update(ForeignKeyAction::Restrict),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(LogicalFunctionRuns::Table, LogicalFunctionRuns::ExecutionId)
                    .to(Executions::Table, Executions::Id)
                    .on_delete(ForeignKeyAction::Restrict)
                    .on_update(ForeignKeyAction::Restrict),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(
                        LogicalFunctionRuns::Table,
                        LogicalFunctionRuns::TransactionId,
                    )
                    .to(Transactions::Table, Transactions::Id)
                    .on_delete(ForeignKeyAction::Restrict)
                    .on_update(ForeignKeyAction::Restrict),
            )
            .to_owned(),
        // logical_function_run_pointers — append-only event log.
        Table::create()
            .table(LogicalFunctionRunPointers::Table)
            .col(
                ColumnDef::new(LogicalFunctionRunPointers::Id)
                    .text()
                    .not_null()
                    .primary_key(),
            )
            .col(
                ColumnDef::new(LogicalFunctionRunPointers::LogicalFunctionRunId)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(LogicalFunctionRunPointers::Status)
                    .text()
                    .not_null(),
            )
            // Nullable: NULL on Pending (mint-time) and Voided rows.
            .col(
                ColumnDef::new(LogicalFunctionRunPointers::PhysicalFunctionRunId)
                    .text()
                    .null(),
            )
            .col(
                ColumnDef::new(LogicalFunctionRunPointers::SetOn)
                    .timestamp()
                    .not_null(),
            )
            .col(
                ColumnDef::new(LogicalFunctionRunPointers::SetById)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(LogicalFunctionRunPointers::Reason)
                    .text()
                    .not_null(),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(
                        LogicalFunctionRunPointers::Table,
                        LogicalFunctionRunPointers::LogicalFunctionRunId,
                    )
                    .to(LogicalFunctionRuns::Table, LogicalFunctionRuns::Id)
                    .on_delete(ForeignKeyAction::Restrict)
                    .on_update(ForeignKeyAction::Restrict),
            )
            .to_owned(),
        // logical_table_data_versions — identity row, insert-once.
        Table::create()
            .table(LogicalTableDataVersions::Table)
            .col(
                ColumnDef::new(LogicalTableDataVersions::Id)
                    .text()
                    .not_null()
                    .primary_key(),
            )
            .col(
                ColumnDef::new(LogicalTableDataVersions::CollectionId)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(LogicalTableDataVersions::ExecutionId)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(LogicalTableDataVersions::TransactionId)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(LogicalTableDataVersions::TableId)
                    .text()
                    .not_null(),
            )
            // Nullable: NULL when the LTDV is a lazy-mint over a V1-born
            // physical_table_data_versions row whose producer pre-dates the logic
            // layer, so no LFR exists for the producer.
            .col(
                ColumnDef::new(LogicalTableDataVersions::ProducerLogicalFunctionRunId)
                    .text()
                    .null(),
            )
            // Nullable for the same reason as ProducerLogicalFunctionRunId.
            .col(
                ColumnDef::new(LogicalTableDataVersions::FunctionParamPos)
                    .integer()
                    .null(),
            )
            .col(
                ColumnDef::new(LogicalTableDataVersions::TriggeredOn)
                    .timestamp()
                    .not_null(),
            )
            .col(
                ColumnDef::new(LogicalTableDataVersions::TriggeredById)
                    .text()
                    .not_null(),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(
                        LogicalTableDataVersions::Table,
                        LogicalTableDataVersions::CollectionId,
                    )
                    .to(Collections::Table, Collections::Id)
                    .on_delete(ForeignKeyAction::Restrict)
                    .on_update(ForeignKeyAction::Restrict),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(
                        LogicalTableDataVersions::Table,
                        LogicalTableDataVersions::ExecutionId,
                    )
                    .to(Executions::Table, Executions::Id)
                    .on_delete(ForeignKeyAction::Restrict)
                    .on_update(ForeignKeyAction::Restrict),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(
                        LogicalTableDataVersions::Table,
                        LogicalTableDataVersions::TransactionId,
                    )
                    .to(Transactions::Table, Transactions::Id)
                    .on_delete(ForeignKeyAction::Restrict)
                    .on_update(ForeignKeyAction::Restrict),
            )
            // No FK on table_id: this is the logical table id (groups
            // multiple versions), matching V1 physical_table_data_versions.table_id
            // which is similarly unconstrained.
            .foreign_key(
                ForeignKey::create()
                    .from(
                        LogicalTableDataVersions::Table,
                        LogicalTableDataVersions::ProducerLogicalFunctionRunId,
                    )
                    .to(LogicalFunctionRuns::Table, LogicalFunctionRuns::Id)
                    .on_delete(ForeignKeyAction::Restrict)
                    .on_update(ForeignKeyAction::Restrict),
            )
            .to_owned(),
        // logical_table_data_version_pointers — append-only event log.
        Table::create()
            .table(LogicalTableDataVersionPointers::Table)
            .col(
                ColumnDef::new(LogicalTableDataVersionPointers::Id)
                    .text()
                    .not_null()
                    .primary_key(),
            )
            .col(
                ColumnDef::new(LogicalTableDataVersionPointers::LogicalTableDataVersionId)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(LogicalTableDataVersionPointers::Status)
                    .text()
                    .not_null(),
            )
            // Nullable: NULL on Pending (mint-time) and Voided rows.
            .col(
                ColumnDef::new(LogicalTableDataVersionPointers::PhysicalTableDataVersionId)
                    .text()
                    .null(),
            )
            .col(
                ColumnDef::new(LogicalTableDataVersionPointers::SetOn)
                    .timestamp()
                    .not_null(),
            )
            .col(
                ColumnDef::new(LogicalTableDataVersionPointers::SetById)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(LogicalTableDataVersionPointers::Reason)
                    .text()
                    .not_null(),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(
                        LogicalTableDataVersionPointers::Table,
                        LogicalTableDataVersionPointers::LogicalTableDataVersionId,
                    )
                    .to(
                        LogicalTableDataVersions::Table,
                        LogicalTableDataVersions::Id,
                    )
                    .on_delete(ForeignKeyAction::Restrict)
                    .on_update(ForeignKeyAction::Restrict),
            )
            .to_owned(),
        // logical_function_run_inputs — V2 binding rows (one per input/version-pos)
        Table::create()
            .table(LogicalFunctionRunInputs::Table)
            .col(
                ColumnDef::new(LogicalFunctionRunInputs::Id)
                    .text()
                    .not_null()
                    .primary_key(),
            )
            .col(
                ColumnDef::new(LogicalFunctionRunInputs::CollectionId)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(LogicalFunctionRunInputs::ExecutionId)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(LogicalFunctionRunInputs::TransactionId)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(LogicalFunctionRunInputs::LogicalFunctionRunId)
                    .text()
                    .not_null(),
            )
            // Nullable: a dependency edge that didn't resolve to any LTDV
            // at plan-build (e.g. self-dep on first execution, no prior
            // upstream) still emits an FFR_input row so the worker yaml
            // can render the input slot. Worker-yaml falls back to
            // `DependencyDB` for the static dep-declaration info.
            .col(
                ColumnDef::new(LogicalFunctionRunInputs::RequirementLogicalTableDataVersionId)
                    .text()
                    .null(),
            )
            // Nullable for trigger-edge rows (no dependency_pos / input_idx).
            .col(
                ColumnDef::new(LogicalFunctionRunInputs::RequirementInputIdx)
                    .integer()
                    .null(),
            )
            .col(
                ColumnDef::new(LogicalFunctionRunInputs::RequirementDependencyPos)
                    .integer()
                    .null(),
            )
            .col(
                ColumnDef::new(LogicalFunctionRunInputs::RequirementVersionPos)
                    .integer()
                    .not_null(),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(
                        LogicalFunctionRunInputs::Table,
                        LogicalFunctionRunInputs::CollectionId,
                    )
                    .to(Collections::Table, Collections::Id)
                    .on_delete(ForeignKeyAction::Restrict)
                    .on_update(ForeignKeyAction::Restrict),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(
                        LogicalFunctionRunInputs::Table,
                        LogicalFunctionRunInputs::ExecutionId,
                    )
                    .to(Executions::Table, Executions::Id)
                    .on_delete(ForeignKeyAction::Restrict)
                    .on_update(ForeignKeyAction::Restrict),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(
                        LogicalFunctionRunInputs::Table,
                        LogicalFunctionRunInputs::TransactionId,
                    )
                    .to(Transactions::Table, Transactions::Id)
                    .on_delete(ForeignKeyAction::Restrict)
                    .on_update(ForeignKeyAction::Restrict),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(
                        LogicalFunctionRunInputs::Table,
                        LogicalFunctionRunInputs::LogicalFunctionRunId,
                    )
                    .to(LogicalFunctionRuns::Table, LogicalFunctionRuns::Id)
                    .on_delete(ForeignKeyAction::Restrict)
                    .on_update(ForeignKeyAction::Restrict),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(
                        LogicalFunctionRunInputs::Table,
                        LogicalFunctionRunInputs::RequirementLogicalTableDataVersionId,
                    )
                    .to(
                        LogicalTableDataVersions::Table,
                        LogicalTableDataVersions::Id,
                    )
                    .on_delete(ForeignKeyAction::Restrict)
                    .on_update(ForeignKeyAction::Restrict),
            )
            .to_owned(),
    ]
}

/// Triggers that lift logical-layer invariants into the DB:
///
/// - sticky-terminal guard on `logical_function_runs.status` for the
///   truly-final states (Committed/Canceled/Yanked) — Done/Failed are
///   intentionally allowed to transition (e.g. `Done → Committed`,
///   `Failed → Done` via replay heal).
/// - insert-only event-log enforcement on both pointer tables.
pub fn triggers(b: &Backend) -> Vec<String> {
    let mut out = Vec::new();
    out.push(
        b.create_sticky_terminals_trigger(&StickyTerminalsTriggerDef {
            name: "logical_function_runs__sticky_terminals",
            source_table: "logical_function_runs",
            status_col: "status",
            terminal_values: &["C", "X", "Y"],
        }),
    );
    out.extend(b.create_immutable_triggers(&ImmutableTriggerDef {
        name: "logical_function_run_pointers__immutable",
        source_table: "logical_function_run_pointers",
    }));
    out.extend(b.create_immutable_triggers(&ImmutableTriggerDef {
        name: "logical_table_data_version_pointers__immutable",
        source_table: "logical_table_data_version_pointers",
    }));
    out
}
