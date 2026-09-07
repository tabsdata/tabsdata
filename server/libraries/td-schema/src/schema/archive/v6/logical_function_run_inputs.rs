//
// Copyright 2026 Tabsdata Inc.
//

//! V2 dependency-binding rows: one per (LFR, input slot, version-pos).

use sea_query::{ColumnDef, ForeignKey, ForeignKeyAction, Iden, Index, IndexCreateStatement, Table, TableCreateStatement};

use super::collections::Collections;
use super::executions::Executions;
use super::logical_function_runs::LogicalFunctionRuns;
use super::logical_table_data_versions::LogicalTableDataVersions;
use super::transactions::Transactions;
use crate::schema::Entity;
use crate::schema::sql::id_column;

#[derive(Iden, Clone, Copy, strum::EnumIter)]
pub enum LogicalFunctionRunInputs {
    Table,
    Id,
    ProjectId,
    CollectionId,
    ExecutionId,
    TransactionId,
    LogicalFunctionRunId,
    RequirementLogicalTableDataVersionId,
    RequirementInputIdx,
    RequirementDependencyPos,
    RequirementVersionPos,
}

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        vec![
            // logical_function_run_inputs — V2 binding rows (one per input/version-pos)
            Table::create()
                .table(LogicalFunctionRunInputs::Table)
                .col(id_column(LogicalFunctionRunInputs::Id).not_null().primary_key())
                .col(id_column(LogicalFunctionRunInputs::ProjectId).not_null())
                .col(id_column(LogicalFunctionRunInputs::CollectionId).not_null())
                .col(id_column(LogicalFunctionRunInputs::ExecutionId).not_null())
                .col(id_column(LogicalFunctionRunInputs::TransactionId).not_null())
                .col(id_column(LogicalFunctionRunInputs::LogicalFunctionRunId).not_null())
                // Nullable: a dependency edge that didn't resolve to any LTDV
                // at plan-build (e.g. self-dep on first execution, no prior
                // upstream) still emits an FFR_input row so the worker yaml
                // can render the input slot. Worker-yaml falls back to
                // `DependencyDB` for the static dep-declaration info.
                .col(id_column(LogicalFunctionRunInputs::RequirementLogicalTableDataVersionId).null())
                // Nullable for trigger-edge rows (no dependency_pos / input_idx).
                .col(ColumnDef::new(LogicalFunctionRunInputs::RequirementInputIdx).integer().null())
                .col(ColumnDef::new(LogicalFunctionRunInputs::RequirementDependencyPos).integer().null())
                .col(ColumnDef::new(LogicalFunctionRunInputs::RequirementVersionPos).integer().not_null())
                .foreign_key(
                    ForeignKey::create()
                        .from(LogicalFunctionRunInputs::Table, LogicalFunctionRunInputs::CollectionId)
                        .to(Collections::Table, Collections::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .foreign_key(
                    ForeignKey::create()
                        .from(LogicalFunctionRunInputs::Table, LogicalFunctionRunInputs::ExecutionId)
                        .to(Executions::Table, Executions::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .foreign_key(
                    ForeignKey::create()
                        .from(LogicalFunctionRunInputs::Table, LogicalFunctionRunInputs::TransactionId)
                        .to(Transactions::Table, Transactions::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .foreign_key(
                    ForeignKey::create()
                        .from(LogicalFunctionRunInputs::Table, LogicalFunctionRunInputs::LogicalFunctionRunId)
                        .to(LogicalFunctionRuns::Table, LogicalFunctionRuns::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .foreign_key(
                    ForeignKey::create()
                        .from(LogicalFunctionRunInputs::Table, LogicalFunctionRunInputs::RequirementLogicalTableDataVersionId)
                        .to(LogicalTableDataVersions::Table, LogicalTableDataVersions::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .to_owned(),
        ]
    }

    fn create_indices(&self) -> Vec<IndexCreateStatement> {
        vec![
            // LFR inputs: scheduler readiness query joins on logical_function_run_id.
            Index::create()
                .name("logical_function_run_inputs___logical_function_run_id___idx")
                .table(LogicalFunctionRunInputs::Table)
                .col(LogicalFunctionRunInputs::LogicalFunctionRunId)
                .to_owned(),
            // LFR inputs: lookup by execution (plan rebuild and read paths).
            Index::create()
                .name("logical_function_run_inputs___execution_id___idx")
                .table(LogicalFunctionRunInputs::Table)
                .col(LogicalFunctionRunInputs::ExecutionId)
                .to_owned(),
            // LFR inputs: consumers of an LTDV (downstream cascade, unused-output anti-join).
            // Short prefix: the spelled-out name exceeds PostgreSQL's 63-byte
            // identifier limit and would be silently truncated.
            Index::create()
                .name("lfri___requirement_logical_table_data_version_id___idx")
                .table(LogicalFunctionRunInputs::Table)
                .col(LogicalFunctionRunInputs::RequirementLogicalTableDataVersionId)
                .to_owned(),
        ]
    }
}
