//
// Copyright 2026 Tabs Data Inc.
//

//! Append-only pointer event log binding an LFR to its currently-bound PFR.
//!
//! Each mint / bind / redirect / void writes one row. The "current"
//! pointer for an LFR is the latest event row by `(set_on DESC, id DESC)`.
//! There is no separate "current" cache to keep in sync — the event log
//! is the source of truth.

use sea_query::{ColumnDef, ForeignKey, ForeignKeyAction, Iden, Index, IndexCreateStatement, IntoIden, Table, TableCreateStatement};

use super::logical_function_run_pointers_current::LogicalFunctionRunPointersCurrent;
use super::logical_function_runs::LogicalFunctionRuns;
use crate::schema::Entity;
use crate::schema::sql::{ImmutableTriggerDef, SeedCurrent, TriggerSpec, VersionedTriggerDef, cols_of};

#[derive(Iden, Clone, Copy, strum::EnumIter)]
pub enum LogicalFunctionRunPointers {
    Table,
    Id,
    ProjectId,
    LogicalFunctionRunId,
    Status,
    PhysicalFunctionRunId,
    SetOn,
    SetById,
    Reason,
}

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        vec![
            // logical_function_run_pointers — append-only event log.
            Table::create()
                .table(LogicalFunctionRunPointers::Table)
                .col(ColumnDef::new(LogicalFunctionRunPointers::Id).text().not_null().primary_key())
                .col(ColumnDef::new(LogicalFunctionRunPointers::ProjectId).text().not_null())
                .col(ColumnDef::new(LogicalFunctionRunPointers::LogicalFunctionRunId).text().not_null())
                .col(ColumnDef::new(LogicalFunctionRunPointers::Status).text().not_null())
                // Nullable: NULL on Pending (mint-time) and Voided rows.
                .col(ColumnDef::new(LogicalFunctionRunPointers::PhysicalFunctionRunId).text().null())
                .col(ColumnDef::new(LogicalFunctionRunPointers::SetOn).timestamp_with_time_zone().not_null())
                .col(ColumnDef::new(LogicalFunctionRunPointers::SetById).text().not_null())
                .col(ColumnDef::new(LogicalFunctionRunPointers::Reason).text().not_null())
                .foreign_key(
                    ForeignKey::create()
                        .from(LogicalFunctionRunPointers::Table, LogicalFunctionRunPointers::LogicalFunctionRunId)
                        .to(LogicalFunctionRuns::Table, LogicalFunctionRuns::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .to_owned(),
        ]
    }

    fn create_indices(&self) -> Vec<IndexCreateStatement> {
        vec![
            // LFR pointers: latest-pointer-per-LFR scan
            // (PARTITION BY logical_function_run_id ORDER BY set_on DESC, id DESC).
            Index::create()
                .name("logical_function_run_pointers___logical_function_run_id__set_on__id___idx")
                .table(LogicalFunctionRunPointers::Table)
                .col(LogicalFunctionRunPointers::LogicalFunctionRunId)
                .col(LogicalFunctionRunPointers::SetOn)
                .col(LogicalFunctionRunPointers::Id)
                .to_owned(),
            // LFR pointers: reverse lookup from concrete FR to the LFR(s) that
            // currently or historically point at it.
            Index::create()
                .name("logical_function_run_pointers___function_run_id___idx")
                .table(LogicalFunctionRunPointers::Table)
                .col(LogicalFunctionRunPointers::PhysicalFunctionRunId)
                .to_owned(),
        ]
    }

    fn create_db_triggers(&self) -> Vec<TriggerSpec> {
        vec![
            // Latest event wins: a later Voided/Pending overwrites an earlier
            // Bound, so the current row reflects being bound *now*.
            TriggerSpec::VersionedCurrent(VersionedTriggerDef {
                name: format!("{}__maintain_current", LogicalFunctionRunPointers::Table.to_string()),
                source_table: LogicalFunctionRunPointers::Table.into_iden(),
                target_table: LogicalFunctionRunPointersCurrent::Table.into_iden(),
                partition_col: LogicalFunctionRunPointers::LogicalFunctionRunId.into_iden(),
                version_col: LogicalFunctionRunPointers::SetOn.into_iden(),
                tie_break_col: LogicalFunctionRunPointers::Id.into_iden(),
                columns: cols_of::<LogicalFunctionRunPointers>(),
            }),
            TriggerSpec::Immutable(ImmutableTriggerDef {
                name: format!("{}__immutable", LogicalFunctionRunPointers::Table.to_string()),
                source_table: LogicalFunctionRunPointers::Table.into_iden(),
            }),
        ]
    }

    fn seed_current(&self) -> Vec<SeedCurrent> {
        vec![SeedCurrent {
            target: LogicalFunctionRunPointersCurrent::Table.into_iden(),
            source: LogicalFunctionRunPointers::Table.into_iden(),
            partition_col: LogicalFunctionRunPointers::LogicalFunctionRunId.into_iden(),
            version_col: LogicalFunctionRunPointers::SetOn.into_iden(),
            columns: cols_of::<LogicalFunctionRunPointers>(),
        }]
    }
}
