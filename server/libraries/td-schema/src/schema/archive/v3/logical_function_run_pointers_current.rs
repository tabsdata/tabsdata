//
// Copyright 2026 Tabsdata Inc.
//

use sea_query::{ColumnDef, Iden, Index, IndexCreateStatement, Table, TableCreateStatement};

use crate::schema::Entity;

#[derive(Iden, Clone, Copy, strum::EnumIter)]
pub enum LogicalFunctionRunPointersCurrent {
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
        // Latest pointer event per LFR, trigger-maintained: views resolve the
        // current binding here instead of ranking the event log.
        vec![
            Table::create()
                .table(LogicalFunctionRunPointersCurrent::Table)
                .col(ColumnDef::new(LogicalFunctionRunPointersCurrent::Id).text().not_null().primary_key())
                .col(ColumnDef::new(LogicalFunctionRunPointersCurrent::ProjectId).text().not_null())
                .col(ColumnDef::new(LogicalFunctionRunPointersCurrent::LogicalFunctionRunId).text().not_null().unique_key())
                .col(ColumnDef::new(LogicalFunctionRunPointersCurrent::Status).text().not_null())
                .col(ColumnDef::new(LogicalFunctionRunPointersCurrent::PhysicalFunctionRunId).text().null())
                .col(ColumnDef::new(LogicalFunctionRunPointersCurrent::SetOn).timestamp_with_time_zone().not_null())
                .col(ColumnDef::new(LogicalFunctionRunPointersCurrent::SetById).text().not_null())
                .col(ColumnDef::new(LogicalFunctionRunPointersCurrent::Reason).text().not_null())
                .to_owned(),
        ]
    }

    fn create_indices(&self) -> Vec<IndexCreateStatement> {
        // Reverse hop for window-driven reads: /metrics selects the window on
        // physical_function_runs.ended_on (indexed) and resolves "which LFR is
        // currently bound to this attempt" through this index instead of
        // scanning the table. (Name kept short of PostgreSQL's 63-char cap.)
        vec![
            Index::create()
                .name("lfr_pointers_current___physical_function_run_id___idx")
                .table(LogicalFunctionRunPointersCurrent::Table)
                .col(LogicalFunctionRunPointersCurrent::PhysicalFunctionRunId)
                .to_owned(),
        ]
    }
}
