//
// Copyright 2026 Tabs Data Inc.
//

use sea_query::{ColumnDef, Iden, Table, TableCreateStatement};

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
}
