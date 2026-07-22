//
// Copyright 2026 Tabsdata Inc.
//

use sea_query::{ColumnDef, Iden, Table, TableCreateStatement};

use crate::schema::Entity;

#[derive(Iden, Clone, Copy, strum::EnumIter)]
pub enum LogicalTableDataVersionPointersCurrent {
    Table,
    Id,
    ProjectId,
    LogicalTableDataVersionId,
    Status,
    PhysicalTableDataVersionId,
    SetOn,
    SetById,
    Reason,
}

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        // Latest pointer event per LTDV, trigger-maintained: views resolve the
        // current binding here instead of ranking the event log.
        vec![
            Table::create()
                .table(LogicalTableDataVersionPointersCurrent::Table)
                .col(ColumnDef::new(LogicalTableDataVersionPointersCurrent::Id).text().not_null().primary_key())
                .col(ColumnDef::new(LogicalTableDataVersionPointersCurrent::ProjectId).text().not_null())
                .col(ColumnDef::new(LogicalTableDataVersionPointersCurrent::LogicalTableDataVersionId).text().not_null().unique_key())
                .col(ColumnDef::new(LogicalTableDataVersionPointersCurrent::Status).text().not_null())
                .col(ColumnDef::new(LogicalTableDataVersionPointersCurrent::PhysicalTableDataVersionId).text().null())
                .col(ColumnDef::new(LogicalTableDataVersionPointersCurrent::SetOn).timestamp_with_time_zone().not_null())
                .col(ColumnDef::new(LogicalTableDataVersionPointersCurrent::SetById).text().not_null())
                .col(ColumnDef::new(LogicalTableDataVersionPointersCurrent::Reason).text().not_null())
                .to_owned(),
        ]
    }
}
