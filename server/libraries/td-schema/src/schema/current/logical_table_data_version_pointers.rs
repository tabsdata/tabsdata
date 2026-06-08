//
// Copyright 2026 Tabs Data Inc.
//

//! Append-only pointer event log binding an LTDV to its currently-bound PTDV.

use sea_query::{ColumnDef, ForeignKey, ForeignKeyAction, Iden, Index, IndexCreateStatement, IntoIden, Table, TableCreateStatement};

use super::logical_table_data_versions::LogicalTableDataVersions;
use crate::schema::Entity;
use crate::schema::sql::{ImmutableTriggerDef, TriggerSpec};

#[derive(Iden, Clone, Copy, strum::EnumIter)]
pub enum LogicalTableDataVersionPointers {
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
        vec![
            // logical_table_data_version_pointers — append-only event log.
            Table::create()
                .table(LogicalTableDataVersionPointers::Table)
                .col(ColumnDef::new(LogicalTableDataVersionPointers::Id).text().not_null().primary_key())
                .col(ColumnDef::new(LogicalTableDataVersionPointers::ProjectId).text().not_null())
                .col(ColumnDef::new(LogicalTableDataVersionPointers::LogicalTableDataVersionId).text().not_null())
                .col(ColumnDef::new(LogicalTableDataVersionPointers::Status).text().not_null())
                // Nullable: NULL on Pending (mint-time) and Voided rows.
                .col(ColumnDef::new(LogicalTableDataVersionPointers::PhysicalTableDataVersionId).text().null())
                .col(ColumnDef::new(LogicalTableDataVersionPointers::SetOn).timestamp_with_time_zone().not_null())
                .col(ColumnDef::new(LogicalTableDataVersionPointers::SetById).text().not_null())
                .col(ColumnDef::new(LogicalTableDataVersionPointers::Reason).text().not_null())
                .foreign_key(
                    ForeignKey::create()
                        .from(LogicalTableDataVersionPointers::Table, LogicalTableDataVersionPointers::LogicalTableDataVersionId)
                        .to(LogicalTableDataVersions::Table, LogicalTableDataVersions::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .to_owned(),
        ]
    }

    fn create_indices(&self) -> Vec<IndexCreateStatement> {
        vec![
            // LTDV pointers: latest-pointer-per-LTDV scan
            // (PARTITION BY logical_table_data_version_id ORDER BY set_on DESC, id DESC).
            Index::create()
                .name("logical_table_data_version_pointers___logical_table_data_version_id__set_on__id___idx")
                .table(LogicalTableDataVersionPointers::Table)
                .col(LogicalTableDataVersionPointers::LogicalTableDataVersionId)
                .col(LogicalTableDataVersionPointers::SetOn)
                .col(LogicalTableDataVersionPointers::Id)
                .to_owned(),
            // LTDV pointers: reverse lookup from concrete TDV.
            Index::create()
                .name("logical_table_data_version_pointers___table_data_version_id___idx")
                .table(LogicalTableDataVersionPointers::Table)
                .col(LogicalTableDataVersionPointers::PhysicalTableDataVersionId)
                .to_owned(),
        ]
    }

    fn create_db_triggers(&self) -> Vec<TriggerSpec> {
        vec![TriggerSpec::Immutable(ImmutableTriggerDef {
            name: format!("{}__immutable", LogicalTableDataVersionPointers::Table.to_string()),
            source_table: LogicalTableDataVersionPointers::Table.into_iden(),
        })]
    }
}
