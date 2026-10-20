//
// Copyright 2026 Tabsdata Inc.
//

use sea_query::{ColumnDef, ForeignKey, ForeignKeyAction, Iden, Table, TableCreateStatement};

use super::collections::Collections;
use super::functions::Functions;
use super::physical_table_data_versions::PhysicalTableDataVersions;
use crate::schema::Entity;
use crate::schema::sql::{id_column, logical_id_column};

#[derive(Iden, Clone, Copy, strum::EnumIter)]
pub enum TablePartitions {
    Table,
    Id,
    ProjectId,
    CollectionId,
    TableId,
    TableVersionId,
    FunctionVersionId,
    TableDataVersionId,
    PartitionKey,
    PartitionDeleted,
}

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        vec![
            Table::create()
                .table(TablePartitions::Table)
                .col(id_column(TablePartitions::Id).not_null().primary_key())
                .col(id_column(TablePartitions::ProjectId).not_null())
                .col(id_column(TablePartitions::CollectionId).not_null())
                .col(id_column(TablePartitions::TableId).not_null())
                .col(id_column(TablePartitions::TableVersionId).not_null())
                .col(logical_id_column(TablePartitions::FunctionVersionId).not_null())
                .col(logical_id_column(TablePartitions::TableDataVersionId).not_null())
                .col(ColumnDef::new(TablePartitions::PartitionKey).text().null())
                .col(ColumnDef::new(TablePartitions::PartitionDeleted).boolean().null())
                .foreign_key(
                    ForeignKey::create()
                        .from(TablePartitions::Table, TablePartitions::CollectionId)
                        .to(Collections::Table, Collections::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .foreign_key(
                    ForeignKey::create()
                        .from(TablePartitions::Table, TablePartitions::FunctionVersionId)
                        .to(Functions::Table, Functions::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .foreign_key(
                    ForeignKey::create()
                        .from(TablePartitions::Table, TablePartitions::TableDataVersionId)
                        .to(PhysicalTableDataVersions::Table, PhysicalTableDataVersions::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .to_owned(),
        ]
    }
}
