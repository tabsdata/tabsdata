//
// Copyright 2026 Tabs Data Inc.
//

use sea_query::{ColumnDef, ForeignKey, ForeignKeyAction, Iden, Table, TableCreateStatement};

use super::collections::Collections;
use super::functions::Functions;
use super::physical_table_data_versions::PhysicalTableDataVersions;
use crate::schema::Entity;

#[derive(Iden, Clone, Copy, strum::EnumIter)]
pub enum TablePartitions {
    Table,
    ProjectId,
    Id,
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
                .col(ColumnDef::new(TablePartitions::Id).text().not_null().primary_key())
                .col(ColumnDef::new(TablePartitions::CollectionId).text().not_null())
                .col(ColumnDef::new(TablePartitions::TableId).text().not_null())
                .col(ColumnDef::new(TablePartitions::TableVersionId).text().not_null())
                .col(ColumnDef::new(TablePartitions::FunctionVersionId).text().not_null())
                .col(ColumnDef::new(TablePartitions::TableDataVersionId).text().not_null())
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
                .col(ColumnDef::new(TablePartitions::ProjectId).text().not_null())
                .to_owned(),
        ]
    }
}
