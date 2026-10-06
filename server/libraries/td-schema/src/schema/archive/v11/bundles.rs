//
// Copyright 2026 Tabsdata Inc.
//

use sea_query::{ColumnDef, ForeignKey, ForeignKeyAction, Iden, Table, TableCreateStatement};

use super::collections::Collections;
use crate::schema::Entity;
use crate::schema::sql::id_column;

#[derive(Iden, Clone, Copy, strum::EnumIter)]
pub enum Bundles {
    Table,
    Id,
    ProjectId,
    CollectionId,
    Hash,
    BasePath,
    Status,
    CreatedOn,
    CreatedById,
}

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        vec![
            Table::create()
                .table(Bundles::Table)
                .col(id_column(Bundles::Id).not_null().primary_key())
                .col(id_column(Bundles::ProjectId).not_null())
                .col(id_column(Bundles::CollectionId).not_null())
                .col(ColumnDef::new(Bundles::Hash).text().not_null())
                // Storage prefix frozen at upload = collection.base_path. The bundle
                // row is durable (kept, marked on claim), so it self-locates its .tar.zst.
                .col(ColumnDef::new(Bundles::BasePath).text().not_null())
                // Lifecycle marker: Pending (uploaded) → Registered (claimed by a
                // function register/update/patch). Rows are never deleted.
                .col(ColumnDef::new(Bundles::Status).text().not_null())
                .col(ColumnDef::new(Bundles::CreatedOn).timestamp_with_time_zone().not_null())
                .col(id_column(Bundles::CreatedById).not_null())
                .foreign_key(
                    ForeignKey::create()
                        .from(Bundles::Table, Bundles::CollectionId)
                        .to(Collections::Table, Collections::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .to_owned(),
        ]
    }
}
