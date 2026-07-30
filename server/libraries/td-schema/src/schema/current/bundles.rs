//
// Copyright 2026 Tabsdata Inc.
//

use sea_query::{ColumnDef, ForeignKey, ForeignKeyAction, Iden, Index, IndexCreateStatement, Table, TableCreateStatement};

use super::collections::Collections;
use crate::schema::Entity;

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
                .col(ColumnDef::new(Bundles::Id).text().not_null().primary_key())
                .col(ColumnDef::new(Bundles::ProjectId).text().not_null())
                .col(ColumnDef::new(Bundles::CollectionId).text().not_null())
                .col(ColumnDef::new(Bundles::Hash).text().not_null())
                // Storage prefix frozen at upload = collection.base_path. The bundle
                // row is durable (kept, marked on claim), so it self-locates its .tar.zst.
                .col(ColumnDef::new(Bundles::BasePath).text().not_null())
                // Lifecycle marker: Pending (uploaded) → Registered (claimed by a
                // function register/update/patch). Rows are never deleted.
                .col(ColumnDef::new(Bundles::Status).text().not_null())
                .col(ColumnDef::new(Bundles::CreatedOn).timestamp_with_time_zone().not_null())
                .col(ColumnDef::new(Bundles::CreatedById).text().not_null())
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

    fn create_indices(&self) -> Vec<IndexCreateStatement> {
        vec![Index::create().name("bundles___collection_id___idx").table(Bundles::Table).col(Bundles::CollectionId).to_owned()]
    }
}
