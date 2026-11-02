//
// Copyright 2026 Tabsdata Inc.
//

use sea_query::{ColumnDef, ForeignKey, ForeignKeyAction, Iden, Index, IndexCreateStatement, Table, TableCreateStatement};

use super::connection_catalogs::ConnectionCatalogs;
use crate::schema::Entity;
use crate::schema::sql::id_column;

#[derive(Iden, Clone, Copy, strum::EnumIter)]
pub enum ConnectionEntities {
    Table,
    Id,
    ConnectionId,
    ProjectId,
    Fqn,
    EntityKind,
    FieldNames,
    FieldCount,
    Doc,
}

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        vec![
            Table::create()
                .table(ConnectionEntities::Table)
                .col(id_column(ConnectionEntities::Id).not_null().primary_key())
                .col(id_column(ConnectionEntities::ConnectionId).not_null())
                .col(id_column(ConnectionEntities::ProjectId).not_null())
                .col(ColumnDef::new(ConnectionEntities::Fqn).text().not_null())
                .col(ColumnDef::new(ConnectionEntities::EntityKind).text().not_null())
                .col(ColumnDef::new(ConnectionEntities::FieldNames).text().not_null().default(""))
                .col(ColumnDef::new(ConnectionEntities::FieldCount).integer().not_null().default(0))
                .col(ColumnDef::new(ConnectionEntities::Doc).json_binary().not_null())
                .foreign_key(
                    ForeignKey::create()
                        .from(ConnectionEntities::Table, ConnectionEntities::ConnectionId)
                        .to(ConnectionCatalogs::Table, ConnectionCatalogs::ConnectionId)
                        .on_delete(ForeignKeyAction::Cascade)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .to_owned(),
        ]
    }

    fn create_indices(&self) -> Vec<IndexCreateStatement> {
        vec![
            Index::create()
                .name("connection_entities___connection_id__fqn___uidx")
                .table(ConnectionEntities::Table)
                .col(ConnectionEntities::ConnectionId)
                .col(ConnectionEntities::Fqn)
                .unique()
                .to_owned(),
        ]
    }
}
