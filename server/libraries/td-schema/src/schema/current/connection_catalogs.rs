//
// Copyright 2026 Tabsdata Inc.
//

use sea_query::{
    Alias, ColumnDef, Expr, ExprTrait, ForeignKey, ForeignKeyAction, Func, Iden, Index, IndexCreateStatement, JoinType, Query, SimpleExpr,
    Table, TableCreateStatement,
};

use super::collections::Collections;
use super::connections::Connections;
use super::projects::{LastProject, Projects};
use crate::schema::Entity;
use crate::schema::sql::ViewDef;
use crate::schema::sql::id_column;
use crate::schema::sql::scd2::{label_of, last_version_of, open_interval};

#[derive(Iden, Clone, Copy, strum::EnumIter)]
pub enum ConnectionCatalogs {
    Table,
    ConnectionId,
    ProjectId,
    CollectionId,
    Kind,
    CheckStatus,
    CheckError,
    CheckedOn,
    SchemaHash,
    EntityCount,
    Truncated,
    MetadataOn,
    MetadataStatus,
    MetadataError,
    LastProbeId,
    UpdatedOn,
    TextRevision,
}

/// One row per connected collection, whether probed yet or not: the catalog
/// columns fall back to their table defaults until the first probe lands.
#[derive(Iden, Clone, Copy)]
#[iden = "connection_catalogs__with_names"]
pub struct ConnectionCatalogsWithNames;

pub(super) struct Defs;

fn catalog(column: ConnectionCatalogs) -> SimpleExpr {
    Expr::col((ConnectionCatalogs::Table, column))
}

fn catalog_or(column: ConnectionCatalogs, default: SimpleExpr) -> SimpleExpr {
    Func::coalesce([catalog(column), default]).into()
}

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        vec![
            Table::create()
                .table(ConnectionCatalogs::Table)
                .col(id_column(ConnectionCatalogs::ConnectionId).not_null().primary_key())
                .col(id_column(ConnectionCatalogs::ProjectId).not_null())
                .col(id_column(ConnectionCatalogs::CollectionId).not_null())
                .col(ColumnDef::new(ConnectionCatalogs::Kind).text().not_null())
                .col(ColumnDef::new(ConnectionCatalogs::CheckStatus).text().not_null().default("unknown"))
                .col(ColumnDef::new(ConnectionCatalogs::CheckError).text().null())
                .col(ColumnDef::new(ConnectionCatalogs::CheckedOn).timestamp_with_time_zone().null())
                .col(ColumnDef::new(ConnectionCatalogs::SchemaHash).text().null())
                .col(ColumnDef::new(ConnectionCatalogs::EntityCount).integer().not_null().default(0))
                .col(ColumnDef::new(ConnectionCatalogs::Truncated).boolean().not_null().default(false))
                .col(ColumnDef::new(ConnectionCatalogs::MetadataOn).timestamp_with_time_zone().null())
                .col(ColumnDef::new(ConnectionCatalogs::MetadataStatus).text().not_null().default("none"))
                .col(ColumnDef::new(ConnectionCatalogs::MetadataError).text().null())
                .col(id_column(ConnectionCatalogs::LastProbeId).null())
                .col(ColumnDef::new(ConnectionCatalogs::UpdatedOn).timestamp_with_time_zone().not_null().default(Expr::current_timestamp()))
                // Renewed from one sequence on every text change: a version per row and a cursor over all rows.
                .col(ColumnDef::new(ConnectionCatalogs::TextRevision).big_integer().not_null().auto_increment())
                .foreign_key(
                    ForeignKey::create()
                        .from(ConnectionCatalogs::Table, ConnectionCatalogs::ConnectionId)
                        .to(Connections::Table, Connections::Id)
                        .on_delete(ForeignKeyAction::Cascade)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .to_owned(),
        ]
    }

    fn create_indices(&self) -> Vec<IndexCreateStatement> {
        vec![
            Index::create()
                .name("connection_catalogs___text_revision___idx")
                .table(ConnectionCatalogs::Table)
                .col(ConnectionCatalogs::TextRevision)
                .to_owned(),
        ]
    }

    fn create_views(&self) -> Vec<ViewDef> {
        let mut select = Query::select();
        select
            .expr_as(Expr::col((Collections::Table, Collections::ConnectionId)), Alias::new("connection_id"))
            .expr_as(Expr::col((Collections::Table, Collections::ProjectId)), Alias::new("project_id"))
            .expr_as(Expr::col((Collections::Table, Collections::CollectionId)), Alias::new("collection_id"))
            .expr_as(Expr::col((Connections::Table, Connections::Kind)), Alias::new("kind"))
            .expr_as(catalog_or(ConnectionCatalogs::CheckStatus, Expr::val("unknown")), Alias::new("check_status"))
            .expr_as(catalog(ConnectionCatalogs::CheckError), Alias::new("check_error"))
            .expr_as(catalog(ConnectionCatalogs::CheckedOn), Alias::new("checked_on"))
            .expr_as(catalog(ConnectionCatalogs::SchemaHash), Alias::new("schema_hash"))
            .expr_as(catalog_or(ConnectionCatalogs::EntityCount, Expr::val(0)), Alias::new("entity_count"))
            .expr_as(catalog_or(ConnectionCatalogs::Truncated, Expr::val(false)), Alias::new("truncated"))
            .expr_as(catalog(ConnectionCatalogs::MetadataOn), Alias::new("metadata_on"))
            .expr_as(catalog_or(ConnectionCatalogs::MetadataStatus, Expr::val("none")), Alias::new("metadata_status"))
            .expr_as(catalog(ConnectionCatalogs::MetadataError), Alias::new("metadata_error"))
            .expr_as(catalog(ConnectionCatalogs::LastProbeId), Alias::new("last_probe_id"))
            .expr_as(catalog_or(ConnectionCatalogs::TextRevision, Expr::val(0i64)), Alias::new("text_revision"))
            .expr_as(Expr::col((Collections::Table, Collections::Name)), Alias::new("collection"))
            .expr_as(Expr::col((Collections::Table, Collections::Description)), Alias::new("collection_description"))
            .expr_as(Expr::col((Collections::Table, Collections::CollectionType)), Alias::new("collection_type"))
            .expr_as(label_of(LastProject, Projects::Name, (Collections::Table, Collections::ProjectId)), Alias::new("project"))
            .from(Collections::Table)
            .join(
                JoinType::InnerJoin,
                Connections::Table,
                Expr::col((Collections::Table, Collections::ConnectionId)).equals((Connections::Table, Connections::Id)),
            )
            .join_as(
                JoinType::LeftJoin,
                Projects::Table,
                LastProject,
                last_version_of(Projects::Table, LastProject, Projects::ProjectId, (Collections::Table, Collections::ProjectId)),
            )
            .join(
                JoinType::LeftJoin,
                ConnectionCatalogs::Table,
                Expr::col((Collections::Table, Collections::ConnectionId))
                    .equals((ConnectionCatalogs::Table, ConnectionCatalogs::ConnectionId)),
            )
            .and_where(open_interval((Collections::Table, Collections::ValidTo)));
        vec![ViewDef::new(ConnectionCatalogsWithNames, select.to_owned())]
    }
}
