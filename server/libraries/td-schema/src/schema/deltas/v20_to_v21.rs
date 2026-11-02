//
// Copyright 2026 Tabsdata Inc.
//

//! v20 → v21: the connection catalog (`connection_catalogs`,
//! `connection_entities`, `connection_probes`) with its row-level security.
//! Indexes are derived objects: the ladder drops them by name and rebuilds
//! them from `current/` after the rungs, so the rung creates only the
//! catalog tables and their policies.

use sea_query::{ColumnDef, Expr, ForeignKey, ForeignKeyAction, Iden, Table, TableCreateStatement};

use crate::schema::render::{render_schema, update_db_version};
use crate::schema::sql::id_column;
use crate::schema::version::SchemaDelta;

const TENANT_PREDICATE: &str = "project_id = current_setting('app.project_id', true)";
const ACROSS_PROJECTS_ROLE: &str = "td_across_projects";

/// Frozen at the shape they had at v21.
#[derive(Iden, Clone, Copy)]
enum ConnectionCatalogs {
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

#[derive(Iden, Clone, Copy)]
enum ConnectionEntities {
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

#[derive(Iden, Clone, Copy)]
enum ConnectionProbes {
    Table,
    Id,
    ConnectionId,
    ProjectId,
    ProbeKind,
    Params,
    DedupeKey,
    Status,
    Error,
    Result,
    ResultExpires,
    RequestedById,
    RequestedOn,
    StartedOn,
    DispatchedOn,
    EndedOn,
}

#[derive(Iden, Clone, Copy)]
enum Connections {
    Table,
    Id,
}

const TABLES: [&str; 3] = ["connection_catalogs", "connection_entities", "connection_probes"];

fn empty_object() -> Expr {
    Expr::cust("'{}'::jsonb")
}

fn catalogs() -> TableCreateStatement {
    type C = ConnectionCatalogs;
    Table::create()
        .table(C::Table)
        .col(id_column(C::ConnectionId).not_null().primary_key())
        .col(id_column(C::ProjectId).not_null())
        .col(id_column(C::CollectionId).not_null())
        .col(ColumnDef::new(C::Kind).text().not_null())
        .col(ColumnDef::new(C::CheckStatus).text().not_null().default("unknown"))
        .col(ColumnDef::new(C::CheckError).text().null())
        .col(ColumnDef::new(C::CheckedOn).timestamp_with_time_zone().null())
        .col(ColumnDef::new(C::SchemaHash).text().null())
        .col(ColumnDef::new(C::EntityCount).integer().not_null().default(0))
        .col(ColumnDef::new(C::Truncated).boolean().not_null().default(false))
        .col(ColumnDef::new(C::MetadataOn).timestamp_with_time_zone().null())
        .col(ColumnDef::new(C::MetadataStatus).text().not_null().default("none"))
        .col(ColumnDef::new(C::MetadataError).text().null())
        .col(id_column(C::LastProbeId).null())
        .col(ColumnDef::new(C::UpdatedOn).timestamp_with_time_zone().not_null().default(Expr::current_timestamp()))
        .col(ColumnDef::new(C::TextRevision).big_integer().not_null().auto_increment())
        .foreign_key(
            ForeignKey::create()
                .from(C::Table, C::ConnectionId)
                .to(Connections::Table, Connections::Id)
                .on_delete(ForeignKeyAction::Cascade)
                .on_update(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

fn entities() -> TableCreateStatement {
    type E = ConnectionEntities;
    Table::create()
        .table(E::Table)
        .col(id_column(E::Id).not_null().primary_key())
        .col(id_column(E::ConnectionId).not_null())
        .col(id_column(E::ProjectId).not_null())
        .col(ColumnDef::new(E::Fqn).text().not_null())
        .col(ColumnDef::new(E::EntityKind).text().not_null())
        .col(ColumnDef::new(E::FieldNames).text().not_null().default(""))
        .col(ColumnDef::new(E::FieldCount).integer().not_null().default(0))
        .col(ColumnDef::new(E::Doc).json_binary().not_null())
        .foreign_key(
            ForeignKey::create()
                .from(E::Table, E::ConnectionId)
                .to(ConnectionCatalogs::Table, ConnectionCatalogs::ConnectionId)
                .on_delete(ForeignKeyAction::Cascade)
                .on_update(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

fn probes() -> TableCreateStatement {
    type P = ConnectionProbes;
    Table::create()
        .table(P::Table)
        .col(id_column(P::Id).not_null().primary_key())
        .col(id_column(P::ConnectionId).not_null())
        .col(id_column(P::ProjectId).not_null())
        .col(ColumnDef::new(P::ProbeKind).text().not_null())
        .col(ColumnDef::new(P::Params).json_binary().not_null().default(empty_object()))
        .col(ColumnDef::new(P::DedupeKey).text().not_null())
        .col(ColumnDef::new(P::Status).text().not_null())
        .col(ColumnDef::new(P::Error).json_binary().null())
        .col(ColumnDef::new(P::Result).json_binary().null())
        .col(ColumnDef::new(P::ResultExpires).timestamp_with_time_zone().null())
        .col(id_column(P::RequestedById).not_null())
        .col(ColumnDef::new(P::RequestedOn).timestamp_with_time_zone().not_null().default(Expr::current_timestamp()))
        .col(ColumnDef::new(P::StartedOn).timestamp_with_time_zone().null())
        .col(ColumnDef::new(P::DispatchedOn).timestamp_with_time_zone().null())
        .col(ColumnDef::new(P::EndedOn).timestamp_with_time_zone().null())
        .foreign_key(
            ForeignKey::create()
                .from(P::Table, P::ConnectionId)
                .to(Connections::Table, Connections::Id)
                .on_delete(ForeignKeyAction::Cascade)
                .on_update(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

fn policies(table: &str) -> Vec<String> {
    vec![
        format!(
            "ALTER TABLE {table} ALTER COLUMN project_id SET DEFAULT \
             NULLIF(current_setting('app.project_id', true), '')"
        ),
        format!("ALTER TABLE {table} ENABLE ROW LEVEL SECURITY"),
        format!("ALTER TABLE {table} FORCE ROW LEVEL SECURITY"),
        format!("CREATE POLICY project_isolation ON {table} USING ({TENANT_PREDICATE}) WITH CHECK ({TENANT_PREDICATE})"),
        format!("CREATE POLICY across_projects ON {table} TO {ACROSS_PROJECTS_ROLE} USING (true) WITH CHECK (true)"),
    ]
}

pub struct Migration;

impl SchemaDelta for Migration {
    const FROM_VERSION: usize = 20;
    const TO_VERSION: usize = 21;

    fn retired() -> Vec<String> {
        vec![]
    }

    fn up() -> Vec<String> {
        let mut out = vec![render_schema(catalogs()), render_schema(entities()), render_schema(probes())];
        out.extend(TABLES.iter().flat_map(|table| policies(table)));
        out.push(update_db_version(Self::TO_VERSION));
        out
    }
}

#[cfg(test)]
mod tests {
    use super::Migration;
    use crate::schema::tests::validate_delta;

    #[test]
    fn test_validate_delta_v20_to_v21() {
        validate_delta::<Migration>();
    }
}
