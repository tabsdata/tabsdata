//
// Copyright 2026 Tabsdata Inc.
//

use sea_query::{
    ColumnDef, ConditionalStatement, Expr, ExprTrait, ForeignKey, ForeignKeyAction, Iden, Index, IndexCreateStatement, Table,
    TableCreateStatement,
};

use super::connections::Connections;
use crate::schema::Entity;
use crate::schema::sql::id_column;

#[derive(Iden, Clone, Copy, strum::EnumIter)]
pub enum ConnectionProbes {
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

/// A submission matching an active probe joins it instead of starting
/// another; the dispatcher reads only these.
const ACTIVE_STATUSES: [&str; 2] = ["queued", "running"];

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        vec![
            Table::create()
                .table(ConnectionProbes::Table)
                .col(id_column(ConnectionProbes::Id).not_null().primary_key())
                .col(id_column(ConnectionProbes::ConnectionId).not_null())
                .col(id_column(ConnectionProbes::ProjectId).not_null())
                .col(ColumnDef::new(ConnectionProbes::ProbeKind).text().not_null())
                .col(ColumnDef::new(ConnectionProbes::Params).json_binary().not_null().default(Expr::cust("'{}'::jsonb")))
                .col(ColumnDef::new(ConnectionProbes::DedupeKey).text().not_null())
                .col(ColumnDef::new(ConnectionProbes::Status).text().not_null())
                .col(ColumnDef::new(ConnectionProbes::Error).json_binary().null())
                // Sample rows live here only until `result_expires`.
                .col(ColumnDef::new(ConnectionProbes::Result).json_binary().null())
                .col(ColumnDef::new(ConnectionProbes::ResultExpires).timestamp_with_time_zone().null())
                .col(id_column(ConnectionProbes::RequestedById).not_null())
                .col(ColumnDef::new(ConnectionProbes::RequestedOn).timestamp_with_time_zone().not_null().default(Expr::current_timestamp()))
                .col(ColumnDef::new(ConnectionProbes::StartedOn).timestamp_with_time_zone().null())
                .col(ColumnDef::new(ConnectionProbes::DispatchedOn).timestamp_with_time_zone().null())
                .col(ColumnDef::new(ConnectionProbes::EndedOn).timestamp_with_time_zone().null())
                .foreign_key(
                    ForeignKey::create()
                        .from(ConnectionProbes::Table, ConnectionProbes::ConnectionId)
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
                .name("connection_probes___connection_id___idx")
                .table(ConnectionProbes::Table)
                .col(ConnectionProbes::ConnectionId)
                .to_owned(),
            Index::create()
                .name("connection_probes___dedupe_key___active_uidx")
                .table(ConnectionProbes::Table)
                .col(ConnectionProbes::DedupeKey)
                .unique()
                .and_where(Expr::col(ConnectionProbes::Status).is_in(ACTIVE_STATUSES))
                .to_owned(),
            Index::create()
                .name("connection_probes___status__requested_on___active_idx")
                .table(ConnectionProbes::Table)
                .col(ConnectionProbes::Status)
                .col(ConnectionProbes::RequestedOn)
                .and_where(Expr::col(ConnectionProbes::Status).is_in(ACTIVE_STATUSES))
                .to_owned(),
            Index::create()
                .name("connection_probes___result_expires___idx")
                .table(ConnectionProbes::Table)
                .col(ConnectionProbes::ResultExpires)
                .and_where(Expr::col(ConnectionProbes::ResultExpires).is_not_null())
                .to_owned(),
            Index::create()
                .name("connection_probes___ended_on___idx")
                .table(ConnectionProbes::Table)
                .col(ConnectionProbes::EndedOn)
                .and_where(Expr::col(ConnectionProbes::EndedOn).is_not_null())
                .to_owned(),
        ]
    }
}
