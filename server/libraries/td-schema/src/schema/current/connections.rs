//
// Copyright 2026 Tabs Data Inc.
//

use sea_query::{ColumnDef, Iden, Index, IndexCreateStatement, InsertStatement, Table, TableCreateStatement};

use crate::schema::Entity;
use crate::schema::backend::Backend;
use crate::schema::sql::ViewDef;

#[derive(Iden, Clone, Copy, strum::EnumIter)]
pub enum Connections {
    Table,
    Id,
    ProjectId,
    Kind,
    CreatedOn,
    CreatedById,
    ModifiedOn,
    ModifiedById,
}

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        vec![
            Table::create()
                .table(Connections::Table)
                // A connection's stable identity: the store backend (bridge,
                // k8s secrets, …) keys the connection CONTENT by this id, so
                // it survives collection rename/recreate. The row also pins
                // the connector kind across upserts.
                .col(ColumnDef::new(Connections::Id).text().not_null())
                // Tenant tag. No base default — fail-closed on every backend: a
                // write that omits it hits NOT NULL. On postgres the RLS phase
                // sets the column default to read `app.project_id` (the request's
                // project); the no-RLS backends (sqlite/mysql) keep
                // no default at all, so writes there must set `project_id`.
                .col(ColumnDef::new(Connections::ProjectId).text().not_null())
                .primary_key(Index::create().col(Connections::ProjectId).col(Connections::Id))
                // Top-level connector kind (e.g. `mysqlSrcConn`) — immutable.
                .col(ColumnDef::new(Connections::Kind).text().not_null())
                .col(ColumnDef::new(Connections::CreatedOn).timestamp_with_time_zone().not_null())
                .col(ColumnDef::new(Connections::CreatedById).text().not_null())
                .col(ColumnDef::new(Connections::ModifiedOn).timestamp_with_time_zone().not_null())
                .col(ColumnDef::new(Connections::ModifiedById).text().not_null())
                .to_owned(),
        ]
    }

    fn create_indices(&self) -> Vec<IndexCreateStatement> {
        vec![]
    }

    fn create_views(&self, _b: &Backend) -> Vec<ViewDef> {
        vec![]
    }

    fn seeds(&self) -> Vec<InsertStatement> {
        vec![]
    }
}
