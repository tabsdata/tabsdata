//
// Copyright 2026 Tabs Data Inc.
//

use sea_query::{
    Alias, Asterisk, ColumnDef, Expr, ExprTrait, Iden, Index, IndexCreateStatement, JoinType, Query, Table, TableCreateStatement,
};

use super::collections::Collections;
use super::functions::Functions;
use super::users::Users;
use crate::schema::Entity;
use crate::schema::backend::Backend;
use crate::schema::sql::ViewDef;

#[derive(Iden, Clone, Copy)]
#[iden = "tables_current__with_names"]
pub struct TablesCurrentWithNames;

#[derive(Iden, Clone, Copy, strum::EnumIter)]
#[iden = "tables_current"]
pub enum TablesCurrent {
    Table,
    ProjectId,
    Id,
    CollectionId,
    TableId,
    Name,
    FunctionId,
    FunctionVersionId,
    FunctionParamPos,
    Private,
    Partitioned,
    DefinedOn,
    DefinedById,
    Status,
    TableType,
}

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        // tables_current — holds the latest version per table_id.
        // Maintained by an AFTER INSERT trigger on tables.
        vec![
            Table::create()
                .table(TablesCurrent::Table)
                .col(ColumnDef::new(TablesCurrent::Id).text().not_null().primary_key())
                .col(ColumnDef::new(TablesCurrent::CollectionId).text().not_null())
                .col(ColumnDef::new(TablesCurrent::TableId).text().not_null().unique_key())
                .col(ColumnDef::new(TablesCurrent::Name).text().not_null())
                .col(ColumnDef::new(TablesCurrent::FunctionId).text().null())
                .col(ColumnDef::new(TablesCurrent::FunctionVersionId).text().not_null())
                .col(ColumnDef::new(TablesCurrent::FunctionParamPos).integer().null())
                .col(ColumnDef::new(TablesCurrent::Private).boolean().not_null())
                .col(ColumnDef::new(TablesCurrent::Partitioned).boolean().null())
                .col(ColumnDef::new(TablesCurrent::DefinedOn).timestamp_with_time_zone().not_null())
                .col(ColumnDef::new(TablesCurrent::DefinedById).text().not_null())
                .col(ColumnDef::new(TablesCurrent::Status).text().not_null())
                .col(ColumnDef::new(TablesCurrent::TableType).text().not_null().default("R"))
                .col(ColumnDef::new(TablesCurrent::ProjectId).text().not_null())
                .to_owned(),
        ]
    }

    fn create_indices(&self) -> Vec<IndexCreateStatement> {
        vec![
            Index::create()
                .name("tables_current___collection_id___idx")
                .table(TablesCurrent::Table)
                .col(TablesCurrent::CollectionId)
                .to_owned(),
            Index::create()
                .name("tables_current___function_id___idx")
                .table(TablesCurrent::Table)
                .col(TablesCurrent::FunctionId)
                .to_owned(),
            Index::create().name("tables_current___status___idx").table(TablesCurrent::Table).col(TablesCurrent::Status).to_owned(),
        ]
    }

    fn create_views(&self, b: &Backend) -> Vec<ViewDef> {
        // JOINs `functions` (event log), not `functions_current`, for the function name:
        // a table row's `function_version_id` pins to the exact fvid at registration time,
        // not whatever happens to be the latest version of that function later.
        vec![ViewDef::new(
            TablesCurrentWithNames,
            Query::select()
                .expr(Expr::col((TablesCurrent::Table, Asterisk)))
                .expr_as(Expr::col((TablesCurrent::Table, TablesCurrent::FunctionParamPos)).lt(0), Alias::new("system"))
                .expr_as(
                    b.name_or_id((Users::Table, Users::Name), (TablesCurrent::Table, TablesCurrent::DefinedById)),
                    Alias::new("defined_by"),
                )
                .expr_as(Expr::col((Collections::Table, Collections::Name)), Alias::new("collection"))
                .expr_as(Expr::col((Functions::Table, Functions::Name)), Alias::new("function"))
                .from(TablesCurrent::Table)
                .join(
                    JoinType::LeftJoin,
                    Collections::Table,
                    Expr::col((TablesCurrent::Table, TablesCurrent::CollectionId)).equals((Collections::Table, Collections::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    Functions::Table,
                    Expr::col((TablesCurrent::Table, TablesCurrent::FunctionVersionId)).equals((Functions::Table, Functions::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    Users::Table,
                    Expr::col((TablesCurrent::Table, TablesCurrent::DefinedById)).equals((Users::Table, Users::Id)),
                )
                .to_owned(),
        )]
    }
}
