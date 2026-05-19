//
// Copyright 2026 Tabs Data Inc.
//

use sea_query::{Alias, Asterisk, ColumnDef, Expr, ExprTrait, Iden, JoinType, Query, Table, TableCreateStatement};

use super::collections::Collections;
use super::executions::Executions;
use super::functions::Functions;
use crate::schema::Entity;
use crate::schema::backend::Backend;
use crate::schema::sql::ViewDef;

#[derive(Iden, Clone, Copy)]
#[iden = "workers__with_names"]
pub struct WorkersWithNames;

#[derive(Iden, Clone, Copy, strum::EnumIter)]
pub enum Workers {
    Table,
    Id,
    CollectionId,
    ExecutionId,
    TransactionId,
    FunctionVersionId,
    PhysicalFunctionRunId,
    MessageStatus,
    StartedOn,
    EndedOn,
    Status,
    WorkerType,
    WorkerEnabled,
    WorkerConfig,
}

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        vec![
            Table::create()
                .table(Workers::Table)
                .col(ColumnDef::new(Workers::Id).text().not_null().primary_key())
                .col(ColumnDef::new(Workers::CollectionId).text().not_null())
                .col(ColumnDef::new(Workers::ExecutionId).text().null())
                .col(ColumnDef::new(Workers::TransactionId).text().null())
                .col(ColumnDef::new(Workers::FunctionVersionId).text().not_null())
                .col(ColumnDef::new(Workers::PhysicalFunctionRunId).text().null())
                .col(ColumnDef::new(Workers::MessageStatus).text().not_null())
                .col(ColumnDef::new(Workers::StartedOn).timestamp_with_time_zone().null())
                .col(ColumnDef::new(Workers::EndedOn).timestamp_with_time_zone().null())
                .col(ColumnDef::new(Workers::Status).text().not_null())
                .col(ColumnDef::new(Workers::WorkerType).text().not_null())
                .col(ColumnDef::new(Workers::WorkerEnabled).boolean().not_null())
                // `WorkerConfig` is a string-typed Rust value (serialized JSON
                // text), so the column is TEXT. Was BLOB historically, but PG
                // rejects text→BYTEA on INSERT ("column is of type bytea but
                // expression is of type text").
                .col(ColumnDef::new(Workers::WorkerConfig).text().null())
                .to_owned(),
        ]
    }

    fn create_views(&self, _b: &Backend) -> Vec<ViewDef> {
        vec![ViewDef::new(
            WorkersWithNames,
            Query::select()
                .expr(Expr::col((Workers::Table, Asterisk)))
                .expr_as(Expr::col((Collections::Table, Collections::Name)), Alias::new("collection"))
                .expr_as(Expr::col((Executions::Table, Executions::Name)), Alias::new("execution"))
                .expr_as(Expr::col((Functions::Table, Functions::Name)), Alias::new("function"))
                .expr_as(Expr::col((Functions::Table, Functions::Enabled)), Alias::new("enabled"))
                .from(Workers::Table)
                .join(
                    JoinType::LeftJoin,
                    Collections::Table,
                    Expr::col((Workers::Table, Workers::CollectionId)).equals((Collections::Table, Collections::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    Executions::Table,
                    Expr::col((Workers::Table, Workers::ExecutionId)).equals((Executions::Table, Executions::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    Functions::Table,
                    Expr::col((Workers::Table, Workers::FunctionVersionId)).equals((Functions::Table, Functions::Id)),
                )
                .and_where(Expr::col((Workers::Table, Workers::WorkerEnabled)).eq(true))
                .to_owned(),
        )]
    }
}
