//
// Copyright 2026 Tabsdata Inc.
//

use sea_query::{
    Alias, Asterisk, ColumnDef, Expr, ExprTrait, Iden, Index, IndexCreateStatement, JoinType, Query, Table, TableCreateStatement,
};

use super::collections::{Collections, CollectionsResolved};
use super::users::Users;
use crate::schema::Entity;
use crate::schema::render::name_or_id;
use crate::schema::sql::ViewDef;

#[derive(Iden, Clone, Copy)]
#[iden = "functions_current__with_names"]
pub struct FunctionsCurrentWithNames;

#[derive(Iden, Clone, Copy, strum::EnumIter)]
pub enum FunctionsCurrent {
    Table,
    Id,
    ProjectId,
    CollectionId,
    Name,
    Description,
    Decorator,
    RuntimeValues,
    FunctionId,
    BasePath,
    StageId,
    StageBasePath,
    StorageVersion,
    BundleId,
    Snippet,
    DefinedOn,
    DefinedById,
    Status,
    Connector,
    Connection,
    Enabled,
}

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        // functions_current — holds the latest version per function_id.
        // Maintained by an AFTER INSERT trigger on functions.
        vec![
            Table::create()
                .table(FunctionsCurrent::Table)
                .col(ColumnDef::new(FunctionsCurrent::Id).text().not_null().primary_key())
                .col(ColumnDef::new(FunctionsCurrent::ProjectId).text().not_null())
                .col(ColumnDef::new(FunctionsCurrent::CollectionId).text().not_null())
                .col(ColumnDef::new(FunctionsCurrent::Name).text().not_null())
                .col(ColumnDef::new(FunctionsCurrent::Description).text().not_null())
                .col(ColumnDef::new(FunctionsCurrent::Decorator).text().not_null())
                .col(ColumnDef::new(FunctionsCurrent::RuntimeValues).text().not_null())
                .col(ColumnDef::new(FunctionsCurrent::FunctionId).text().not_null().unique_key())
                .col(ColumnDef::new(FunctionsCurrent::BasePath).text().null())
                .col(ColumnDef::new(FunctionsCurrent::StageId).text().not_null())
                .col(ColumnDef::new(FunctionsCurrent::StageBasePath).text().not_null())
                .col(ColumnDef::new(FunctionsCurrent::StorageVersion).text().not_null())
                .col(ColumnDef::new(FunctionsCurrent::BundleId).text().not_null())
                .col(ColumnDef::new(FunctionsCurrent::Snippet).text().not_null())
                .col(ColumnDef::new(FunctionsCurrent::DefinedOn).timestamp_with_time_zone().not_null())
                .col(ColumnDef::new(FunctionsCurrent::DefinedById).text().not_null())
                .col(ColumnDef::new(FunctionsCurrent::Status).text().not_null())
                .col(ColumnDef::new(FunctionsCurrent::Connector).text().null())
                .col(ColumnDef::new(FunctionsCurrent::Connection).text().null())
                .col(ColumnDef::new(FunctionsCurrent::Enabled).boolean().default(true))
                .to_owned(),
        ]
    }

    fn create_indices(&self) -> Vec<IndexCreateStatement> {
        vec![
            Index::create()
                .name("functions_current___collection_id___idx")
                .table(FunctionsCurrent::Table)
                .col(FunctionsCurrent::CollectionId)
                .to_owned(),
            Index::create()
                .name("functions_current___status___idx")
                .table(FunctionsCurrent::Table)
                .col(FunctionsCurrent::Status)
                .to_owned(),
        ]
    }

    fn create_views(&self) -> Vec<ViewDef> {
        vec![ViewDef::new(
            FunctionsCurrentWithNames,
            Query::select()
                .expr(Expr::col((FunctionsCurrent::Table, Asterisk)))
                .expr_as(Expr::col((CollectionsResolved, Collections::Name)), Alias::new("collection"))
                .expr_as(Expr::col((CollectionsResolved, Alias::new("deleted"))), Alias::new("collection_deleted"))
                .expr_as(Expr::col((CollectionsResolved, Collections::GroupId)), Alias::new("group_id"))
                .expr_as(
                    name_or_id((Users::Table, Users::Name), (FunctionsCurrent::Table, FunctionsCurrent::DefinedById)),
                    Alias::new("defined_by"),
                )
                .from(FunctionsCurrent::Table)
                .join(
                    JoinType::LeftJoin,
                    CollectionsResolved,
                    Expr::col((FunctionsCurrent::Table, FunctionsCurrent::CollectionId)).equals((CollectionsResolved, Collections::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    Users::Table,
                    Expr::col((FunctionsCurrent::Table, FunctionsCurrent::DefinedById)).equals((Users::Table, Users::Id)),
                )
                .to_owned(),
        )]
    }
}
