//
// Copyright 2026 Tabs Data Inc.
//

use sea_query::{
    Alias, Asterisk, ColumnDef, Expr, ExprTrait, Iden, Index, IndexCreateStatement, JoinType, Query, Table, TableCreateStatement,
};

use super::collections::Collections;
use super::users::Users;
use crate::schema::Entity;
use crate::schema::backend::Backend;
use crate::schema::sql::ViewDef;

#[derive(Iden, Clone, Copy)]
#[iden = "dependencies_current__with_names"]
pub struct DependenciesCurrentWithNames;

#[derive(Iden, Clone, Copy, strum::EnumIter)]
#[iden = "dependencies_current"]
pub enum DependenciesCurrent {
    Table,
    ProjectId,
    Id,
    CollectionId,
    DependencyId,
    FunctionId,
    TableCollectionId,
    TableFunctionId,
    TableId,
    TableVersions,
    DepPos,
    Status,
    DefinedOn,
    DefinedById,
    System,
}

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        // dependencies_current — holds the latest version per dependency_id.
        // Maintained by an AFTER INSERT trigger on dependencies.
        vec![
            Table::create()
                .table(DependenciesCurrent::Table)
                .col(ColumnDef::new(DependenciesCurrent::Id).text().not_null().primary_key())
                .col(ColumnDef::new(DependenciesCurrent::CollectionId).text().not_null())
                .col(ColumnDef::new(DependenciesCurrent::DependencyId).text().not_null().unique_key())
                .col(ColumnDef::new(DependenciesCurrent::FunctionId).text().not_null())
                .col(ColumnDef::new(DependenciesCurrent::TableCollectionId).text().not_null())
                .col(ColumnDef::new(DependenciesCurrent::TableFunctionId).text().not_null())
                .col(ColumnDef::new(DependenciesCurrent::TableId).text().not_null())
                .col(ColumnDef::new(DependenciesCurrent::TableVersions).text().not_null())
                .col(ColumnDef::new(DependenciesCurrent::DepPos).integer().not_null())
                .col(ColumnDef::new(DependenciesCurrent::Status).text().not_null())
                .col(ColumnDef::new(DependenciesCurrent::DefinedOn).timestamp_with_time_zone().not_null())
                .col(ColumnDef::new(DependenciesCurrent::DefinedById).text().not_null())
                .col(ColumnDef::new(DependenciesCurrent::System).boolean().not_null())
                .col(ColumnDef::new(DependenciesCurrent::ProjectId).text().not_null())
                .to_owned(),
        ]
    }

    fn create_indices(&self) -> Vec<IndexCreateStatement> {
        vec![
            Index::create()
                .name("dependencies_current___function_id___idx")
                .table(DependenciesCurrent::Table)
                .col(DependenciesCurrent::FunctionId)
                .to_owned(),
            Index::create()
                .name("dependencies_current___table_function_id___idx")
                .table(DependenciesCurrent::Table)
                .col(DependenciesCurrent::TableFunctionId)
                .to_owned(),
        ]
    }

    fn create_views(&self, _b: &Backend) -> Vec<ViewDef> {
        let table_collection = Alias::new("table_collection");
        vec![ViewDef::new(
            DependenciesCurrentWithNames,
            Query::select()
                .expr(Expr::col((DependenciesCurrent::Table, Asterisk)))
                .expr_as(Expr::col((Collections::Table, Collections::Name)), Alias::new("collection"))
                .expr_as(Expr::col((table_collection.clone(), Collections::Name)), Alias::new("table_collection"))
                .from(DependenciesCurrent::Table)
                .join(
                    JoinType::LeftJoin,
                    Collections::Table,
                    Expr::col((DependenciesCurrent::Table, DependenciesCurrent::CollectionId))
                        .equals((Collections::Table, Collections::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    Users::Table,
                    Expr::col((DependenciesCurrent::Table, DependenciesCurrent::DefinedById)).equals((Users::Table, Users::Id)),
                )
                .join_as(
                    JoinType::LeftJoin,
                    Collections::Table,
                    table_collection.clone(),
                    Expr::col((DependenciesCurrent::Table, DependenciesCurrent::TableCollectionId))
                        .equals((table_collection, Collections::Id)),
                )
                .to_owned(),
        )]
    }
}
