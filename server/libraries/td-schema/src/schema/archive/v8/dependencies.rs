//
// Copyright 2026 Tabsdata Inc.
//

use sea_query::{
    Alias, Asterisk, ColumnDef, ConditionalStatement, Expr, ExprTrait, ForeignKey, ForeignKeyAction, Iden, Index, IndexCreateStatement,
    IntoIden, JoinType, Query, Table, TableCreateStatement,
};

use super::collections::{Collections, CollectionsResolved};
use super::users::Users;
use crate::schema::Entity;
use crate::schema::source::Derived;
use crate::schema::sql::id_column;
use crate::schema::sql::scd2::open_interval;
use crate::schema::sql::{Scd2TriggerDef, ViewDef};

#[derive(Iden, Clone, Copy)]
#[iden = "dependencies__with_names"]
pub struct DependenciesWithNames;

#[derive(Iden, Clone, Copy, strum::EnumIter)]
pub enum Dependencies {
    Table,
    Id,
    ProjectId,
    CollectionId,
    DependencyId,
    FunctionId,
    TableCollectionId,
    TableFunctionId,
    TableId,
    TableVersions,
    DepPos,
    ValidFrom,
    DefinedById,
    System,
    ValidTo,
}

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        vec![
            Table::create()
                .table(Dependencies::Table)
                .col(id_column(Dependencies::Id).not_null().primary_key())
                .col(id_column(Dependencies::ProjectId).not_null())
                .col(id_column(Dependencies::CollectionId).not_null())
                .col(id_column(Dependencies::DependencyId).not_null())
                .col(id_column(Dependencies::FunctionId).not_null())
                .col(id_column(Dependencies::TableCollectionId).not_null())
                .col(id_column(Dependencies::TableFunctionId).not_null())
                .col(id_column(Dependencies::TableId).not_null())
                .col(ColumnDef::new(Dependencies::TableVersions).text().not_null())
                .col(ColumnDef::new(Dependencies::DepPos).integer().not_null())
                .col(ColumnDef::new(Dependencies::ValidFrom).timestamp_with_time_zone().not_null())
                .col(id_column(Dependencies::DefinedById).not_null())
                .col(ColumnDef::new(Dependencies::System).boolean().not_null())
                .col(ColumnDef::new(Dependencies::ValidTo).timestamp_with_time_zone().not_null())
                .foreign_key(
                    ForeignKey::create()
                        .from(Dependencies::Table, Dependencies::CollectionId)
                        .to(Collections::Table, Collections::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .to_owned(),
        ]
    }

    fn create_indices(&self) -> Vec<IndexCreateStatement> {
        vec![
            // The graph recursion's edge set: every open row of the tenant,
            // materialized as a CTE, so the traversal's own joins land on it
            // and not on the table.
            Index::create()
                .name("dependencies___project_id___current_idx")
                .table(Dependencies::Table)
                .col(Dependencies::ProjectId)
                .and_where(open_interval(Dependencies::ValidTo))
                .to_owned(),
            // Keyset pagination: `ORDER BY id` under the tenant predicate.
            Index::create()
                .name("dependencies___project_id__id___idx")
                .table(Dependencies::Table)
                .col(Dependencies::ProjectId)
                .col(Dependencies::Id)
                .to_owned(),
            // Collection delete and the table-rooted graph build read the
            // open dependencies of a set of tables.
            Index::create()
                .name("dependencies___table_id___current_idx")
                .table(Dependencies::Table)
                .col(Dependencies::TableId)
                .and_where(open_interval(Dependencies::ValidTo))
                .to_owned(),
            // Register and collection delete resolve the dependencies on a set of tables.
            Index::create().name("dependencies___table_id___idx").table(Dependencies::Table).col(Dependencies::TableId).to_owned(),
            Index::create()
                .name("dependencies___dependency_id__valid_from__id___idx")
                .table(Dependencies::Table)
                .col(Dependencies::DependencyId)
                .col(Dependencies::ValidFrom)
                .col(Dependencies::Id)
                .to_owned(),
            // For recursive downstream graph traversal (recurse_down column)
            Index::create().name("dependencies___function_id___idx").table(Dependencies::Table).col(Dependencies::FunctionId).to_owned(),
            Index::create()
                .name("dependencies___dependency_id___current_uidx")
                .table(Dependencies::Table)
                .col(Dependencies::DependencyId)
                .unique()
                .and_where(open_interval(Dependencies::ValidTo))
                .to_owned(),
            Index::create()
                .name("dependencies___function_id___current_idx")
                .table(Dependencies::Table)
                .col(Dependencies::FunctionId)
                .and_where(open_interval(Dependencies::ValidTo))
                .to_owned(),
        ]
    }

    fn create_views(&self) -> Vec<ViewDef> {
        let table_collection = Alias::new("table_collection");
        vec![ViewDef::new(
            DependenciesWithNames,
            Query::select()
                .expr(Expr::col((Dependencies::Table, Asterisk)))
                .expr_as(Expr::col((CollectionsResolved, Collections::Name)), Alias::new("collection"))
                .expr_as(Expr::col((CollectionsResolved, Alias::new("deleted"))), Alias::new("collection_deleted"))
                .expr_as(Expr::col((table_collection.clone(), Collections::Name)), Alias::new("table_collection"))
                .from(Dependencies::Table)
                .join(
                    JoinType::LeftJoin,
                    CollectionsResolved,
                    Expr::col((Dependencies::Table, Dependencies::CollectionId)).equals((CollectionsResolved, Collections::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    Users::Table,
                    Expr::col((Dependencies::Table, Dependencies::DefinedById)).equals((Users::Table, Users::Id)),
                )
                .join_as(
                    JoinType::LeftJoin,
                    CollectionsResolved,
                    table_collection.clone(),
                    Expr::col((Dependencies::Table, Dependencies::TableCollectionId)).equals((table_collection, Collections::Id)),
                )
                .to_owned(),
        )]
    }

    fn create_db_triggers(&self) -> Vec<Derived> {
        vec![
            Scd2TriggerDef {
                name: format!("{}__scd2_close", Dependencies::Table.to_string()),
                source_table: Dependencies::Table.into_iden(),
                partition_col: Dependencies::DependencyId.into_iden(),
                valid_from_col: Dependencies::ValidFrom.into_iden(),
                valid_to_col: Dependencies::ValidTo.into_iden(),
                pk_col: Dependencies::Id.into_iden(),
            }
            .derived(),
        ]
    }
}
