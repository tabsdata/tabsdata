//
// Copyright 2026 Tabsdata Inc.
//

use sea_query::{
    Alias, Asterisk, ColumnDef, Expr, ExprTrait, ForeignKey, ForeignKeyAction, Iden, Index, IndexCreateStatement, IntoIden, JoinType,
    Query, Table, TableCreateStatement,
};

use super::collections::{Collections, CollectionsResolved};
use super::dependencies_current::DependenciesCurrent;
use super::users::Users;
use crate::schema::Entity;
use crate::schema::backend::Backend;
use crate::schema::sql::{SeedCurrent, TriggerDef, TriggerSpec, ViewDef, cols_of};

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
    Status,
    DefinedOn,
    DefinedById,
    System,
}

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        vec![
            Table::create()
                .table(Dependencies::Table)
                .col(ColumnDef::new(Dependencies::Id).text().not_null().primary_key())
                .col(ColumnDef::new(Dependencies::ProjectId).text().not_null())
                .col(ColumnDef::new(Dependencies::CollectionId).text().not_null())
                .col(ColumnDef::new(Dependencies::DependencyId).text().not_null())
                .col(ColumnDef::new(Dependencies::FunctionId).text().not_null())
                .col(ColumnDef::new(Dependencies::TableCollectionId).text().not_null())
                .col(ColumnDef::new(Dependencies::TableFunctionId).text().not_null())
                .col(ColumnDef::new(Dependencies::TableId).text().not_null())
                .col(ColumnDef::new(Dependencies::TableVersions).text().not_null())
                .col(ColumnDef::new(Dependencies::DepPos).integer().not_null())
                .col(ColumnDef::new(Dependencies::Status).text().not_null())
                .col(ColumnDef::new(Dependencies::DefinedOn).timestamp_with_time_zone().not_null())
                .col(ColumnDef::new(Dependencies::DefinedById).text().not_null())
                .col(ColumnDef::new(Dependencies::System).boolean().not_null())
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
            // Composite index for versioned queries:
            // PARTITION BY dependency_id ORDER BY defined_on DESC, id DESC
            Index::create()
                .name("dependencies___dependency_id__defined_on__id___idx")
                .table(Dependencies::Table)
                .col(Dependencies::DependencyId)
                .col(Dependencies::DefinedOn)
                .col(Dependencies::Id)
                .to_owned(),
            // For recursive downstream graph traversal (recurse_down column)
            Index::create().name("dependencies___function_id___idx").table(Dependencies::Table).col(Dependencies::FunctionId).to_owned(),
            // For recursive upstream graph traversal (recurse_up column)
            Index::create()
                .name("dependencies___table_function_id___idx")
                .table(Dependencies::Table)
                .col(Dependencies::TableFunctionId)
                .to_owned(),
        ]
    }

    fn create_views(&self, _b: &Backend) -> Vec<ViewDef> {
        // dependencies__with_names (collections joined twice: once for collection_id, once for table_collection_id)
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

    fn create_db_triggers(&self) -> Vec<TriggerSpec> {
        vec![TriggerSpec::MaintainCurrent(TriggerDef {
            name: format!("{}__maintain_current", Dependencies::Table.to_string()),
            source_table: Dependencies::Table.into_iden(),
            target_table: DependenciesCurrent::Table.into_iden(),
            partition_col: Dependencies::DependencyId.into_iden(),
            columns: cols_of::<Dependencies>(),
        })]
    }

    fn seed_current(&self) -> Vec<SeedCurrent> {
        vec![SeedCurrent {
            target: DependenciesCurrent::Table.into_iden(),
            source: Dependencies::Table.into_iden(),
            partition_col: Dependencies::DependencyId.into_iden(),
            version_col: Dependencies::DefinedOn.into_iden(),
            columns: cols_of::<Dependencies>(),
        }]
    }
}
