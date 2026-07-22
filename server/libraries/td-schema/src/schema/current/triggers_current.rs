//
// Copyright 2026 Tabsdata Inc.
//

use sea_query::{
    Alias, Asterisk, ColumnDef, Expr, ExprTrait, Iden, Index, IndexCreateStatement, JoinType, Query, Table, TableCreateStatement,
};

use super::collections::{Collections, CollectionsResolved};
use super::users::Users;
use crate::schema::Entity;
use crate::schema::backend::Backend;
use crate::schema::sql::ViewDef;

#[derive(Iden, Clone, Copy)]
#[iden = "triggers_current__with_names"]
pub struct TriggersCurrentWithNames;

#[derive(Iden, Clone, Copy, strum::EnumIter)]
pub enum TriggersCurrent {
    Table,
    Id,
    ProjectId,
    CollectionId,
    TriggerId,
    FunctionId,
    TriggerByCollectionId,
    TriggerByFunctionId,
    TriggerByTableId,
    Status,
    DefinedOn,
    DefinedById,
    System,
}

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        // triggers_current — holds the latest version per trigger_id.
        // Maintained by an AFTER INSERT trigger on triggers.
        vec![
            Table::create()
                .table(TriggersCurrent::Table)
                .col(ColumnDef::new(TriggersCurrent::Id).text().not_null().primary_key())
                .col(ColumnDef::new(TriggersCurrent::ProjectId).text().not_null())
                .col(ColumnDef::new(TriggersCurrent::CollectionId).text().not_null())
                .col(ColumnDef::new(TriggersCurrent::TriggerId).text().not_null().unique_key())
                .col(ColumnDef::new(TriggersCurrent::FunctionId).text().not_null())
                .col(ColumnDef::new(TriggersCurrent::TriggerByCollectionId).text().not_null())
                .col(ColumnDef::new(TriggersCurrent::TriggerByFunctionId).text().not_null())
                .col(ColumnDef::new(TriggersCurrent::TriggerByTableId).text().not_null())
                .col(ColumnDef::new(TriggersCurrent::Status).text().not_null())
                .col(ColumnDef::new(TriggersCurrent::DefinedOn).timestamp_with_time_zone().not_null())
                .col(ColumnDef::new(TriggersCurrent::DefinedById).text().not_null())
                .col(ColumnDef::new(TriggersCurrent::System).boolean().not_null())
                .to_owned(),
        ]
    }

    fn create_indices(&self) -> Vec<IndexCreateStatement> {
        vec![
            Index::create()
                .name("triggers_current___collection_id___idx")
                .table(TriggersCurrent::Table)
                .col(TriggersCurrent::CollectionId)
                .to_owned(),
            Index::create()
                .name("triggers_current___function_id___idx")
                .table(TriggersCurrent::Table)
                .col(TriggersCurrent::FunctionId)
                .to_owned(),
            Index::create()
                .name("triggers_current___trigger_by_function_id___idx")
                .table(TriggersCurrent::Table)
                .col(TriggersCurrent::TriggerByFunctionId)
                .to_owned(),
        ]
    }

    fn create_views(&self, _b: &Backend) -> Vec<ViewDef> {
        let trigger_by_collection = Alias::new("trigger_by_collection");
        vec![ViewDef::new(
            TriggersCurrentWithNames,
            Query::select()
                .expr(Expr::col((TriggersCurrent::Table, Asterisk)))
                .expr_as(Expr::col((CollectionsResolved, Collections::Name)), Alias::new("collection"))
                .expr_as(Expr::col((CollectionsResolved, Alias::new("deleted"))), Alias::new("collection_deleted"))
                .expr_as(Expr::col((trigger_by_collection.clone(), Collections::Name)), Alias::new("trigger_by_collection"))
                .from(TriggersCurrent::Table)
                .join(
                    JoinType::LeftJoin,
                    CollectionsResolved,
                    Expr::col((TriggersCurrent::Table, TriggersCurrent::CollectionId)).equals((CollectionsResolved, Collections::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    Users::Table,
                    Expr::col((TriggersCurrent::Table, TriggersCurrent::DefinedById)).equals((Users::Table, Users::Id)),
                )
                .join_as(
                    JoinType::LeftJoin,
                    CollectionsResolved,
                    trigger_by_collection.clone(),
                    Expr::col((TriggersCurrent::Table, TriggersCurrent::TriggerByCollectionId))
                        .equals((trigger_by_collection, Collections::Id)),
                )
                .to_owned(),
        )]
    }
}
