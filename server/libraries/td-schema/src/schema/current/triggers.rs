//
// Copyright 2026 Tabsdata Inc.
//

//! `triggers` — the **domain** dataset-triggers entity. A row in this table
//! registers a function to fire when an upstream table changes. Distinct
//! from DB triggers (CREATE TRIGGER statements), which this module also
//! produces via `create_db_triggers` for its SCD2 close-out.

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

/// `triggers__with_names` view name. Re-used inside this file by
/// `triggers__read` and exposed to consumers.
#[derive(Iden, Clone, Copy)]
#[iden = "triggers__with_names"]
pub struct TriggersWithNames;

#[derive(Iden, Clone, Copy)]
#[iden = "triggers__read"]
pub struct TriggersRead;

#[derive(Iden, Clone, Copy, strum::EnumIter)]
pub enum Triggers {
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
                .table(Triggers::Table)
                .col(id_column(Triggers::Id).not_null().primary_key())
                .col(id_column(Triggers::ProjectId).not_null())
                .col(id_column(Triggers::CollectionId).not_null())
                .col(id_column(Triggers::TriggerId).not_null())
                .col(id_column(Triggers::FunctionId).not_null())
                .col(id_column(Triggers::TriggerByCollectionId).not_null())
                .col(id_column(Triggers::TriggerByFunctionId).not_null())
                .col(id_column(Triggers::TriggerByTableId).not_null())
                .col(ColumnDef::new(Triggers::Status).text().not_null())
                .col(ColumnDef::new(Triggers::ValidFrom).timestamp_with_time_zone().not_null())
                .col(id_column(Triggers::DefinedById).not_null())
                .col(ColumnDef::new(Triggers::System).boolean().not_null())
                .col(ColumnDef::new(Triggers::ValidTo).timestamp_with_time_zone().not_null())
                .foreign_key(
                    ForeignKey::create()
                        .from(Triggers::Table, Triggers::CollectionId)
                        .to(Collections::Table, Collections::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .to_owned(),
        ]
    }

    fn create_indices(&self) -> Vec<IndexCreateStatement> {
        vec![
            // The graph recursion's edge set: every open row of the tenant
            // filtered on status, materialized as a CTE, so the traversal's
            // own joins land on it and not on the table.
            Index::create()
                .name("triggers___project_id__status___current_idx")
                .table(Triggers::Table)
                .col(Triggers::ProjectId)
                .col(Triggers::Status)
                .and_where(open_interval(Triggers::ValidTo))
                .to_owned(),
            // Keyset pagination: `ORDER BY id` under the tenant predicate.
            Index::create()
                .name("triggers___project_id__id___idx")
                .table(Triggers::Table)
                .col(Triggers::ProjectId)
                .col(Triggers::Id)
                .to_owned(),
            // The at-now twin of `trigger_by_table_id`, read per plan build
            // and on collection delete.
            Index::create()
                .name("triggers___trigger_by_table_id___current_idx")
                .table(Triggers::Table)
                .col(Triggers::TriggerByTableId)
                .and_where(open_interval(Triggers::ValidTo))
                .to_owned(),
            // Register and collection delete resolve the triggers fed by a set of tables.
            Index::create().name("triggers___trigger_by_table_id___idx").table(Triggers::Table).col(Triggers::TriggerByTableId).to_owned(),
            Index::create().name("triggers___function_id___idx").table(Triggers::Table).col(Triggers::FunctionId).to_owned(),
            Index::create()
                .name("triggers___trigger_id__valid_from__id___idx")
                .table(Triggers::Table)
                .col(Triggers::TriggerId)
                .col(Triggers::ValidFrom)
                .col(Triggers::Id)
                .to_owned(),
            Index::create()
                .name("triggers___trigger_id___current_uidx")
                .table(Triggers::Table)
                .col(Triggers::TriggerId)
                .unique()
                .and_where(open_interval(Triggers::ValidTo))
                .to_owned(),
            Index::create()
                .name("triggers___function_id___current_idx")
                .table(Triggers::Table)
                .col(Triggers::FunctionId)
                .and_where(open_interval(Triggers::ValidTo))
                .to_owned(),
        ]
    }

    fn create_views(&self) -> Vec<ViewDef> {
        vec![
            triggers_with_names(),
            ViewDef::new(
                TriggersRead,
                Query::select()
                    .expr(Expr::col(Asterisk))
                    .from(TriggersWithNames)
                    .and_where(Expr::col((TriggersWithNames, Triggers::System)).eq(false))
                    .to_owned(),
            ),
        ]
    }

    fn create_db_triggers(&self) -> Vec<Derived> {
        vec![
            Scd2TriggerDef {
                name: format!("{}__scd2_close", Triggers::Table.to_string()),
                source_table: Triggers::Table.into_iden(),
                partition_col: Triggers::TriggerId.into_iden(),
                valid_from_col: Triggers::ValidFrom.into_iden(),
                valid_to_col: Triggers::ValidTo.into_iden(),
                pk_col: Triggers::Id.into_iden(),
            }
            .derived(),
        ]
    }
}

fn triggers_with_names() -> ViewDef {
    let trigger_by_collection = Alias::new("trigger_by_collection");
    ViewDef::new(
        TriggersWithNames,
        Query::select()
            .expr(Expr::col((Triggers::Table, Asterisk)))
            .expr_as(Expr::col((CollectionsResolved, Collections::Name)), Alias::new("collection"))
            .expr_as(Expr::col((CollectionsResolved, Alias::new("deleted"))), Alias::new("collection_deleted"))
            .expr_as(Expr::col((trigger_by_collection.clone(), Collections::Name)), Alias::new("trigger_by_collection"))
            .from(Triggers::Table)
            .join(
                JoinType::LeftJoin,
                CollectionsResolved,
                Expr::col((Triggers::Table, Triggers::CollectionId)).equals((CollectionsResolved, Collections::Id)),
            )
            .join(JoinType::LeftJoin, Users::Table, Expr::col((Triggers::Table, Triggers::DefinedById)).equals((Users::Table, Users::Id)))
            .join_as(
                JoinType::LeftJoin,
                CollectionsResolved,
                trigger_by_collection.clone(),
                Expr::col((Triggers::Table, Triggers::TriggerByCollectionId)).equals((trigger_by_collection, Collections::Id)),
            )
            .to_owned(),
    )
}
