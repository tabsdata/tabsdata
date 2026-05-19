//
// Copyright 2026 Tabs Data Inc.
//

//! `triggers` — the **domain** dataset-triggers entity. A row in this table
//! registers a function to fire when an upstream table changes. Distinct
//! from DB triggers (CREATE TRIGGER statements), which this module also
//! produces via `create_db_triggers` to maintain its `_current` snapshot.

use sea_query::{
    Alias, Asterisk, ColumnDef, Expr, ExprTrait, ForeignKey, ForeignKeyAction, Iden, Index, IndexCreateStatement, IntoIden, JoinType,
    Query, Table, TableCreateStatement,
};

use super::collections::Collections;
use super::triggers_current::TriggersCurrent;
use super::users::Users;
use crate::schema::Entity;
use crate::schema::backend::Backend;
use crate::schema::sql::{SeedCurrent, TriggerDef, TriggerSpec, ViewDef, cols_of};

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
        vec![
            Table::create()
                .table(Triggers::Table)
                .col(ColumnDef::new(Triggers::Id).text().not_null().primary_key())
                .col(ColumnDef::new(Triggers::CollectionId).text().not_null())
                .col(ColumnDef::new(Triggers::TriggerId).text().not_null())
                .col(ColumnDef::new(Triggers::FunctionId).text().not_null())
                .col(ColumnDef::new(Triggers::TriggerByCollectionId).text().not_null())
                .col(ColumnDef::new(Triggers::TriggerByFunctionId).text().not_null())
                .col(ColumnDef::new(Triggers::TriggerByTableId).text().not_null())
                .col(ColumnDef::new(Triggers::Status).text().not_null())
                .col(ColumnDef::new(Triggers::DefinedOn).timestamp_with_time_zone().not_null())
                .col(ColumnDef::new(Triggers::DefinedById).text().not_null())
                .col(ColumnDef::new(Triggers::System).boolean().not_null())
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
            Index::create().name("triggers___collection_id___idx").table(Triggers::Table).col(Triggers::CollectionId).to_owned(),
            Index::create().name("triggers___function_id___idx").table(Triggers::Table).col(Triggers::FunctionId).to_owned(),
            // Composite index for versioned queries:
            // PARTITION BY trigger_id ORDER BY defined_on DESC, id DESC
            Index::create()
                .name("triggers___trigger_id__defined_on__id___idx")
                .table(Triggers::Table)
                .col(Triggers::TriggerId)
                .col(Triggers::DefinedOn)
                .col(Triggers::Id)
                .to_owned(),
            // For recursive upstream traversal
            Index::create()
                .name("triggers___trigger_by_function_id___idx")
                .table(Triggers::Table)
                .col(Triggers::TriggerByFunctionId)
                .to_owned(),
        ]
    }

    fn create_views(&self, _b: &Backend) -> Vec<ViewDef> {
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

    fn create_db_triggers(&self) -> Vec<TriggerSpec> {
        vec![TriggerSpec::MaintainCurrent(TriggerDef {
            name: format!("{}__maintain_current", Triggers::Table.to_string()),
            source_table: Triggers::Table.into_iden(),
            target_table: TriggersCurrent::Table.into_iden(),
            partition_col: Triggers::TriggerId.into_iden(),
            columns: cols_of::<Triggers>(),
        })]
    }

    fn seed_current(&self) -> Vec<SeedCurrent> {
        vec![SeedCurrent {
            target: TriggersCurrent::Table.into_iden(),
            source: Triggers::Table.into_iden(),
            partition_col: Triggers::TriggerId.into_iden(),
            version_col: Triggers::DefinedOn.into_iden(),
            columns: cols_of::<Triggers>(),
        }]
    }
}

fn triggers_with_names() -> ViewDef {
    let trigger_by_collection = Alias::new("trigger_by_collection");
    ViewDef::new(
        TriggersWithNames,
        Query::select()
            .expr(Expr::col((Triggers::Table, Asterisk)))
            .expr_as(Expr::col((Collections::Table, Collections::Name)), Alias::new("collection"))
            .expr_as(Expr::col((trigger_by_collection.clone(), Collections::Name)), Alias::new("trigger_by_collection"))
            .from(Triggers::Table)
            .join(
                JoinType::LeftJoin,
                Collections::Table,
                Expr::col((Triggers::Table, Triggers::CollectionId)).equals((Collections::Table, Collections::Id)),
            )
            .join(JoinType::LeftJoin, Users::Table, Expr::col((Triggers::Table, Triggers::DefinedById)).equals((Users::Table, Users::Id)))
            .join_as(
                JoinType::LeftJoin,
                Collections::Table,
                trigger_by_collection.clone(),
                Expr::col((Triggers::Table, Triggers::TriggerByCollectionId)).equals((trigger_by_collection, Collections::Id)),
            )
            .to_owned(),
    )
}
