//
// Copyright 2026 Tabsdata Inc.
//

use sea_query::{
    Alias, Asterisk, ColumnDef, Expr, ExprTrait, ForeignKey, ForeignKeyAction, Iden, Index, IndexCreateStatement, JoinType, Query, Table,
    TableCreateStatement,
};

use super::collections::{Collections, CollectionsResolved};
use super::executions::Executions;
use super::groups::Groups;
use super::projects::{Projects, ProjectsResolved};
use super::users::Users;
use crate::schema::Entity;
use crate::schema::render::name_or_id;
use crate::schema::sql::ViewDef;
use crate::schema::sql::id_column;

/// `transactions__with_names` view name. Re-used cross-file by
/// `with_status_views` to build `transactions__with_status`.
#[derive(Iden, Clone, Copy)]
#[iden = "transactions__with_names"]
pub struct TransactionsWithNames;

#[derive(Iden, Clone, Copy, strum::EnumIter)]
pub enum Transactions {
    Table,
    Id,
    ProjectId,
    CollectionId,
    ExecutionId,
    TransactionBy,
    TransactionKey,
    TriggeredOn,
    TriggeredById,
    GroupId,
}

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        vec![
            Table::create()
                .table(Transactions::Table)
                .col(id_column(Transactions::Id).not_null().primary_key())
                .col(id_column(Transactions::ProjectId).not_null())
                .col(id_column(Transactions::CollectionId).not_null())
                .col(id_column(Transactions::ExecutionId).not_null())
                .col(ColumnDef::new(Transactions::TransactionBy).text().not_null())
                .col(ColumnDef::new(Transactions::TransactionKey).text().not_null())
                .col(ColumnDef::new(Transactions::TriggeredOn).timestamp_with_time_zone().not_null())
                .col(id_column(Transactions::TriggeredById).not_null())
                .col(id_column(Transactions::GroupId).not_null())
                .foreign_key(
                    ForeignKey::create()
                        .from(Transactions::Table, Transactions::CollectionId)
                        .to(Collections::Table, Collections::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .foreign_key(
                    ForeignKey::create()
                        .from(Transactions::Table, Transactions::ExecutionId)
                        .to(Executions::Table, Executions::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .foreign_key(
                    // Composite: group ids are only unique per project, and this
                    // also forbids cross-project group references at the DB level.
                    ForeignKey::create()
                        .from(Transactions::Table, (Transactions::ProjectId, Transactions::GroupId))
                        .to(Groups::Table, (Groups::ProjectId, Groups::Id))
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .to_owned(),
        ]
    }

    fn create_indices(&self) -> Vec<IndexCreateStatement> {
        vec![
            // The group-delete RESTRICT FK probe (see `collections`).
            Index::create()
                .name("transactions___project_id__group_id___idx")
                .table(Transactions::Table)
                .col(Transactions::ProjectId)
                .col(Transactions::GroupId)
                .to_owned(),
            Index::create().name("transactions___execution_id___idx").table(Transactions::Table).col(Transactions::ExecutionId).to_owned(),
            // Keyset pagination: `ORDER BY id` under the tenant predicate.
            Index::create()
                .name("transactions___project_id__id___idx")
                .table(Transactions::Table)
                .col(Transactions::ProjectId)
                .col(Transactions::Id)
                .to_owned(),
            // The transaction list's default time ordering.
            Index::create()
                .name("transactions___project_id__triggered_on__id___idx")
                .table(Transactions::Table)
                .col(Transactions::ProjectId)
                .col(Transactions::TriggeredOn)
                .col(Transactions::Id)
                .to_owned(),
        ]
    }

    fn create_views(&self) -> Vec<ViewDef> {
        let groups_alias = Alias::new("groups_for_transactions");
        vec![ViewDef::new(
            TransactionsWithNames,
            Query::select()
                .expr(Expr::col((Transactions::Table, Asterisk)))
                .expr_as(Expr::col((CollectionsResolved, Collections::Name)), Alias::new("collection"))
                .expr_as(Expr::col((CollectionsResolved, Alias::new("deleted"))), Alias::new("collection_deleted"))
                .expr_as(Expr::col((Executions::Table, Executions::Name)), Alias::new("execution"))
                .expr_as(
                    name_or_id((Users::Table, Users::Name), (Transactions::Table, Transactions::TriggeredById)),
                    Alias::new("triggered_by"),
                )
                .expr_as(Expr::col((groups_alias.clone(), Groups::Name)), Alias::new("group"))
                .expr_as(Expr::col((ProjectsResolved, Projects::Name)), Alias::new("project"))
                .from(Transactions::Table)
                .join(
                    JoinType::LeftJoin,
                    CollectionsResolved,
                    Expr::col((Transactions::Table, Transactions::CollectionId)).equals((CollectionsResolved, Collections::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    Executions::Table,
                    Expr::col((Transactions::Table, Transactions::ExecutionId)).equals((Executions::Table, Executions::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    Users::Table,
                    Expr::col((Transactions::Table, Transactions::TriggeredById)).equals((Users::Table, Users::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    ProjectsResolved,
                    Expr::col((Transactions::Table, Transactions::ProjectId)).equals((ProjectsResolved, Projects::Id)),
                )
                .join_as(
                    JoinType::LeftJoin,
                    Groups::Table,
                    groups_alias.clone(),
                    // Group ids are only unique per project; matching project_id
                    // too keeps the join exact under RLS-bypass (global) sessions.
                    Expr::col((Transactions::Table, Transactions::GroupId))
                        .equals((groups_alias.clone(), Groups::Id))
                        .and(Expr::col((Transactions::Table, Transactions::ProjectId)).equals((groups_alias, Groups::ProjectId))),
                )
                .to_owned(),
        )]
    }
}
