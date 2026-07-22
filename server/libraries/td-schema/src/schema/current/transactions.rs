//
// Copyright 2026 Tabsdata Inc.
//

use sea_query::{
    Alias, Asterisk, ColumnDef, Expr, ExprTrait, ForeignKey, ForeignKeyAction, Iden, Index, IndexCreateStatement, JoinType, Query, Table,
    TableCreateStatement,
};

use super::collections::Collections;
use super::executions::Executions;
use super::groups::Groups;
use super::projects::Projects;
use super::users::Users;
use crate::schema::Entity;
use crate::schema::backend::Backend;
use crate::schema::sql::ViewDef;

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
                .col(ColumnDef::new(Transactions::Id).text().not_null().primary_key())
                .col(ColumnDef::new(Transactions::ProjectId).text().not_null())
                .col(ColumnDef::new(Transactions::CollectionId).text().not_null())
                .col(ColumnDef::new(Transactions::ExecutionId).text().not_null())
                .col(ColumnDef::new(Transactions::TransactionBy).text().not_null())
                .col(ColumnDef::new(Transactions::TransactionKey).text().not_null())
                .col(ColumnDef::new(Transactions::TriggeredOn).timestamp_with_time_zone().not_null())
                .col(ColumnDef::new(Transactions::TriggeredById).text().not_null())
                .col(ColumnDef::new(Transactions::GroupId).text().not_null())
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
            Index::create()
                .name("transactions___collection_id___idx")
                .table(Transactions::Table)
                .col(Transactions::CollectionId)
                .to_owned(),
            Index::create().name("transactions___execution_id___idx").table(Transactions::Table).col(Transactions::ExecutionId).to_owned(),
            Index::create()
                .name("transactions___group_id__collection_id___idx")
                .table(Transactions::Table)
                .col(Transactions::GroupId)
                .col(Transactions::CollectionId)
                .to_owned(),
        ]
    }

    fn create_views(&self, b: &Backend) -> Vec<ViewDef> {
        let groups_alias = Alias::new("groups_for_transactions");
        vec![ViewDef::new(
            TransactionsWithNames,
            Query::select()
                .expr(Expr::col((Transactions::Table, Asterisk)))
                .expr_as(Expr::col((Collections::Table, Collections::Name)), Alias::new("collection"))
                .expr_as(Expr::col((Executions::Table, Executions::Name)), Alias::new("execution"))
                .expr_as(
                    b.name_or_id((Users::Table, Users::Name), (Transactions::Table, Transactions::TriggeredById)),
                    Alias::new("triggered_by"),
                )
                .expr_as(Expr::col((groups_alias.clone(), Groups::Name)), Alias::new("group"))
                .expr_as(Expr::col((Projects::Table, Projects::Name)), Alias::new("project"))
                .from(Transactions::Table)
                .join(
                    JoinType::LeftJoin,
                    Collections::Table,
                    Expr::col((Transactions::Table, Transactions::CollectionId)).equals((Collections::Table, Collections::Id)),
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
                    Projects::Table,
                    Expr::col((Transactions::Table, Transactions::ProjectId)).equals((Projects::Table, Projects::Id)),
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
