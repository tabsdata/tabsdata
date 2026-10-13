//
// Copyright 2026 Tabsdata Inc.
//

use sea_query::{
    Alias, Asterisk, ColumnDef, Expr, ExprTrait, ForeignKey, ForeignKeyAction, Iden, Index, IndexCreateStatement, IntoIden, JoinType,
    Query, Table, TableCreateStatement,
};

use super::collections::{Collections, CollectionsResolved};
use super::executions::Executions;
use super::groups::Groups;
use super::logical_function_runs::{LogicalFunctionRuns, RUN_LADDER_FALLBACK, RUN_TERMINALS, run_status_count};
use super::physical_function_runs::PhysicalFunctionRuns;
use super::projects::{Projects, ProjectsResolved};
use super::users::Users;
use crate::schema::Entity;
use crate::schema::archive::sql::{Logical, Parent, Physical, RollupArm, RollupDef};
use crate::schema::render::name_or_id;
use crate::schema::sql::ViewDef;
use crate::schema::sql::{id_column, logical_id_column};

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
    Status,
    StartedOn,
    EndedOn,
}

/// The transaction status ladder. Unlike an execution's, each terminal run
/// status rolls up to its own transaction status, so a committed transaction
/// is told apart from a canceled or a yanked one.
const TRANSACTION_LADDER: &[RollupArm] = &[
    RollupArm::All { all: &["S"], status: "S" },
    RollupArm::All { all: &["C"], status: "C" },
    RollupArm::All { all: &["X"], status: "X" },
    RollupArm::All { all: &["Y"], status: "Y" },
    RollupArm::AllWithAny {
        all: &["D", "F", "H"],
        any: &["F", "H"],
        status: "L",
    },
    RollupArm::Any {
        any: &["S", "RR", "RS", "R", "D", "E"],
        status: "R",
    },
];

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        vec![
            Table::create()
                .table(Transactions::Table)
                .col(logical_id_column(Transactions::Id).not_null().primary_key())
                .col(id_column(Transactions::ProjectId).not_null())
                .col(id_column(Transactions::CollectionId).not_null())
                .col(logical_id_column(Transactions::ExecutionId).not_null())
                .col(ColumnDef::new(Transactions::TransactionBy).text().not_null())
                .col(ColumnDef::new(Transactions::TransactionKey).text().not_null())
                .col(ColumnDef::new(Transactions::TriggeredOn).timestamp_with_time_zone().not_null())
                .col(id_column(Transactions::TriggeredById).not_null())
                .col(id_column(Transactions::GroupId).not_null())
                .col(ColumnDef::new(Transactions::Status).text().not_null().default("S"))
                .col(ColumnDef::new(Transactions::StartedOn).timestamp_with_time_zone().null())
                .col(ColumnDef::new(Transactions::EndedOn).timestamp_with_time_zone().null())
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
                    ForeignKey::create()
                        .from(Transactions::Table, Transactions::GroupId)
                        .to(Groups::Table, Groups::Id)
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
                .name("transactions___group_id__project_id___idx")
                .table(Transactions::Table)
                .col(Transactions::GroupId)
                .col(Transactions::ProjectId)
                .to_owned(),
            Index::create()
                .name("transactions___execution_id__triggered_on__id___idx")
                .table(Transactions::Table)
                .col(Transactions::ExecutionId)
                .col(Transactions::TriggeredOn)
                .col(Transactions::Id)
                .to_owned(),
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
            Index::create()
                .name("transactions___triggered_on__id___idx")
                .table(Transactions::Table)
                .col(Transactions::TriggeredOn)
                .col(Transactions::Id)
                .to_owned(),
            Index::create()
                .name("transactions___project_id__status__triggered_on__id___idx")
                .table(Transactions::Table)
                .col(Transactions::ProjectId)
                .col(Transactions::Status)
                .col(Transactions::TriggeredOn)
                .col(Transactions::Id)
                .to_owned(),
            Index::create()
                .name("transactions___project_id__started_on__id___idx")
                .table(Transactions::Table)
                .col(Transactions::ProjectId)
                .col(Transactions::StartedOn)
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
                .expr_as(
                    run_status_count(LogicalFunctionRuns::TransactionId, (Transactions::Table, Transactions::Id)),
                    Alias::new("function_run_status_count"),
                )
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
                    Expr::col((Transactions::Table, Transactions::GroupId)).equals((groups_alias, Groups::Id)),
                )
                .to_owned(),
        )]
    }
}

pub fn rollup() -> RollupDef {
    RollupDef {
        parent: Parent {
            table: Transactions::Table.into_iden(),
            id: Transactions::Id.into_iden(),
            status: Transactions::Status.into_iden(),
            started_on: Transactions::StartedOn.into_iden(),
            ended_on: Transactions::EndedOn.into_iden(),
        },
        logical: Logical {
            table: LogicalFunctionRuns::Table.into_iden(),
            id: LogicalFunctionRuns::Id.into_iden(),
            parent: LogicalFunctionRuns::TransactionId.into_iden(),
            status: LogicalFunctionRuns::Status.into_iden(),
        },
        physical: Physical {
            table: PhysicalFunctionRuns::Table.into_iden(),
            id: PhysicalFunctionRuns::Id.into_iden(),
            parent: PhysicalFunctionRuns::TransactionId.into_iden(),
            started_on: PhysicalFunctionRuns::StartedOn.into_iden(),
            ended_on: PhysicalFunctionRuns::EndedOn.into_iden(),
        },
        ladder: TRANSACTION_LADDER,
        fallback: RUN_LADDER_FALLBACK,
        finished: RUN_TERMINALS,
    }
}
