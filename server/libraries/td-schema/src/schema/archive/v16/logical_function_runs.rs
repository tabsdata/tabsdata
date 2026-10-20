//
// Copyright 2026 Tabsdata Inc.
//

//! Logical function-run identity rows + canonical (sticky-Committed) status.
//!
//! The `logical_function_runs` table is the canonical per-run identity
//! created at plan-build; rows are insert-once. The status column is the
//! flow-control source of truth (sticky-Committed) — distinct from the
//! per-attempt audit `status` on `physical_function_runs`.
//!

use sea_query::{
    Alias, Asterisk, ColumnDef, Expr, ExprTrait, ForeignKey, ForeignKeyAction, Func, Iden, Index, IndexCreateStatement, IntoIden, JoinType,
    Query, SimpleExpr, SubQueryStatement, Table, TableCreateStatement,
};

use super::collections::{Collections, CollectionsResolved};
use super::executions::Executions;
use super::functions::Functions;
use super::groups::Groups;
use super::physical_function_runs::PhysicalFunctionRuns;
use super::projects::{Projects, ProjectsResolved};
use super::transactions::Transactions;
use super::users::Users;
use crate::schema::Entity;
use crate::schema::render::{json_object_agg, name_or_id};
use crate::schema::source::Derived;
use crate::schema::sql::scd2::open_interval;
use crate::schema::sql::{Transitions, TransitionsTriggerDef, ViewDef};
use crate::schema::sql::{id_column, logical_id_column};

#[derive(Iden, Clone, Copy)]
#[iden = "function_runs__with_names"]
pub struct FunctionRunsWithNames;

/// The classes the transaction ladder ranks a run's status into; together
/// they hold every status exactly once.
///
/// Can still move on its own: scheduled, requested, rescheduled, running,
/// or in error (retried).
pub(super) const RUN_ACTIVE: &[&str] = &["S", "RR", "RS", "R", "E"];

/// Waits for an operator: failed, or on hold behind a failure.
pub(super) const RUN_BLOCKED: &[&str] = &["F", "H"];

/// Done, awaiting its transaction's commit or discard.
pub(super) const RUN_AWAITING: &[&str] = &["D"];

/// A run in one of these never leaves it; a parent all of whose runs are in
/// the set has ended.
pub(super) const RUN_TERMINALS: &[&str] = &["C", "X", "Y"];

/// Every status change a run may make. X and Y have no way out, and C only
/// to Y.
const RUN_TRANSITIONS: &[(&str, &str)] = &[
    ("S", "RR"),
    ("S", "H"),
    ("S", "RS"),
    ("S", "X"),
    ("RR", "S"),
    ("RR", "R"),
    ("RR", "E"),
    ("RR", "F"),
    ("RR", "H"),
    ("RR", "RS"),
    ("RR", "X"),
    ("RS", "RR"),
    ("RS", "H"),
    ("RS", "X"),
    ("R", "D"),
    ("R", "E"),
    ("R", "F"),
    ("R", "H"),
    ("R", "RS"),
    ("R", "X"),
    ("D", "C"),
    ("D", "H"),
    ("D", "RS"),
    ("D", "X"),
    ("C", "Y"),
    ("E", "H"),
    ("E", "RS"),
    ("E", "X"),
    ("F", "D"),
    ("F", "H"),
    ("F", "RS"),
    ("F", "X"),
    ("H", "RS"),
    ("H", "X"),
];

/// Alias for the per-status count derived table inside the status-count
/// scalar subquery; the dialect JSON aggregate reads its `status`/`count`
/// columns back out through this name.
#[derive(Iden, Clone, Copy)]
struct InnerFr;

#[derive(Iden, Clone, Copy, strum::EnumIter)]
pub enum LogicalFunctionRuns {
    Table,
    Id,
    ProjectId,
    CollectionId,
    ExecutionId,
    TransactionId,
    FunctionId,
    FunctionVersionId,
    Trigger,
    TriggeredOn,
    TriggeredById,
    Status,
    // Group snapshot, copied from collections.group_id at LFR mint.
    // Immutable thereafter — collection-group reassignment does not
    // mutate already-minted runs.
    GroupId,
    Recoveries,
}

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        vec![
            Table::create()
                .table(LogicalFunctionRuns::Table)
                .col(logical_id_column(LogicalFunctionRuns::Id).not_null().primary_key())
                .col(id_column(LogicalFunctionRuns::ProjectId).not_null())
                .col(id_column(LogicalFunctionRuns::CollectionId).not_null())
                .col(logical_id_column(LogicalFunctionRuns::ExecutionId).not_null())
                .col(logical_id_column(LogicalFunctionRuns::TransactionId).not_null())
                .col(id_column(LogicalFunctionRuns::FunctionId).not_null())
                // Plan-time fvid the LFR was minted against; topology
                // reference for primitives. Set once at plan-build.
                .col(logical_id_column(LogicalFunctionRuns::FunctionVersionId).not_null().default(197001010000000001i64))
                .col(ColumnDef::new(LogicalFunctionRuns::Trigger).text().not_null())
                .col(ColumnDef::new(LogicalFunctionRuns::TriggeredOn).timestamp_with_time_zone().not_null())
                .col(id_column(LogicalFunctionRuns::TriggeredById).not_null())
                .col(ColumnDef::new(LogicalFunctionRuns::Status).text().not_null())
                .col(id_column(LogicalFunctionRuns::GroupId).not_null())
                .col(ColumnDef::new(LogicalFunctionRuns::Recoveries).integer().not_null().default(0))
                .foreign_key(
                    ForeignKey::create()
                        .from(LogicalFunctionRuns::Table, LogicalFunctionRuns::CollectionId)
                        .to(Collections::Table, Collections::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .foreign_key(
                    ForeignKey::create()
                        .from(LogicalFunctionRuns::Table, LogicalFunctionRuns::GroupId)
                        .to(Groups::Table, Groups::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .foreign_key(
                    ForeignKey::create()
                        .from(LogicalFunctionRuns::Table, LogicalFunctionRuns::ExecutionId)
                        .to(Executions::Table, Executions::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .foreign_key(
                    ForeignKey::create()
                        .from(LogicalFunctionRuns::Table, LogicalFunctionRuns::TransactionId)
                        .to(Transactions::Table, Transactions::Id)
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
                .name("logical_function_runs___group_id__project_id___idx")
                .table(LogicalFunctionRuns::Table)
                .col(LogicalFunctionRuns::GroupId)
                .col(LogicalFunctionRuns::ProjectId)
                .to_owned(),
            // The scheduler's execute tick: `status IN (..) ORDER BY
            // triggered_on` — status alone cannot serve the ordering. Ticks are
            // per project, but the recovery snapshot reads the same status
            // across projects, so `project_id` sits after the shared prefix.
            Index::create()
                .name("logical_function_runs___status__project_id__triggered_on___idx")
                .table(LogicalFunctionRuns::Table)
                .col(LogicalFunctionRuns::Status)
                .col(LogicalFunctionRuns::ProjectId)
                .col(LogicalFunctionRuns::TriggeredOn)
                .to_owned(),
            // Keyset pagination: `ORDER BY id` under the tenant predicate.
            Index::create()
                .name("logical_function_runs___project_id__id___idx")
                .table(LogicalFunctionRuns::Table)
                .col(LogicalFunctionRuns::ProjectId)
                .col(LogicalFunctionRuns::Id)
                .to_owned(),
            // Executions of one function version; the column is otherwise
            // unindexed and the lookup drives an OR of two EXISTS probes.
            Index::create()
                .name("logical_function_runs___function_version_id___idx")
                .table(LogicalFunctionRuns::Table)
                .col(LogicalFunctionRuns::FunctionVersionId)
                .to_owned(),
            // FR → LFR derivation (status cascade, callback, redirect, replay).
            // Unique by construction: at most one LFR per `(execution_id,
            // function_id)`. The `.unique()` lifts that invariant from
            // service-layer convention to DB structure.
            Index::create()
                .name("logical_function_runs___execution_id__function_id___idx")
                .table(LogicalFunctionRuns::Table)
                .col(LogicalFunctionRuns::ExecutionId)
                .col(LogicalFunctionRuns::FunctionId)
                .unique()
                .to_owned(),
            // `Versions::New` cutoff lookup: filter by `function_id`, order
            // by `(triggered_on DESC, id DESC)`, LIMIT 1.
            Index::create()
                .name("logical_function_runs___function_id__triggered_on__id___idx")
                .table(LogicalFunctionRuns::Table)
                .col(LogicalFunctionRuns::FunctionId)
                .col(LogicalFunctionRuns::TriggeredOn)
                .col(LogicalFunctionRuns::Id)
                .to_owned(),
            // Canonical (LFR) status indices for the logical-chain cascade.
            Index::create()
                .name("logical_function_runs___execution_id__status___idx")
                .table(LogicalFunctionRuns::Table)
                .col(LogicalFunctionRuns::ExecutionId)
                .col(LogicalFunctionRuns::Status)
                .to_owned(),
            Index::create()
                .name("logical_function_runs___transaction_id__status___idx")
                .table(LogicalFunctionRuns::Table)
                .col(LogicalFunctionRuns::TransactionId)
                .col(LogicalFunctionRuns::Status)
                .to_owned(),
            time_ordered("logical_function_runs___triggered_on__id___idx", None),
            time_ordered("logical_function_runs___project_id__triggered_on__id___idx", Some(LogicalFunctionRuns::ProjectId)),
            time_ordered("logical_function_runs___collection_id__triggered_on__id___idx", Some(LogicalFunctionRuns::CollectionId)),
            time_ordered("logical_function_runs___execution_id__triggered_on__id___idx", Some(LogicalFunctionRuns::ExecutionId)),
            time_ordered("logical_function_runs___transaction_id__triggered_on__id___idx", Some(LogicalFunctionRuns::TransactionId)),
        ]
    }

    fn create_views(&self) -> Vec<ViewDef> {
        // function_runs__with_names — names-joined view of the LFR identity
        // row. Backs the listing endpoint that powers the execution-plan
        // graph — `status` here is the canonical (sticky-Committed)
        // flow-control status, not the per-attempt audit on the PFR.
        let groups_alias = Alias::new("groups_for_lfr");
        let function_runs_with_names = ViewDef::new(
            FunctionRunsWithNames,
            Query::select()
                .expr(Expr::col((LogicalFunctionRuns::Table, Asterisk)))
                .expr_as(Expr::col((Functions::Table, Functions::Name)), Alias::new("name"))
                .expr_as(Expr::col((CollectionsResolved, Collections::Name)), Alias::new("collection"))
                .expr_as(Expr::col((CollectionsResolved, Alias::new("deleted"))), Alias::new("collection_deleted"))
                .expr_as(Expr::col((Executions::Table, Executions::Name)), Alias::new("execution"))
                .expr_as(
                    name_or_id((Users::Table, Users::Name), (LogicalFunctionRuns::Table, LogicalFunctionRuns::TriggeredById)),
                    Alias::new("triggered_by"),
                )
                .expr_as(Expr::col((groups_alias.clone(), Groups::Name)), Alias::new("group"))
                .expr_as(Expr::col((ProjectsResolved, Projects::Name)), Alias::new("project"))
                .expr_as(Expr::col((PhysicalFunctionRuns::Table, PhysicalFunctionRuns::Id)), Alias::new("physical_function_run_id"))
                .expr_as(Expr::col((PhysicalFunctionRuns::Table, PhysicalFunctionRuns::StartedOn)), Alias::new("started_on"))
                .expr_as(Expr::col((PhysicalFunctionRuns::Table, PhysicalFunctionRuns::EndedOn)), Alias::new("ended_on"))
                .expr_as(Expr::col((PhysicalFunctionRuns::Table, PhysicalFunctionRuns::Status)), Alias::new("physical_status"))
                .expr_as(
                    Expr::col((PhysicalFunctionRuns::Table, PhysicalFunctionRuns::FunctionVersionPatchId)),
                    Alias::new("function_version_patch_id"),
                )
                .from(LogicalFunctionRuns::Table)
                .join(
                    JoinType::LeftJoin,
                    CollectionsResolved,
                    Expr::col((LogicalFunctionRuns::Table, LogicalFunctionRuns::CollectionId))
                        .equals((CollectionsResolved, Collections::Id)),
                )
                .join_as(
                    JoinType::LeftJoin,
                    Groups::Table,
                    groups_alias.clone(),
                    Expr::col((LogicalFunctionRuns::Table, LogicalFunctionRuns::GroupId)).equals((groups_alias, Groups::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    ProjectsResolved,
                    Expr::col((LogicalFunctionRuns::Table, LogicalFunctionRuns::ProjectId)).equals((ProjectsResolved, Projects::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    Functions::Table,
                    Expr::col((LogicalFunctionRuns::Table, LogicalFunctionRuns::FunctionVersionId))
                        .equals((Functions::Table, Functions::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    Executions::Table,
                    Expr::col((LogicalFunctionRuns::Table, LogicalFunctionRuns::ExecutionId)).equals((Executions::Table, Executions::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    Users::Table,
                    Expr::col((LogicalFunctionRuns::Table, LogicalFunctionRuns::TriggeredById)).equals((Users::Table, Users::Id)),
                )
                // The attempt currently bound to this run: its open SCD2
                // interval. Unbound runs (no attempt yet, or all of them
                // voided) read as NULL on every PFR-derived column.
                .join(
                    JoinType::LeftJoin,
                    PhysicalFunctionRuns::Table,
                    Expr::col((PhysicalFunctionRuns::Table, PhysicalFunctionRuns::LogicalFunctionRunId))
                        .equals((LogicalFunctionRuns::Table, LogicalFunctionRuns::Id))
                        .and(open_interval((PhysicalFunctionRuns::Table, PhysicalFunctionRuns::ValidTo))),
                )
                .to_owned(),
        );

        vec![function_runs_with_names]
    }

    fn create_db_triggers(&self) -> Vec<Derived> {
        vec![
            TransitionsTriggerDef {
                name: format!("{}__transitions", LogicalFunctionRuns::Table.to_string()),
                source_table: LogicalFunctionRuns::Table.into_iden(),
                status_col: LogicalFunctionRuns::Status.into_iden(),
                transitions: Transitions::Any(RUN_TRANSITIONS),
            }
            .derived(),
        ]
    }
}

/// The per-status run counts of one parent, as a scalar subquery yielding a
/// JSON object. Not filterable, so the listing views evaluate it for the rows
/// of the page only — the counts stay derived where materializing them would
/// buy nothing.
///
pub(super) fn run_status_count<T: IntoIden, C: IntoIden>(run_parent_col: LogicalFunctionRuns, parent: (T, C)) -> SimpleExpr {
    let (parent_table, parent_id) = parent;
    let counts = Query::select()
        .expr(Expr::col((LogicalFunctionRuns::Table, LogicalFunctionRuns::Status)))
        .expr_as(Func::count(Expr::col(Asterisk)), Alias::new("count"))
        .from(LogicalFunctionRuns::Table)
        .and_where(Expr::col((LogicalFunctionRuns::Table, run_parent_col)).equals((parent_table, parent_id)))
        .group_by_col((LogicalFunctionRuns::Table, LogicalFunctionRuns::Status))
        .take();
    let json = json_object_agg("inner_fr.status", "inner_fr.count");
    Expr::from(SubQueryStatement::from(Query::select().expr(Expr::cust(json)).from_subquery(counts, InnerFr).take()))
}

/// `id` last carries the keyset pagination tie-break.
fn time_ordered(name: &str, filter: Option<LogicalFunctionRuns>) -> IndexCreateStatement {
    let mut index = Index::create();
    index.name(name).table(LogicalFunctionRuns::Table);
    if let Some(filter) = filter {
        index.col(filter);
    }
    index.col(LogicalFunctionRuns::TriggeredOn).col(LogicalFunctionRuns::Id).to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn run_classes_partition_the_statuses() {
        let classes = [RUN_ACTIVE, RUN_BLOCKED, RUN_AWAITING, RUN_TERMINALS];
        let classified: Vec<&str> = classes.iter().flat_map(|c| c.iter().copied()).collect();
        let distinct: BTreeSet<&str> = classified.iter().copied().collect();
        assert_eq!(distinct.len(), classified.len(), "a status is in two classes");
        let statuses: BTreeSet<&str> = RUN_TRANSITIONS.iter().flat_map(|(from, to)| [*from, *to]).collect();
        assert_eq!(distinct, statuses, "every status is in exactly one class");
    }
}
