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
//! This file also owns the cross-cutting views that drive off LFR:
//! `function_runs__with_names` (LFR + currently-bound PFR via pointer hop)
//! and the dialect-specific `global_status_summary` / `execution_status_summary`
//! / `transaction_status_summary` views (LFR-driven aggregations).

use sea_query::{
    Alias, Asterisk, ColumnDef, Expr, ExprTrait, ForeignKey, ForeignKeyAction, Func, Iden, Index, IndexCreateStatement, IntoIden, JoinType,
    Query, SubQueryStatement, Table, TableCreateStatement,
};

use super::collections::{Collections, CollectionsResolved};
use super::executions::Executions;
use super::functions::Functions;
use super::groups::Groups;
use super::logical_function_run_pointers::POINTER_STATUS_BOUND;
use super::logical_function_run_pointers_current::LogicalFunctionRunPointersCurrent;
use super::physical_function_runs::PhysicalFunctionRuns;
use super::projects::{Projects, ProjectsResolved};
use super::transactions::Transactions;
use super::users::Users;
use crate::schema::Entity;
use crate::schema::backend::Backend;
use crate::schema::sql::{StickyTerminalsTriggerDef, TriggerSpec, ViewDef};

#[derive(Iden, Clone, Copy)]
#[iden = "function_runs__with_names"]
pub struct FunctionRunsWithNames;

/// View names of the LFR-driven status summary views. Declared here (with the
/// views' producer) and consumed by `executions__with_status` /
/// `transactions__with_status` in `with_status_views.rs`.
#[derive(Iden, Clone, Copy)]
pub struct ExecutionStatusSummary;

#[derive(Iden, Clone, Copy)]
pub struct TransactionStatusSummary;

/// Alias for the per-status count derived table inside the status-summary
/// scalar subqueries; the dialect JSON aggregate reads its `status`/`count`
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
}

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        vec![
            // logical_function_runs — identity row, insert-once.
            Table::create()
                .table(LogicalFunctionRuns::Table)
                .col(ColumnDef::new(LogicalFunctionRuns::Id).text().not_null().primary_key())
                .col(ColumnDef::new(LogicalFunctionRuns::ProjectId).text().not_null())
                .col(ColumnDef::new(LogicalFunctionRuns::CollectionId).text().not_null())
                .col(ColumnDef::new(LogicalFunctionRuns::ExecutionId).text().not_null())
                .col(ColumnDef::new(LogicalFunctionRuns::TransactionId).text().not_null())
                .col(ColumnDef::new(LogicalFunctionRuns::FunctionId).text().not_null())
                // Plan-time fvid the LFR was minted against; topology
                // reference for primitives. Set once at plan-build.
                .col(ColumnDef::new(LogicalFunctionRuns::FunctionVersionId).text().not_null().default(""))
                .col(ColumnDef::new(LogicalFunctionRuns::Trigger).text().not_null())
                .col(ColumnDef::new(LogicalFunctionRuns::TriggeredOn).timestamp_with_time_zone().not_null())
                .col(ColumnDef::new(LogicalFunctionRuns::TriggeredById).text().not_null())
                .col(ColumnDef::new(LogicalFunctionRuns::Status).text().not_null())
                .col(ColumnDef::new(LogicalFunctionRuns::GroupId).text().not_null())
                .foreign_key(
                    ForeignKey::create()
                        .from(LogicalFunctionRuns::Table, LogicalFunctionRuns::CollectionId)
                        .to(Collections::Table, Collections::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .foreign_key(
                    // Composite: group ids are only unique per project.
                    ForeignKey::create()
                        .from(LogicalFunctionRuns::Table, (LogicalFunctionRuns::ProjectId, LogicalFunctionRuns::GroupId))
                        .to(Groups::Table, (Groups::ProjectId, Groups::Id))
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
            Index::create()
                .name("logical_function_runs___status___idx")
                .table(LogicalFunctionRuns::Table)
                .col(LogicalFunctionRuns::Status)
                .to_owned(),
            // Group-filtered metrics / Sankey hot path: covers
            // `(group_id, collection_id)` to support both group-level
            // aggregations and per-collection drill-downs.
            Index::create()
                .name("logical_function_runs___group_id__collection_id___idx")
                .table(LogicalFunctionRuns::Table)
                .col(LogicalFunctionRuns::GroupId)
                .col(LogicalFunctionRuns::CollectionId)
                .to_owned(),
        ]
    }

    fn create_views(&self, b: &Backend) -> Vec<ViewDef> {
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
                    b.name_or_id((Users::Table, Users::Name), (LogicalFunctionRuns::Table, LogicalFunctionRuns::TriggeredById)),
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
                .expr_as(Expr::col((PhysicalFunctionRuns::Table, PhysicalFunctionRuns::EndedOnHourMs)), Alias::new("ended_on_hour_ms"))
                .expr_as(Expr::col((PhysicalFunctionRuns::Table, PhysicalFunctionRuns::EndedOnDayMs)), Alias::new("ended_on_day_ms"))
                .expr_as(Expr::col((PhysicalFunctionRuns::Table, PhysicalFunctionRuns::EndedOnWeekMs)), Alias::new("ended_on_week_ms"))
                .expr_as(Expr::col((PhysicalFunctionRuns::Table, PhysicalFunctionRuns::EndedOnMonthMs)), Alias::new("ended_on_month_ms"))
                .expr_as(
                    Expr::col((PhysicalFunctionRuns::Table, PhysicalFunctionRuns::MetricTableVersions)),
                    Alias::new("metric_table_versions"),
                )
                .expr_as(Expr::col((PhysicalFunctionRuns::Table, PhysicalFunctionRuns::MetricRows)), Alias::new("metric_rows"))
                .expr_as(Expr::col((PhysicalFunctionRuns::Table, PhysicalFunctionRuns::MetricDataBytes)), Alias::new("metric_data_bytes"))
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
                    // Group ids are only unique per project; match project_id too.
                    Expr::col((LogicalFunctionRuns::Table, LogicalFunctionRuns::GroupId)).equals((groups_alias.clone(), Groups::Id)).and(
                        Expr::col((LogicalFunctionRuns::Table, LogicalFunctionRuns::ProjectId)).equals((groups_alias, Groups::ProjectId)),
                    ),
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
                // Currently-bound PFR via the pointer-current snapshot; the
                // status predicate makes non-Bound rows read as unbound
                // without relying on their NULL concrete id.
                .join(
                    JoinType::LeftJoin,
                    LogicalFunctionRunPointersCurrent::Table,
                    Expr::col((LogicalFunctionRunPointersCurrent::Table, LogicalFunctionRunPointersCurrent::LogicalFunctionRunId))
                        .equals((LogicalFunctionRuns::Table, LogicalFunctionRuns::Id))
                        .and(
                            Expr::col((LogicalFunctionRunPointersCurrent::Table, LogicalFunctionRunPointersCurrent::Status))
                                .eq(POINTER_STATUS_BOUND),
                        ),
                )
                .join(
                    JoinType::LeftJoin,
                    PhysicalFunctionRuns::Table,
                    Expr::col((PhysicalFunctionRuns::Table, PhysicalFunctionRuns::Id))
                        .equals((LogicalFunctionRunPointersCurrent::Table, LogicalFunctionRunPointersCurrent::PhysicalFunctionRunId)),
                )
                .to_owned(),
        );

        let mut views = vec![function_runs_with_names];
        views.extend(status_summary_views(b));
        views
    }

    fn create_db_triggers(&self) -> Vec<TriggerSpec> {
        vec![TriggerSpec::StickyTerminals(StickyTerminalsTriggerDef {
            name: format!("{}__sticky_terminals", LogicalFunctionRuns::Table.to_string()),
            source_table: LogicalFunctionRuns::Table.into_iden(),
            status_col: LogicalFunctionRuns::Status.into_iden(),
            terminal_values: &["C", "X", "Y"],
        })]
    }
}

/// Status summary views that use dialect-specific JSON aggregation.
/// The CASE/COUNT expressions and the scalar subquery wrapping are all
/// sea-query builders; only the JSON aggregate function itself stays as
/// `Expr::cust()`, because its name is inherently dialect-specific.
///
/// Status lives on `logical_function_runs.status`; aggregation rows up
/// from LFRs (one per logical run) rather than FRs (per-attempt audit).
fn status_summary_views(b: &Backend) -> Vec<ViewDef> {
    // Helper: COUNT(CASE WHEN logical_function_runs.status IN (values) THEN 1 END)
    let count_status_in = |statuses: &[&str]| {
        Expr::expr(Func::count(Expr::expr(Expr::case(
            Expr::col((LogicalFunctionRuns::Table, LogicalFunctionRuns::Status)).is_in(statuses.iter().copied()),
            Expr::val(1),
        ))))
    };

    // Helper: COUNT(logical_function_runs.status)
    let count_all = || Expr::expr(Func::count(Expr::col((LogicalFunctionRuns::Table, LogicalFunctionRuns::Status))));

    // --- status CASE (global + execution) ---
    let status_case = Expr::case(count_status_in(&["S"]).eq(count_all()), Expr::val("S"))
        .case(count_status_in(&["C", "X", "Y"]).eq(count_all()), Expr::val("F"))
        .case(count_status_in(&["C", "D", "F", "H"]).eq(count_all()).and(count_status_in(&["F", "H"]).gt(0)), Expr::val("L"))
        .case(count_status_in(&["S", "RR", "RS", "R", "D", "E"]).gt(0), Expr::val("R"))
        .finally(Expr::val("U"));

    // --- finished (global + execution) ---
    let finished = Expr::expr(Func::count(Expr::col(Asterisk))).eq(count_status_in(&["C", "X", "Y"]));

    // --- txn status CASE (transaction — individual C/X/Y branches) ---
    let txn_status_case = Expr::case(count_status_in(&["S"]).eq(count_all()), Expr::val("S"))
        .case(count_status_in(&["C"]).eq(count_all()), Expr::val("C"))
        .case(count_status_in(&["X"]).eq(count_all()), Expr::val("X"))
        .case(count_status_in(&["Y"]).eq(count_all()), Expr::val("Y"))
        .case(count_status_in(&["D", "F", "H"]).eq(count_all()).and(count_status_in(&["F", "H"]).gt(0)), Expr::val("L"))
        .case(count_status_in(&["S", "RR", "RS", "R", "D", "E"]).gt(0), Expr::val("R"))
        .finally(Expr::val("U"));

    // --- txn finished ---
    let txn_finished = Expr::expr(Func::count(Expr::col(Asterisk))).eq(count_status_in(&["C", "X", "Y"]));

    // --- JSON subqueries (fully sea-query; only the dialect JSON aggregate is custom) ---
    let json_agg = b.json_object_agg("inner_fr.status", "inner_fr.count");

    // A per-status `(status, count)` select, ready to be wrapped as a scalar
    // subquery. The optional `correlate` predicate ties the counts to the outer
    // row (per-execution / per-transaction); the global summary passes `None`.
    let inner_status_counts = |correlate: Option<Expr>| {
        let mut q = Query::select();
        q.expr(Expr::col((LogicalFunctionRuns::Table, LogicalFunctionRuns::Status)))
            .expr_as(Func::count(Expr::col(Asterisk)), Alias::new("count"))
            .from(LogicalFunctionRuns::Table)
            .group_by_col((LogicalFunctionRuns::Table, LogicalFunctionRuns::Status));
        if let Some(pred) = correlate {
            q.and_where(pred);
        }
        q.take()
    };

    // Wrap an inner status-count select as a scalar subquery that JSON-aggregates
    // its rows into a single object. `from_subquery` owns the `inner_fr` alias, so
    // the subquery wrapping is the query builder's job, not a `format!` string.
    let json_count_subquery = |inner| {
        Expr::from(SubQueryStatement::from(Query::select().expr(Expr::cust(json_agg.clone())).from_subquery(inner, InnerFr).take()))
    };

    let json_subquery = json_count_subquery(inner_status_counts(None));
    let exec_json_subquery = json_count_subquery(inner_status_counts(Some(
        Expr::col((LogicalFunctionRuns::Table, LogicalFunctionRuns::ExecutionId)).equals((Executions::Table, Executions::Id)),
    )));
    let txn_json_subquery = json_count_subquery(inner_status_counts(Some(
        Expr::col((LogicalFunctionRuns::Table, LogicalFunctionRuns::TransactionId)).equals((Transactions::Table, Transactions::Id)),
    )));

    // --- View definitions ---
    let global = ViewDef::new(
        Alias::new("global_status_summary"),
        Query::select()
            .expr_as(status_case.clone(), Alias::new("status"))
            .expr_as(finished.clone(), Alias::new("finished"))
            .expr_as(json_subquery, Alias::new("function_run_status_count"))
            .from(LogicalFunctionRuns::Table)
            .to_owned(),
    );

    let execution = ViewDef::new(
        ExecutionStatusSummary,
        Query::select()
            .expr_as(Expr::col((Executions::Table, Executions::Id)), Alias::new("execution_id"))
            .expr_as(status_case, Alias::new("status"))
            .expr_as(finished, Alias::new("finished"))
            .expr_as(exec_json_subquery, Alias::new("function_run_status_count"))
            .from(Executions::Table)
            .join(
                JoinType::LeftJoin,
                LogicalFunctionRuns::Table,
                Expr::col((LogicalFunctionRuns::Table, LogicalFunctionRuns::ExecutionId)).equals((Executions::Table, Executions::Id)),
            )
            .group_by_col((Executions::Table, Executions::Id))
            .to_owned(),
    );

    let transaction = ViewDef::new(
        TransactionStatusSummary,
        Query::select()
            .expr_as(Expr::col((Transactions::Table, Transactions::Id)), Alias::new("transaction_id"))
            .expr_as(txn_status_case, Alias::new("status"))
            .expr_as(txn_finished, Alias::new("finished"))
            .expr_as(txn_json_subquery, Alias::new("function_run_status_count"))
            .from(Transactions::Table)
            .join(
                JoinType::LeftJoin,
                LogicalFunctionRuns::Table,
                Expr::col((LogicalFunctionRuns::Table, LogicalFunctionRuns::TransactionId)).equals((Transactions::Table, Transactions::Id)),
            )
            .group_by_col((Transactions::Table, Transactions::Id))
            .to_owned(),
    );

    vec![global, execution, transaction]
}
