//
// Copyright 2026 Tabsdata Inc.
//

//! The roll-up as v10 and v11 shipped it: executions and transactions both
//! from their logical runs. Full recompute of a parent from its children,
//! never an increment:
//!
//! ```sql
//! UPDATE parent AS target SET status, started_on, ended_on
//! FROM (
//!     SELECT parents.id, <ladder over logical_runs.status>, <every logical run finished>,
//!            MIN(instants.started_on), MAX(instants.ended_on)
//!     FROM parent AS parents
//!     LEFT JOIN logical_runs ON logical_runs.parent = parents.id
//!     LEFT JOIN (SELECT parent, MIN(started_on), MAX(ended_on)
//!                FROM physical_runs [WHERE parent IN <changed>] GROUP BY parent) AS instants
//!            ON instants.parent = parents.id
//!     [WHERE parents.id IN <changed>]
//!     GROUP BY parents.id
//! ) AS summary
//! WHERE target.id = summary.id
//! ```
//!
//! `<changed>` is the parents of the children a door just wrote; the backfill
//! leaves it out. No lock of its own: the doors that run it are typed on the
//! `XW` connection, which holds the project's exclusive-write lock for the
//! whole transaction.

use sea_query::{
    CaseStatement, DynIden, Expr, ExprTrait, Func, Iden, IntoIden, IntoTableRef, JoinType, Query, SelectStatement, TableRef,
    UpdateStatement, Value,
};

pub(crate) enum RollupArm {
    All {
        all: &'static [&'static str],
        status: &'static str,
    },
    AllWithAny {
        all: &'static [&'static str],
        any: &'static [&'static str],
        status: &'static str,
    },
    Any {
        any: &'static [&'static str],
        status: &'static str,
    },
}

pub(crate) struct RollupDef {
    pub parent: Parent,
    pub logical: Logical,
    pub physical: Physical,
    pub ladder: &'static [RollupArm],
    pub fallback: &'static str,
    /// `ended_on` stays NULL until every logical run is in this set, so the column is
    /// a plain instant a list can order on.
    pub finished: &'static [&'static str],
}

pub(crate) struct Parent {
    pub table: DynIden,
    pub id: DynIden,
    pub status: DynIden,
    pub started_on: DynIden,
    pub ended_on: DynIden,
}

pub(crate) struct Logical {
    pub table: DynIden,
    pub id: DynIden,
    pub parent: DynIden,
    pub status: DynIden,
}

pub(crate) struct Physical {
    pub table: DynIden,
    pub id: DynIden,
    pub parent: DynIden,
    pub started_on: DynIden,
    pub ended_on: DynIden,
}

#[derive(Clone, Copy)]
pub(crate) enum Changed {
    LogicalRuns,
    PhysicalRuns,
}

#[derive(Iden, Clone, Copy)]
struct Target;

#[derive(Iden, Clone, Copy)]
struct Parents;

#[derive(Iden, Clone, Copy)]
struct LogicalRuns;

#[derive(Iden, Clone, Copy)]
struct PhysicalRuns;

#[derive(Iden, Clone, Copy)]
struct Instants;

#[derive(Iden, Clone, Copy)]
struct Summary;

#[derive(Iden, Clone, Copy)]
enum Col {
    Id,
    ParentId,
    Status,
    Finished,
    StartedOn,
    EndedOn,
}

impl RollupDef {
    pub fn backfill(&self) -> UpdateStatement {
        self.update(None)
    }

    pub fn recompute_for(&self, changed: Changed, ids: impl IntoIterator<Item = Value>) -> UpdateStatement {
        let (table, id, parent) = match changed {
            Changed::LogicalRuns => (&self.logical.table, &self.logical.id, &self.logical.parent),
            Changed::PhysicalRuns => (&self.physical.table, &self.physical.id, &self.physical.parent),
        };
        let changed = Query::select().column(parent.clone()).from(table.clone()).and_where(Expr::col(id.clone()).is_in(ids)).to_owned();
        self.update(Some(changed))
    }

    fn update(&self, changed: Option<SelectStatement>) -> UpdateStatement {
        let s = |col: Col| Expr::col((Summary, col));
        Query::update()
            .table(self.parent.table.clone().into_table_ref().alias(Target))
            .value(self.parent.status.clone(), s(Col::Status))
            .value(self.parent.started_on.clone(), s(Col::StartedOn))
            .value(self.parent.ended_on.clone(), Expr::case(s(Col::Finished), s(Col::EndedOn)))
            .from(TableRef::SubQuery(Box::new(self.summary(changed.as_ref())), Summary.into_iden()))
            .and_where(Expr::col((Target, self.parent.id.clone())).equals((Summary, Col::Id)))
            .to_owned()
    }

    fn summary(&self, changed: Option<&SelectStatement>) -> SelectStatement {
        let parent_id = Expr::col((Parents, self.parent.id.clone()));
        Query::select()
            .expr_as(parent_id.clone(), Col::Id)
            .expr_as(self.ladder(), Col::Status)
            .expr_as(self.finished(), Col::Finished)
            .expr_as(Func::min(Expr::col((Instants, Col::StartedOn))), Col::StartedOn)
            .expr_as(Func::max(Expr::col((Instants, Col::EndedOn))), Col::EndedOn)
            .from(self.parent.table.clone().into_table_ref().alias(Parents))
            .join(
                JoinType::LeftJoin,
                self.logical.table.clone().into_table_ref().alias(LogicalRuns),
                Expr::col((LogicalRuns, self.logical.parent.clone())).equals((Parents, self.parent.id.clone())),
            )
            .join_subquery(
                JoinType::LeftJoin,
                self.instants(changed),
                Instants,
                Expr::col((Instants, Col::ParentId)).equals((Parents, self.parent.id.clone())),
            )
            .and_where_option(changed.map(|changed| parent_id.in_subquery(changed.clone())))
            .group_by_col((Parents, self.parent.id.clone()))
            .to_owned()
    }

    fn instants(&self, changed: Option<&SelectStatement>) -> SelectStatement {
        let parent = Expr::col((PhysicalRuns, self.physical.parent.clone()));
        Query::select()
            .expr_as(parent.clone(), Col::ParentId)
            .expr_as(Func::min(Expr::col((PhysicalRuns, self.physical.started_on.clone()))), Col::StartedOn)
            .expr_as(Func::max(Expr::col((PhysicalRuns, self.physical.ended_on.clone()))), Col::EndedOn)
            .from(self.physical.table.clone().into_table_ref().alias(PhysicalRuns))
            .and_where_option(changed.map(|changed| parent.in_subquery(changed.clone())))
            .group_by_col((PhysicalRuns, self.physical.parent.clone()))
            .to_owned()
    }

    fn ladder(&self) -> Expr {
        let mut case: Option<CaseStatement> = None;
        for arm in self.ladder {
            let (cond, status) = match arm {
                RollupArm::All { all, status } => (self.count_in(all).eq(self.count_all()), status),
                RollupArm::AllWithAny { all, any, status } => {
                    (self.count_in(all).eq(self.count_all()).and(self.count_in(any).gt(0)), status)
                }
                RollupArm::Any { any, status } => (self.count_in(any).gt(0), status),
            };
            case = Some(match case {
                None => Expr::case(cond, *status),
                Some(case) => case.case(cond, *status),
            });
        }
        case.expect("a ladder has at least one arm").finally(self.fallback).into()
    }

    /// A parent with no logical runs at all counts zero rows but one joined NULL, so
    /// it is never finished — matching the ladder's own empty case.
    fn finished(&self) -> Expr {
        Expr::cust("count(*)").eq(self.count_in(self.finished))
    }

    fn count_in(&self, statuses: &[&str]) -> Expr {
        Expr::cust_with_expr(
            "count(*) FILTER (WHERE $1)",
            Expr::col((LogicalRuns, self.logical.status.clone())).is_in(statuses.iter().copied()),
        )
    }

    fn count_all(&self) -> Expr {
        Func::count(Expr::col((LogicalRuns, self.logical.status.clone()))).into()
    }
}
