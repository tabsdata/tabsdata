//
// Copyright 2026 Tabsdata Inc.
//

//! The roll-up as v13 shipped it, frozen.

use sea_query::{
    BinOper, CaseStatement, DynIden, Expr, ExprTrait, Func, Iden, IntoIden, IntoTableRef, JoinType, Query, SelectStatement, TableRef,
    UpdateStatement,
};

pub enum RollupArm {
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

pub struct RollupDef {
    pub parent: Parent,
    /// Whose statuses the ladder reads.
    pub children: Children,
    /// How a written logical run finds the parent to recompute.
    pub runs: Runs,
    /// How a written attempt finds the parent to recompute.
    pub physical: Physical,
    pub instants: Instants,
    pub ladder: &'static [RollupArm],
    /// The status when no arm holds: the ladder is total.
    pub fallback: &'static str,
    /// `ended_on` stays NULL until every child is in this set, so the column is
    /// a plain instant a list can order on.
    pub finished: &'static [&'static str],
}

pub struct Parent {
    pub table: DynIden,
    pub id: DynIden,
    pub status: DynIden,
    pub started_on: DynIden,
    pub ended_on: DynIden,
}

pub struct Children {
    pub table: DynIden,
    pub parent: DynIden,
    pub status: DynIden,
}

pub struct Runs {
    pub table: DynIden,
    pub id: DynIden,
    pub parent: DynIden,
}

/// Where a parent's `started_on` and `ended_on` come from.
pub enum Instants {
    /// Its attempts. Finished with none ended, it ends when the recompute that
    /// finished it runs; finished children never change again.
    Attempts,
    Children {
        started_on: DynIden,
        ended_on: DynIden,
    },
}

pub struct Physical {
    pub table: DynIden,
    pub id: DynIden,
    pub parent: DynIden,
    pub started_on: DynIden,
    pub ended_on: DynIden,
}

#[derive(Clone, Copy)]
pub enum Changed {
    LogicalRuns,
    PhysicalRuns,
}

#[derive(Iden, Clone, Copy)]
struct Target;

#[derive(Iden, Clone, Copy)]
struct Parents;

#[derive(Iden, Clone, Copy)]
#[iden = "children"]
struct Kids;

#[derive(Iden, Clone, Copy)]
struct PhysicalRuns;

#[derive(Iden, Clone, Copy)]
#[iden = "instants"]
struct AttemptInstants;

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
        self.update(None, None)
    }

    pub fn recompute_for(&self, changed: Changed, ids: impl IntoIterator<Item = Expr>) -> UpdateStatement {
        let (table, id, parent) = match changed {
            Changed::LogicalRuns => (&self.runs.table, &self.runs.id, &self.runs.parent),
            Changed::PhysicalRuns => (&self.physical.table, &self.physical.id, &self.physical.parent),
        };
        let changed = Query::select().column(parent.clone()).from(table.clone()).and_where(Expr::col(id.clone()).is_in(ids)).to_owned();
        self.update(Some(changed), Some(Expr::cust("transaction_timestamp()")))
    }

    fn update(&self, changed: Option<SelectStatement>, now: Option<Expr>) -> UpdateStatement {
        let s = |col: Col| Expr::col((Summary, col));
        let last_end = match (&self.instants, now) {
            (Instants::Attempts, Some(now)) => Func::coalesce([s(Col::EndedOn), now]).into(),
            _ => s(Col::EndedOn),
        };
        let ended_on = || -> Expr { Expr::case(s(Col::Finished), last_end.clone()).into() };
        let differs =
            |column: &DynIden, value: Expr| Expr::col((Target, column.clone())).binary(BinOper::Custom("IS DISTINCT FROM"), value);
        Query::update()
            .table(self.parent.table.clone().into_table_ref().alias(Target))
            .value(self.parent.status.clone(), s(Col::Status))
            .value(self.parent.started_on.clone(), s(Col::StartedOn))
            .value(self.parent.ended_on.clone(), ended_on())
            .from(TableRef::SubQuery(Box::new(self.summary(changed.as_ref())), Summary.into_iden()))
            .and_where(Expr::col((Target, self.parent.id.clone())).equals((Summary, Col::Id)))
            .and_where(
                differs(&self.parent.status, s(Col::Status))
                    .or(differs(&self.parent.started_on, s(Col::StartedOn)))
                    .or(differs(&self.parent.ended_on, ended_on())),
            )
            .to_owned()
    }

    fn summary(&self, changed: Option<&SelectStatement>) -> SelectStatement {
        let parent_id = Expr::col((Parents, self.parent.id.clone()));
        let (started_on, ended_on) = match &self.instants {
            Instants::Attempts => (Expr::col((AttemptInstants, Col::StartedOn)), Expr::col((AttemptInstants, Col::EndedOn))),
            Instants::Children { started_on, ended_on } => (Expr::col((Kids, started_on.clone())), Expr::col((Kids, ended_on.clone()))),
        };
        let mut summary = Query::select()
            .expr_as(parent_id.clone(), Col::Id)
            .expr_as(self.ladder(), Col::Status)
            .expr_as(self.finished(), Col::Finished)
            .expr_as(Func::min(started_on), Col::StartedOn)
            .expr_as(Func::max(ended_on), Col::EndedOn)
            .from(self.parent.table.clone().into_table_ref().alias(Parents))
            .join(
                JoinType::LeftJoin,
                self.children.table.clone().into_table_ref().alias(Kids),
                Expr::col((Kids, self.children.parent.clone())).equals((Parents, self.parent.id.clone())),
            )
            .to_owned();
        if let Instants::Attempts = self.instants {
            summary.join_subquery(
                JoinType::LeftJoin,
                self.attempt_instants(changed),
                AttemptInstants,
                Expr::col((AttemptInstants, Col::ParentId)).equals((Parents, self.parent.id.clone())),
            );
        }
        summary
            .and_where_option(changed.map(|changed| parent_id.in_subquery(changed.clone())))
            .group_by_col((Parents, self.parent.id.clone()))
            .to_owned()
    }

    fn attempt_instants(&self, changed: Option<&SelectStatement>) -> SelectStatement {
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

    /// A parent with no children at all counts zero rows but one joined NULL, so
    /// it is never finished — matching the ladder's own empty case.
    fn finished(&self) -> Expr {
        Expr::cust("count(*)").eq(self.count_in(self.finished))
    }

    fn count_in(&self, statuses: &[&str]) -> Expr {
        Expr::cust_with_expr("count(*) FILTER (WHERE $1)", Expr::col((Kids, self.children.status.clone())).is_in(statuses.iter().copied()))
    }

    fn count_all(&self) -> Expr {
        Func::count(Expr::col((Kids, self.children.status.clone()))).into()
    }
}
