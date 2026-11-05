//
// Copyright 2026 Tabsdata Inc.
//

//! Full recompute of a parent from its children, never an increment:
//!
//! ```sql
//! WITH children AS MATERIALIZED (
//!          SELECT parent, status FROM children [WHERE parent IN <parents>]),
//!      instants AS MATERIALIZED (
//!          SELECT parent, MIN(started_on), MAX(ended_on) FROM physical_runs
//!          [WHERE parent IN <parents>] GROUP BY parent),
//!      summary AS MATERIALIZED (
//!          SELECT parents.id, <ladder over children.status>, <every child finished>,
//!                 MIN(instants.started_on), MAX(instants.ended_on)
//!          FROM parent AS parents
//!          LEFT JOIN children ON children.parent = parents.id
//!          LEFT JOIN instants ON instants.parent = parents.id
//!          [WHERE parents.id IN <parents>]
//!          GROUP BY parents.id)
//! UPDATE parent AS target SET status, started_on, ended_on
//! FROM summary
//! WHERE [target.id IN <parents> AND] target.id = summary.id
//!   AND (target.status, target.started_on, target.ended_on)
//!       IS DISTINCT FROM (summary.status, summary.started_on, summary.ended_on)
//! ```
//!
//! The children are a transaction's logical runs, or an execution's
//! transactions; the backfill leaves `<parents>` out. Each table is read once
//! and each aggregate computed once, so the plan holds without statistics. A
//! parent the recompute would not change is not written, so it takes no new
//! row version. An execution spans its transactions and is recomputed after
//! them. Callers hold the project's lock.

use sea_query::{
    BinOper, CaseStatement, CommonTableExpression, DynIden, Expr, ExprTrait, Func, Iden, IntoIden, IntoTableRef, JoinType, Query,
    SelectStatement, WithClause, WithQuery,
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
    pub parent: DynIden,
    pub started_on: DynIden,
    pub ended_on: DynIden,
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
    pub fn backfill(&self) -> WithQuery {
        self.update(None)
    }

    pub fn recompute(&self, parents: impl IntoIterator<Item = Expr>) -> WithQuery {
        self.update(Some(parents.into_iter().collect()))
    }

    fn update(&self, parents: Option<Vec<Expr>>) -> WithQuery {
        let within = |column: Expr| parents.clone().map(|parents| column.is_in(parents));
        let s = |col: Col| Expr::col((Summary, col));
        let last_end = match (&self.instants, &parents) {
            (Instants::Attempts, Some(_)) => Func::coalesce([s(Col::EndedOn), Expr::cust("transaction_timestamp()")]).into(),
            _ => s(Col::EndedOn),
        };
        let ended_on = || -> Expr { Expr::case(s(Col::Finished), last_end.clone()).into() };
        let differs =
            |column: &DynIden, value: Expr| Expr::col((Target, column.clone())).binary(BinOper::Custom("IS DISTINCT FROM"), value);
        let mut with = WithClause::new();
        with.cte(materialized(self.children(&within), Kids));
        if let Instants::Attempts = self.instants {
            with.cte(materialized(self.attempt_instants(&within), AttemptInstants));
        }
        with.cte(materialized(self.summary(&within), Summary));
        Query::update()
            .table(self.parent.table.clone().into_table_ref().alias(Target))
            .value(self.parent.status.clone(), s(Col::Status))
            .value(self.parent.started_on.clone(), s(Col::StartedOn))
            .value(self.parent.ended_on.clone(), ended_on())
            .from(Summary)
            .and_where_option(within(Expr::col((Target, self.parent.id.clone()))))
            .and_where(Expr::col((Target, self.parent.id.clone())).equals((Summary, Col::Id)))
            .and_where(
                differs(&self.parent.status, s(Col::Status))
                    .or(differs(&self.parent.started_on, s(Col::StartedOn)))
                    .or(differs(&self.parent.ended_on, ended_on())),
            )
            .to_owned()
            .with(with)
    }

    fn children(&self, within: &impl Fn(Expr) -> Option<Expr>) -> SelectStatement {
        let mut children = Query::select()
            .column(self.children.parent.clone())
            .column(self.children.status.clone())
            .from(self.children.table.clone())
            .and_where_option(within(Expr::col(self.children.parent.clone())))
            .to_owned();
        if let Instants::Children { started_on, ended_on } = &self.instants {
            children.column(started_on.clone()).column(ended_on.clone());
        }
        children
    }

    fn summary(&self, within: &impl Fn(Expr) -> Option<Expr>) -> SelectStatement {
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
            .join(JoinType::LeftJoin, Kids, Expr::col((Kids, self.children.parent.clone())).equals((Parents, self.parent.id.clone())))
            .to_owned();
        if let Instants::Attempts = self.instants {
            summary.join(
                JoinType::LeftJoin,
                AttemptInstants,
                Expr::col((AttemptInstants, Col::ParentId)).equals((Parents, self.parent.id.clone())),
            );
        }
        summary.and_where_option(within(parent_id)).group_by_col((Parents, self.parent.id.clone())).to_owned()
    }

    fn attempt_instants(&self, within: &impl Fn(Expr) -> Option<Expr>) -> SelectStatement {
        let parent = Expr::col((PhysicalRuns, self.physical.parent.clone()));
        Query::select()
            .expr_as(parent.clone(), Col::ParentId)
            .expr_as(Func::min(Expr::col((PhysicalRuns, self.physical.started_on.clone()))), Col::StartedOn)
            .expr_as(Func::max(Expr::col((PhysicalRuns, self.physical.ended_on.clone()))), Col::EndedOn)
            .from(self.physical.table.clone().into_table_ref().alias(PhysicalRuns))
            .and_where_option(within(parent))
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

fn materialized(query: SelectStatement, name: impl IntoIden) -> CommonTableExpression {
    CommonTableExpression::new().query(query).table_name(name).materialized(true).to_owned()
}

#[cfg(test)]
mod tests {
    use sea_query::{Alias, PostgresQueryBuilder};

    use super::*;

    const LADDER: &[RollupArm] = &[
        RollupArm::All { all: &["S"], status: "S" },
        RollupArm::AllWithAny {
            all: &["C", "D"],
            any: &["F"],
            status: "L",
        },
        RollupArm::Any { any: &["R"], status: "R" },
    ];

    fn def() -> RollupDef {
        RollupDef {
            parent: Parent {
                table: Alias::new("parents").into_iden(),
                id: Alias::new("id").into_iden(),
                status: Alias::new("status").into_iden(),
                started_on: Alias::new("started_on").into_iden(),
                ended_on: Alias::new("ended_on").into_iden(),
            },
            children: Children {
                table: Alias::new("logical_runs").into_iden(),
                parent: Alias::new("parent_id").into_iden(),
                status: Alias::new("status").into_iden(),
            },
            physical: Physical {
                table: Alias::new("physical_runs").into_iden(),
                parent: Alias::new("parent_id").into_iden(),
                started_on: Alias::new("started_on").into_iden(),
                ended_on: Alias::new("ended_on").into_iden(),
            },
            instants: Instants::Attempts,
            ladder: LADDER,
            fallback: "U",
            finished: &["C"],
        }
    }

    fn backfill() -> String {
        def().backfill().to_string(PostgresQueryBuilder)
    }

    #[test]
    fn ladder_arms_render_in_order() {
        let sql = backfill();
        let ladder = sql.split(" AS \"status\"").next().unwrap();
        let s = ladder.find("THEN 'S'").unwrap();
        let l = ladder.find("THEN 'L'").unwrap();
        let r = ladder.find("THEN 'R'").unwrap();
        assert!(s < l && l < r, "{ladder}");
        assert!(ladder.contains("ELSE 'U'"), "{ladder}");
    }

    #[test]
    fn all_with_any_needs_both_counts() {
        let sql = backfill();
        assert!(
            sql.contains(
                "WHEN ((count(*) FILTER (WHERE \"children\".\"status\" IN ('C', 'D'))) = COUNT(\"children\".\"status\") \
                 AND (count(*) FILTER (WHERE \"children\".\"status\" IN ('F'))) > 0) THEN 'L'"
            ),
            "{sql}"
        );
    }

    #[test]
    fn ended_on_is_kept_only_once_finished() {
        let sql = backfill();
        assert!(sql.contains("\"ended_on\" = (CASE WHEN (\"summary\".\"finished\") THEN \"summary\".\"ended_on\" END)"), "{sql}");
    }

    #[test]
    fn backfill_recomputes_every_parent() {
        let sql = backfill();
        assert!(!sql.contains(r#""parent_id" IN"#) && !sql.contains(r#""id" IN"#), "{sql}");
    }

    #[test]
    fn recompute_reads_each_table_by_the_parents_once() {
        let sql = def().recompute([1.into(), 2.into()]).to_string(PostgresQueryBuilder);
        for scoped in [
            r#""parent_id" IN (1, 2)"#,
            r#""physical_runs"."parent_id" IN (1, 2)"#,
            r#""parents"."id" IN (1, 2)"#,
            r#""target"."id" IN (1, 2)"#,
        ] {
            assert!(sql.contains(scoped), "{scoped} in {sql}");
        }
        assert_eq!(sql.matches("MATERIALIZED (").count(), 3, "{sql}");
    }
}
