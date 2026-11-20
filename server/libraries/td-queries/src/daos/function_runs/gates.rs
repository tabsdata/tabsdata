//
// Copyright 2026 Tabsdata Inc.
//

//! The transactions a subscriber waits for. A subscriber writes outside
//! Tabsdata, where no failure can take the write back, so it runs only on data
//! that is certain to commit: every other transaction it reads from has to
//! commit first.

use sea_query::{Expr, ExprTrait, Iden, JoinType, Query, SelectStatement, SimpleExpr};
use ta_objects::SeaValue;
use td_objects::dxo::function::FunctionDBIden;
use td_objects::dxo::logical::{
    LogicalFunctionRunDBIden, LogicalFunctionRunInputDBIden, LogicalTableDataVersionDBIden,
};
use td_objects::types::basic::{Decorator, LogicalFunctionRunStatus};

/// Alias of the subscriber run that waits.
#[derive(Iden, Clone, Copy)]
pub(crate) struct Gated;

/// Alias of the uncommitted run, in another transaction, that produced a
/// version a [`Gated`] run reads: the gated run waits for its transaction to
/// commit.
#[derive(Iden, Clone, Copy)]
pub(crate) struct GateProducer;

#[derive(Iden, Clone, Copy)]
struct GatedFunction;

pub(crate) fn is_subscriber<T: Iden + 'static>(function: T) -> SimpleExpr {
    Expr::col((function, FunctionDBIden::Decorator)).eq(Decorator::Subscriber.as_sea_expr())
}

/// [`Gated`] × [`GateProducer`] over what a subscriber reads from other
/// transactions that have not committed, as a bare `FROM … JOIN …` for the
/// caller to project and filter.
pub(crate) fn gates() -> SelectStatement {
    Query::select()
        .from_as(LogicalFunctionRunDBIden::Table, Gated)
        .join_as(
            JoinType::InnerJoin,
            FunctionDBIden::Table,
            GatedFunction,
            Expr::col((GatedFunction, FunctionDBIden::Id))
                .equals((Gated, LogicalFunctionRunDBIden::FunctionVersionId))
                .and(is_subscriber(GatedFunction)),
        )
        .join(
            JoinType::InnerJoin,
            LogicalFunctionRunInputDBIden::Table,
            Expr::col((
                LogicalFunctionRunInputDBIden::Table,
                LogicalFunctionRunInputDBIden::LogicalFunctionRunId,
            ))
            .equals((Gated, LogicalFunctionRunDBIden::Id)),
        )
        .join(
            JoinType::InnerJoin,
            LogicalTableDataVersionDBIden::Table,
            Expr::col((
                LogicalTableDataVersionDBIden::Table,
                LogicalTableDataVersionDBIden::Id,
            ))
            .equals((
                LogicalFunctionRunInputDBIden::Table,
                LogicalFunctionRunInputDBIden::RequirementLogicalTableDataVersionId,
            )),
        )
        .join_as(
            JoinType::InnerJoin,
            LogicalFunctionRunDBIden::Table,
            GateProducer,
            Expr::col((GateProducer, LogicalFunctionRunDBIden::Id))
                .equals((
                    LogicalTableDataVersionDBIden::Table,
                    LogicalTableDataVersionDBIden::ProducerLogicalFunctionRunId,
                ))
                .and(
                    Expr::col((GateProducer, LogicalFunctionRunDBIden::TransactionId))
                        .ne(Expr::col((Gated, LogicalFunctionRunDBIden::TransactionId))),
                )
                .and(
                    Expr::col((GateProducer, LogicalFunctionRunDBIden::Status))
                        .ne(LogicalFunctionRunStatus::Committed.as_sea_expr()),
                ),
        )
        .take()
}
