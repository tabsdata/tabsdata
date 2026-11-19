//
// Copyright 2026 Tabsdata Inc.
//

//! The runs a subscriber waits for. A subscriber writes outside Tabsdata,
//! where no failure can take the write back, so it runs only on data that is
//! certain to commit: every other transaction it reads from has to commit
//! first. Its gates are the runs of those transactions.

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

/// Alias of a run a [`Gated`] one waits for.
#[derive(Iden, Clone, Copy)]
pub(crate) struct Gate;

#[derive(Iden, Clone, Copy)]
struct GatedFunction;

#[derive(Iden, Clone, Copy)]
struct GateProducer;

pub(crate) fn is_subscriber<T: Iden + 'static>(function: T) -> SimpleExpr {
    Expr::col((function, FunctionDBIden::Decorator)).eq(Decorator::Subscriber.as_sea_expr())
}

/// [`Gated`] × [`Gate`] over every run of the other transactions a
/// subscriber reads from, as a bare `FROM … JOIN …` for the caller to project
/// and filter.
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
                ),
        )
        .join_as(
            JoinType::InnerJoin,
            LogicalFunctionRunDBIden::Table,
            Gate,
            Expr::col((Gate, LogicalFunctionRunDBIden::TransactionId))
                .equals((GateProducer, LogicalFunctionRunDBIden::TransactionId)),
        )
        .take()
}

/// A [`Gate`] that is Failed or OnHold: what holds its [`Gated`] run.
pub(crate) fn gate_blocks() -> SimpleExpr {
    Expr::col((Gate, LogicalFunctionRunDBIden::Status)).is_in([
        LogicalFunctionRunStatus::Failed.as_sea_expr(),
        LogicalFunctionRunStatus::OnHold.as_sea_expr(),
    ])
}
