//
// Copyright 2026 Tabs Data Inc.
//

//! `executions__with_status` and `transactions__with_status` — listing
//! views that consume the LFR-driven `*_status_summary` views to expose
//! the rolled-up run / transaction status. Declared last in `ENTITIES`
//! so every base view they read is already emitted when the view loop
//! reaches here, keeping the single-pass emission dep-safe.

use sea_query::{Alias, Asterisk, Expr, ExprTrait, Func, Iden, IntoIden, JoinType, Query};

use super::executions::ExecutionsWithNames;
use super::logical_function_runs::{ExecutionStatusSummary, TransactionStatusSummary};
use super::physical_function_runs::PhysicalFunctionRuns;
use super::transactions::TransactionsWithNames;
use crate::schema::Entity;
use crate::schema::backend::Backend;
use crate::schema::sql::ViewDef;

#[derive(Iden, Clone, Copy)]
#[iden = "executions__with_status"]
pub struct ExecutionsWithStatus;

#[derive(Iden, Clone, Copy)]
#[iden = "transactions__with_status"]
pub struct TransactionsWithStatus;

pub(super) struct Defs;

impl Entity for Defs {
    fn create_views(&self, _b: &Backend) -> Vec<ViewDef> {
        vec![
            with_status_view(
                ExecutionsWithStatus,
                ExecutionsWithNames,
                ExecutionStatusSummary,
                PhysicalFunctionRuns::ExecutionId,
                "execution_id",
            ),
            with_status_view(
                TransactionsWithStatus,
                TransactionsWithNames,
                TransactionStatusSummary,
                PhysicalFunctionRuns::TransactionId,
                "transaction_id",
            ),
        ]
    }
}

/// Build a `*__with_status` view that joins a `*__with_names` source view to:
///   1. a per-partition PFR aggregate (min started_on / max ended_on), and
///   2. the corresponding `*_status_summary` view.
fn with_status_view<Name, SrcView, StatusView, PfrPartitionCol>(
    name: Name,
    src_view: SrcView,
    status_view: StatusView,
    pfr_partition_col: PfrPartitionCol,
    partition_label: &'static str,
) -> ViewDef
where
    Name: IntoIden,
    SrcView: IntoIden + Copy,
    StatusView: IntoIden + Copy,
    PfrPartitionCol: IntoIden,
{
    let pfr_partition_col = pfr_partition_col.into_iden();
    let pfr_agg = Alias::new("pfr_agg");
    let pfr_agg_subquery = Query::select()
        .expr_as(Expr::col((PhysicalFunctionRuns::Table, pfr_partition_col.clone())), Alias::new(partition_label))
        .expr_as(Func::min(Expr::col((PhysicalFunctionRuns::Table, PhysicalFunctionRuns::StartedOn))), Alias::new("started_on"))
        .expr_as(Func::max(Expr::col((PhysicalFunctionRuns::Table, PhysicalFunctionRuns::EndedOn))), Alias::new("ended_on"))
        .from(PhysicalFunctionRuns::Table)
        .group_by_col((PhysicalFunctionRuns::Table, pfr_partition_col))
        .to_owned();

    ViewDef::new(
        name,
        Query::select()
            .expr(Expr::col((src_view, Asterisk)))
            .expr_as(Expr::col((pfr_agg.clone(), Alias::new("started_on"))), Alias::new("started_on"))
            .expr_as(
                Expr::case(
                    Expr::col((status_view, Alias::new("finished"))),
                    Expr::col((pfr_agg.clone(), Alias::new("ended_on"))),
                ),
                Alias::new("ended_on"),
            )
            .expr_as(Expr::col((status_view, Alias::new("status"))), Alias::new("status"))
            .expr_as(
                Expr::col((status_view, Alias::new("function_run_status_count"))),
                Alias::new("function_run_status_count"),
            )
            .from(src_view)
            .join_subquery(
                JoinType::LeftJoin,
                pfr_agg_subquery,
                pfr_agg.clone(),
                Expr::col((pfr_agg, Alias::new(partition_label))).equals((src_view, Alias::new("id"))),
            )
            .join_as(
                JoinType::LeftJoin,
                status_view,
                status_view,
                Expr::col((status_view, Alias::new(partition_label))).equals((src_view, Alias::new("id"))),
            )
            .to_owned(),
    )
}
