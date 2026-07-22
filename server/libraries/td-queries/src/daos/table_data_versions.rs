//
// Copyright 2026 Tabsdata Inc.
//

//! Table-data-version queries.
//!
//! - [`Ltdv`] — LTDV-rooted handle. Provides:
//!   - [`pointers`](Ltdv::pointers): constructor for the LTDV pointer
//!     event log.
//!   - [`latest_bound_tdv_with_data_for_table`](Ltdv::latest_bound_tdv_with_data_for_table):
//!     the canonical "what's the data PTDV for this table at time T"
//!     lookup — a single SQL that joins LTDV → its current `Bound`
//!     pointer event → PTDV, LTDV-rooted in filter and ordering.

use crate::QueryExecutor;
use crate::daos::pointers::{LogicalPointers, ProducerLfr};
use sea_query::{
    Asterisk, Expr, ExprTrait, Iden, JoinType, Order, Query, SimpleExpr, SubQueryStatement,
};
use ta_objects::DaoCore;
use ta_objects::SeaValue;
use ta_objects::errors::SqlError;
use td_database::sea_query::builder::SeaQueryExt;
use td_objects::dxo::logical::{
    LogicalFunctionRunDBIden, LogicalTableDataVersionDBIden, LogicalTableDataVersionPointerDB,
    LogicalTableDataVersionPointerDBIden,
};
use td_objects::dxo::physical_table_data_version::{
    PhysicalTableDataVersionDBIden, PhysicalTableDataVersionDBWithNames,
    PhysicalTableDataVersionDBWithNamesIden,
};
use td_objects::types::basic::{
    HasData, LogicalFunctionRunStatus, LogicalPointerStatus, TableId, TriggeredOn,
};

query_handle! {
    /// Entry trait providing [`ltdv`](LtdvEntry::ltdv) on every
    /// [`QueryExecutor`](crate::QueryExecutor). Returns an [`Ltdv`] handle.
    pub trait LtdvEntry {
        fn ltdv(self) -> Ltdv<Self> {
            Ltdv { executor: self }
        }
    }
}

/// LTDV-rooted handle. Reach via [`LtdvEntry::ltdv`]:
///
/// ```ignore
/// conn.ltdv().pointers().at(&at).concrete(&ltdv_id).await?;
/// conn.ltdv().latest_bound_tdv_with_data_for_table(&table_id, &triggered_on, true).await?;
/// ```
pub struct Ltdv<E> {
    pub(crate) executor: E,
}

impl<E> Ltdv<E>
where
    E: QueryExecutor,
{
    /// Pointer event log for LTDVs (`logical_table_data_version_pointers`).
    pub fn pointers(self) -> LogicalPointers<E, LogicalTableDataVersionPointerDB> {
        LogicalPointers::new(self.executor)
    }

    /// Latest with-data PTDV for `table_id` as of `triggered_on`.
    ///
    /// LTDV-rooted: ranges over LTDVs with `triggered_on <= ?` (replay-
    /// stable since replays don't move LTDV.triggered_on), then resolves
    /// each LTDV's *current* `Bound` pointer (no `set_on` upper bound,
    /// so post-`triggered_on` rebinds are reflected). The `has_data =
    /// TRUE` + `ORDER BY ltdv.triggered_on DESC LIMIT 1` clause gives
    /// chase-back for free — a current PTDV without data is dropped and
    /// the next-newest LTDV wins.
    ///
    /// When `committed_only`, also LEFT-JOINs the producer LFR and
    /// keeps rows whose producer is sticky-`Committed` *or* NULL
    /// (V1-born lazy-mint over a pre-V2 PTDV is treated as committed).
    pub async fn latest_bound_tdv_with_data_for_table(
        self,
        table_id: &TableId,
        triggered_on: &TriggeredOn,
        committed_only: bool,
    ) -> Result<Option<PhysicalTableDataVersionDBWithNames>, SqlError> {
        let current_bound_pointer_id = Query::select()
            .column((LpInner, LogicalTableDataVersionPointerDBIden::Id))
            .from_as(LogicalTableDataVersionPointerDBIden::Table, LpInner)
            .and_where(
                Expr::col((
                    LpInner,
                    LogicalTableDataVersionPointerDBIden::LogicalTableDataVersionId,
                ))
                .equals((
                    LogicalTableDataVersionPointerDBIden::Table,
                    LogicalTableDataVersionPointerDBIden::LogicalTableDataVersionId,
                )),
            )
            .and_where(
                Expr::col((LpInner, LogicalTableDataVersionPointerDBIden::Status))
                    .eq(LogicalPointerStatus::Bound.as_sea_value()),
            )
            .order_by(
                (LpInner, LogicalTableDataVersionPointerDBIden::SetOn),
                Order::Desc,
            )
            .order_by(
                (LpInner, LogicalTableDataVersionPointerDBIden::Id),
                Order::Desc,
            )
            .limit(1)
            .to_owned();

        // Qualify with the view's table — the joined LTDV / pointer
        // tables also carry an `id` column, and Postgres rejects the
        // unqualified ref as ambiguous (SQLite tolerates it).
        let mut q = Query::select();
        q.expr(Expr::col((
            PhysicalTableDataVersionDBWithNamesIden::Table,
            Asterisk,
        )))
        .from(PhysicalTableDataVersionDBWithNames::table())
        .join(
            JoinType::InnerJoin,
            LogicalTableDataVersionPointerDBIden::Table,
            Expr::col((
                LogicalTableDataVersionPointerDBIden::Table,
                LogicalTableDataVersionPointerDBIden::PhysicalTableDataVersionId,
            ))
            .equals((
                PhysicalTableDataVersionDBWithNamesIden::Table,
                PhysicalTableDataVersionDBIden::Id,
            )),
        )
        .join(
            JoinType::InnerJoin,
            LogicalTableDataVersionDBIden::Table,
            Expr::col((
                LogicalTableDataVersionDBIden::Table,
                LogicalTableDataVersionDBIden::Id,
            ))
            .equals((
                LogicalTableDataVersionPointerDBIden::Table,
                LogicalTableDataVersionPointerDBIden::LogicalTableDataVersionId,
            )),
        )
        .and_where(
            Expr::col((
                LogicalTableDataVersionPointerDBIden::Table,
                LogicalTableDataVersionPointerDBIden::Id,
            ))
            .eq(Expr::SubQuery(
                None,
                Box::new(SubQueryStatement::SelectStatement(current_bound_pointer_id)),
            )),
        )
        .and_where(
            Expr::col((
                LogicalTableDataVersionDBIden::Table,
                LogicalTableDataVersionDBIden::TableId,
            ))
            .eq(table_id.as_sea_value()),
        )
        .and_where(
            Expr::col((
                LogicalTableDataVersionDBIden::Table,
                LogicalTableDataVersionDBIden::TriggeredOn,
            ))
            .lte(triggered_on.as_sea_value()),
        )
        .and_where(
            Expr::col((
                PhysicalTableDataVersionDBWithNamesIden::Table,
                PhysicalTableDataVersionDBIden::HasData,
            ))
            .eq(HasData::TRUE.as_sea_value()),
        )
        .order_by(
            (
                LogicalTableDataVersionDBIden::Table,
                LogicalTableDataVersionDBIden::TriggeredOn,
            ),
            Order::Desc,
        )
        .order_by(
            (
                LogicalTableDataVersionDBIden::Table,
                LogicalTableDataVersionDBIden::Id,
            ),
            Order::Desc,
        )
        .limit(1);

        if committed_only {
            q.join_as(
                JoinType::LeftJoin,
                LogicalFunctionRunDBIden::Table,
                ProducerLfr,
                Expr::col((
                    LogicalTableDataVersionDBIden::Table,
                    LogicalTableDataVersionDBIden::ProducerLogicalFunctionRunId,
                ))
                .equals((ProducerLfr, LogicalFunctionRunDBIden::Id)),
            )
            .and_where(producer_status_in_or_lazy(&[
                LogicalFunctionRunStatus::Committed,
            ]));
        }

        let row: Option<PhysicalTableDataVersionDBWithNames> = q
            .build_query_as()
            .fetch_optional(self.executor)
            .await
            .map_err(SqlError::on_read::<PhysicalTableDataVersionDBWithNames>)?;
        Ok(row)
    }
}

/// Self-correlated LTDV-pointer alias used inside the "latest Bound
/// event per LTDV" subquery. Lets the inner pointer reference correlate
/// against the outer pointer table without ambiguous column refs.
#[derive(Iden, Clone, Copy)]
struct LpInner;

/// True when the producer LFR's status is one of `statuses`, or when
/// the producer is NULL (lazy-mint over a pre-V2 concrete TDV is
/// treated as terminal). Requires the caller to have joined the
/// producer LFR aliased as [`ProducerLfr`].
fn producer_status_in_or_lazy(statuses: &[LogicalFunctionRunStatus]) -> SimpleExpr {
    let values: Vec<_> = statuses.iter().map(|s| s.as_sea_value()).collect();
    Expr::col((ProducerLfr, LogicalFunctionRunDBIden::Id))
        .is_null()
        .or(Expr::col((ProducerLfr, LogicalFunctionRunDBIden::Status)).is_in(values))
}
