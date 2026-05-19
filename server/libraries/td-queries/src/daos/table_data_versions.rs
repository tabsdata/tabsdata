//
// Copyright 2026 Tabs Data Inc.
//

//! Table-data-version queries.
//!
//! - [`Ltdv`] — LTDV identity-table operations, plus a `.pointers()`
//!   constructor for the LTDV pointer event log and the LTDV-rooted
//!   pointer-walk read [`Ltdv::latest_bound_tdv_with_data_for_table`].

use crate::QueryExecutor;
use crate::daos::pointers::{LogicalPointers, ptdv_is_bound, ptdv_is_canonical_committed};
use sea_query::{Expr, ExprTrait, Order, Query};
use ta_objects::SeaValue;
use ta_objects::errors::SqlError;
use ta_objects::{Dao, DaoCore};
use td_database::sea_query::builder::SeaQueryExt;
use td_objects::dxo::logical::LogicalTableDataVersionPointerDB;
use td_objects::dxo::physical_table_data_version::{
    PhysicalTableDataVersionDBIden, PhysicalTableDataVersionDBWithNames,
};
use td_objects::types::basic::{HasData, TableId, TriggeredOn};

query_handle! {
    /// Entry trait providing [`ltdv`](LtdvEntry::ltdv) on every
    /// [`QueryExecutor`](crate::QueryExecutor). Returns an [`Ltdv`] handle.
    pub trait LtdvEntry {
        fn ltdv(self) -> Ltdv<Self> {
            Ltdv { executor: self }
        }
    }
}

/// LTDV-rooted queries. Reach via [`LtdvEntry::ltdv`]:
///
/// ```ignore
/// conn.ltdv().pointers().concrete_at(&ltdv_id, &at).await?;
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

    /// Latest with-data PTDV row for `table_id` whose triggered_on is
    /// at or before `triggered_on`, restricted to PTDVs currently
    /// `Bound` under their LTDV pointer. When `committed_only` is true,
    /// also requires the producer LFR to be sticky-`Committed` (the
    /// canonical "published" projection — see
    /// [`ptdv_is_canonical_committed`]).
    pub async fn latest_bound_tdv_with_data_for_table(
        self,
        table_id: &TableId,
        triggered_on: &TriggeredOn,
        committed_only: bool,
    ) -> Result<Option<PhysicalTableDataVersionDBWithNames>, SqlError> {
        let bound_filter = if committed_only {
            ptdv_is_canonical_committed()
        } else {
            ptdv_is_bound()
        };
        let row: Option<PhysicalTableDataVersionDBWithNames> = Query::select()
            .columns(PhysicalTableDataVersionDBWithNames::columns())
            .from(PhysicalTableDataVersionDBWithNames::table())
            .and_where(
                Expr::col(PhysicalTableDataVersionDBIden::TableId).eq(table_id.as_sea_value()),
            )
            .and_where(
                Expr::col(PhysicalTableDataVersionDBIden::HasData)
                    .eq(HasData::TRUE.as_sea_value()),
            )
            .and_where(
                Expr::col(PhysicalTableDataVersionDBIden::TriggeredOn)
                    .lte(triggered_on.as_sea_value()),
            )
            .and_where(bound_filter)
            .order_by(PhysicalTableDataVersionDBIden::TriggeredOn, Order::Desc)
            .limit(1)
            .build_query_as()
            .fetch_optional(self.executor)
            .await
            .map_err(SqlError::on_read::<PhysicalTableDataVersionDBWithNames>)?;
        Ok(row)
    }
}
