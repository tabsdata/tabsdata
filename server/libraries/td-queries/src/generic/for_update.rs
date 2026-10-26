//
// Copyright 2026 Tabsdata Inc.
//

//! The row-lock door: [`Reader`] over unversioned rows, `FOR UPDATE` until the
//! transaction ends.

use crate::Serves;
use crate::generic::grain::{Locked, Unversioned, select_in};
use crate::generic::reader::{Reader, rows};
use crate::primitives::table::claim;
use ta_objects::errors::SqlError;
use ta_objects::sql_condition::SqlCondition;
use ta_objects::{SelectDao, TableDao, UnversionedDao};

impl<E> Reader<E, Locked> {
    /// `SKIP LOCKED`: rows another transaction holds are passed over, not
    /// waited on.
    pub async fn claim_all<D: SelectDao + UnversionedDao + TableDao>(
        self,
        condition: impl SqlCondition<D>,
    ) -> Result<Vec<D>, SqlError>
    where
        E: Serves<D>,
    {
        let mut query = select_in::<D, Unversioned>(&Unversioned, condition);
        query.order_by_columns(D::order_by());
        claim::<D>(&mut query);
        rows::<D>(query, self.executor).await
    }
}
