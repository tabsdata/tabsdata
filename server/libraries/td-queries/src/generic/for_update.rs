//
// Copyright 2026 Tabsdata Inc.
//

//! The unversioned reads, holding `FOR UPDATE` row locks until the
//! transaction ends.

use crate::Serves;
use crate::primitives::table::{select_claim, select_for_update};
use ta_objects::errors::SqlError;
use ta_objects::sql_condition::SqlCondition;
use ta_objects::{AddressesOneRow, SelectDao, UnversionedDao, single_row};
use td_database::sea_query::builder::SeaQueryExt;

pub struct ForUpdate<E> {
    pub(crate) executor: E,
}

impl<E> ForUpdate<E> {
    pub async fn fetch_one<D: SelectDao + UnversionedDao>(
        self,
        condition: impl AddressesOneRow<D, E::Scope>,
    ) -> Result<D, SqlError>
    where
        E: Serves<D>,
    {
        self.fetch_optional(condition)
            .await?
            .ok_or_else(SqlError::not_found::<D>)
    }

    pub async fn fetch_optional<D: SelectDao + UnversionedDao>(
        self,
        condition: impl AddressesOneRow<D, E::Scope>,
    ) -> Result<Option<D>, SqlError>
    where
        E: Serves<D>,
    {
        let rows = select_for_update::<D>(condition)
            .limit(2)
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<D>)?;
        single_row::<D>(rows)
    }

    pub async fn fetch_all<D: SelectDao + UnversionedDao>(
        self,
        condition: impl SqlCondition<D>,
    ) -> Result<Vec<D>, SqlError>
    where
        E: Serves<D>,
    {
        select_for_update::<D>(condition)
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<D>)
    }

    /// `SKIP LOCKED`: rows another transaction holds are passed over, not
    /// waited on.
    pub async fn claim_all<D: SelectDao + UnversionedDao>(
        self,
        condition: impl SqlCondition<D>,
    ) -> Result<Vec<D>, SqlError>
    where
        E: Serves<D>,
    {
        select_claim::<D>(condition)
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<D>)
    }
}
