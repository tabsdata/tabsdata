//
// Copyright 2026 Tabsdata Inc.
//

//! Latest-at-time reads: the caller's condition composed with the SCD2
//! visibility predicate, so cost follows the condition's selectivity.

use crate::generic::chunked::AsOfChunked;
use crate::primitives::scd2::select_visible_at;
use crate::{QueryExecutor, Serves};
use ta_objects::errors::{SqlError, format_hint};
use ta_objects::sql_condition::SqlCondition;
use ta_objects::{AddressesOneVersion, Dao, ListDto, Scd2Dao, SelectDao, single_row};
use td_database::domain::Domained;
use td_database::sea_query::builder::SeaQueryExt;
use td_objects::list::params::ListQueryParams;

pub struct AsOf<'a, E, V> {
    pub(crate) on: &'a V,
    pub(crate) executor: E,
}

impl<'a, E, V> AsOf<'a, E, V> {
    pub fn chunked(self) -> AsOfChunked<'a, E, V> {
        AsOfChunked {
            on: self.on,
            executor: self.executor,
        }
    }

    pub async fn fetch_one<D: SelectDao + Scd2Dao<VersionOrder = V>>(
        self,
        condition: impl AddressesOneVersion<D>,
    ) -> Result<D, SqlError>
    where
        E: Serves<D>,
    {
        self.fetch_optional(condition)
            .await?
            .ok_or_else(SqlError::not_found::<D>)
    }

    pub async fn fetch_optional<D: SelectDao + Scd2Dao<VersionOrder = V>>(
        self,
        condition: impl AddressesOneVersion<D>,
    ) -> Result<Option<D>, SqlError>
    where
        E: Serves<D>,
    {
        let rows = select_visible_at::<D>(condition, self.on)
            .limit(2)
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<D>)?;
        single_row::<D>(rows)
    }

    /// The first visible row in the DAO's default order, of however many match.
    pub async fn fetch_first<D: SelectDao + Scd2Dao<VersionOrder = V>>(
        self,
        condition: impl SqlCondition<D>,
    ) -> Result<Option<D>, SqlError>
    where
        E: Serves<D>,
    {
        select_visible_at::<D>(condition, self.on)
            .order_by_columns(D::order_by())
            .limit(1)
            .build_query_as()
            .fetch_optional(self.executor)
            .await
            .map_err(SqlError::on_read::<D>)
    }

    pub async fn fetch_all<D: SelectDao + Scd2Dao<VersionOrder = V>>(
        self,
        condition: impl SqlCondition<D>,
    ) -> Result<Vec<D>, SqlError>
    where
        E: Serves<D>,
    {
        select_visible_at::<D>(condition, self.on)
            .order_by_columns(D::order_by())
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<D>)
    }

    pub async fn assert_not_exists<D: SelectDao + Scd2Dao<VersionOrder = V>>(
        self,
        condition: impl SqlCondition<D>,
    ) -> Result<(), SqlError>
    where
        E: Serves<D>,
    {
        let vals = condition.display_values();
        match self.fetch_first::<D>(condition).await? {
            Some(_) => {
                let cols: Vec<String> = vals.iter().map(|(k, _)| k.clone()).collect();
                Err(SqlError::EntityAlreadyExists(
                    D::display_name().to_string(),
                    format_hint(&cols, &vals),
                ))
            }
            None => Ok(()),
        }
    }

    /// The cross-project surface: a project-blind caller narrows by its
    /// viewable projects in `condition`, so this asks only for the domain.
    pub async fn list<T>(
        self,
        condition: impl SqlCondition<T::Dao>,
        params: &ListQueryParams<T>,
    ) -> Result<Vec<T>, SqlError>
    where
        T: ListDto,
        T::Dao: Scd2Dao<VersionOrder = V>,
        E: QueryExecutor + Domained<Domain = <T::Dao as Dao>::Db>,
    {
        let daos: Vec<T::Dao> = params
            .apply_to(select_visible_at::<T::Dao>(condition, self.on))
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<T::Dao>)?;
        daos.into_iter()
            .map(|dao| T::from_dao(dao).map_err(SqlError::DtoConversionError))
            .collect()
    }
}
