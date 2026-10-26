//
// Copyright 2026 Tabsdata Inc.
//

//! The read verbs, written once over a [`Grain`]: the doors
//! ([`scd2`](crate::Queries::scd2), [`timeline`](crate::Queries::timeline),
//! [`for_update`](crate::Queries::for_update) and the wordless reads) pick the
//! grain, the verb picks the shape.

use crate::generic::chunked::Chunked;
use crate::generic::grain::{Grain, OneRow, select_in};
use crate::generic::tagged::TagFilter;
use crate::{QueryExecutor, Serves};
use sea_query::{Order, SelectStatement};
use ta_objects::errors::{SqlError, format_hint};
use ta_objects::sql_condition::SqlCondition;
use ta_objects::{ListDto, SelectDao, single_row};
use td_database::sea_query::builder::SeaQueryExt;
use td_objects::list::params::ListQueryParams;

pub struct Reader<E, G> {
    pub(crate) executor: E,
    pub(crate) grain: G,
}

impl<E, G> Reader<E, G> {
    /// The same grain over a slice that may exceed the bind-parameter limit.
    pub fn chunked(self) -> Chunked<E, G> {
        Chunked {
            executor: self.executor,
            grain: self.grain,
        }
    }

    pub async fn fetch_one<D: SelectDao>(
        self,
        condition: impl OneRow<D, G, E::Scope>,
    ) -> Result<D, SqlError>
    where
        G: Grain<D>,
        E: Serves<D>,
    {
        self.fetch_optional(condition)
            .await?
            .ok_or_else(SqlError::not_found::<D>)
    }

    /// `LIMIT 2`, so a second match is an error rather than a silent pick.
    pub async fn fetch_optional<D: SelectDao>(
        self,
        condition: impl OneRow<D, G, E::Scope>,
    ) -> Result<Option<D>, SqlError>
    where
        G: Grain<D>,
        E: Serves<D>,
    {
        let mut query = select_in::<D, G>(&self.grain, condition);
        query.limit(2);
        single_row::<D>(rows::<D>(query, self.executor).await?)
    }

    /// The first row in the DAO's default order, of however many match.
    pub async fn fetch_first<D: SelectDao>(
        self,
        condition: impl SqlCondition<D>,
    ) -> Result<Option<D>, SqlError>
    where
        G: Grain<D>,
        E: Serves<D>,
    {
        let mut query = select_in::<D, G>(&self.grain, condition);
        query.order_by_columns(D::order_by()).limit(1);
        optional::<D>(query, self.executor).await
    }

    /// The DAO's default order reversed, `LIMIT 1`.
    pub async fn fetch_last<D: SelectDao>(
        self,
        condition: impl SqlCondition<D>,
    ) -> Result<Option<D>, SqlError>
    where
        G: Grain<D>,
        E: Serves<D>,
    {
        let mut query = select_in::<D, G>(&self.grain, condition);
        query.order_by_columns(reverse_order::<D>()).limit(1);
        optional::<D>(query, self.executor).await
    }

    /// `offset = 0` is the most recent match.
    pub async fn fetch_nth_last<D: SelectDao>(
        self,
        condition: impl SqlCondition<D>,
        offset: u64,
    ) -> Result<Option<D>, SqlError>
    where
        G: Grain<D>,
        E: Serves<D>,
    {
        let mut query = select_in::<D, G>(&self.grain, condition);
        query.order_by_columns(D::order_by()).limit(1);
        if offset > 0 {
            query.offset(offset);
        }
        optional::<D>(query, self.executor).await
    }

    pub async fn fetch_all<D: SelectDao>(
        self,
        condition: impl SqlCondition<D>,
    ) -> Result<Vec<D>, SqlError>
    where
        G: Grain<D>,
        E: Serves<D>,
    {
        let mut query = select_in::<D, G>(&self.grain, condition);
        query.order_by_columns(D::order_by());
        rows::<D>(query, self.executor).await
    }

    /// A clean "could not find" error rather than the raw fetch error path.
    pub async fn assert_exists<D: SelectDao>(
        self,
        condition: impl SqlCondition<D>,
    ) -> Result<(), SqlError>
    where
        G: Grain<D>,
        E: Serves<D>,
    {
        let vals = condition.display_values();
        match self.fetch_first::<D>(condition).await? {
            Some(_) => Ok(()),
            None => Err(SqlError::CouldNotFindEntity(
                D::display_name().to_string(),
                hint(&vals),
            )),
        }
    }

    /// Fails with "already exists" rather than the unique-constraint
    /// violation a later write would raise.
    pub async fn assert_not_exists<D: SelectDao>(
        self,
        condition: impl SqlCondition<D>,
    ) -> Result<(), SqlError>
    where
        G: Grain<D>,
        E: Serves<D>,
    {
        let vals = condition.display_values();
        match self.fetch_first::<D>(condition).await? {
            Some(_) => Err(SqlError::EntityAlreadyExists(
                D::display_name().to_string(),
                hint(&vals),
            )),
            None => Ok(()),
        }
    }

    /// The list surface: a cross-project caller narrows by its viewable
    /// projects in `condition`. Bounded like every other read, so a `Global`
    /// handle cannot list a project-partitioned relation; that is
    /// `AcrossProjects` work.
    pub async fn list<T>(
        self,
        condition: impl SqlCondition<T::Dao>,
        params: &ListQueryParams<T>,
    ) -> Result<Vec<T>, SqlError>
    where
        T: ListDto,
        G: Grain<T::Dao>,
        E: Serves<T::Dao>,
        T::Tags: TagFilter<T::Dao, G, E>,
    {
        let mut base = select_in::<T::Dao, G>(&self.grain, condition);
        <T::Tags as TagFilter<T::Dao, G, E>>::restrict(&self.grain, &params.tags, &mut base);
        rows::<T::Dao>(params.apply_to(base), self.executor)
            .await?
            .into_iter()
            .map(|dao| T::from_dao(dao).map_err(SqlError::DtoConversionError))
            .collect()
    }
}

fn hint(vals: &[(String, String)]) -> String {
    let cols: Vec<String> = vals.iter().map(|(k, _)| k.clone()).collect();
    format_hint(&cols, vals)
}

pub(crate) fn reverse_order<D: SelectDao>() -> impl Iterator<Item = (D::Iden, Order)> {
    D::order_by().into_iter().map(|(col, ord)| {
        let flipped = match ord {
            Order::Asc => Order::Desc,
            Order::Desc => Order::Asc,
            other => other,
        };
        (col, flipped)
    })
}

pub(crate) async fn rows<D: SelectDao>(
    mut query: SelectStatement,
    executor: impl QueryExecutor,
) -> Result<Vec<D>, SqlError> {
    query
        .build_query_as()
        .fetch_all(executor)
        .await
        .map_err(SqlError::on_read::<D>)
}

pub(crate) async fn optional<D: SelectDao>(
    mut query: SelectStatement,
    executor: impl QueryExecutor,
) -> Result<Option<D>, SqlError> {
    query
        .build_query_as()
        .fetch_optional(executor)
        .await
        .map_err(SqlError::on_read::<D>)
}
