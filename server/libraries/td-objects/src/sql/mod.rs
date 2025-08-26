//
// Copyright 2025 Tabs Data Inc.
//

// IMPL NOTES:
//
// * The `Queries` classes of function/dependency/table/trigger are a placeholder
//   in preparation for SQL generation for different DBs.
//
// *  the `Queries` classes of function/dependency/table/trigger modules could be
//    refactored into a Generic component providing that SQL building for the
//    'current' and 'at_time' methods.
pub mod cte;
pub mod list;
pub mod recursive;

use crate::sql::cte::LATEST_VERSIONS_CTE;
use crate::sql::cte::{ranked_versions_at, select_ranked_versions_at};
use crate::sql::list::{ListError, ListQueryParams, Order, Pagination};
use crate::types::{DataAccessObject, ListQuery, PartitionBy, SqlEntity, VersionedAt};
use async_trait::async_trait;
use std::ops::Deref;
use td_error::td_error;
use tracing::trace;

#[td_error]
pub enum QueryError {
    #[error("List query error: {0:?}")]
    ListQueryError(#[from] ListError) = 0,

    #[error("Type [{0:?}] not found in [{1:?}] ")]
    TypeNotFound(String, String) = 5000,
}

/// Struct holding the Queries.
pub struct DaoQueries(Box<dyn Queries + Send + Sync>);

impl DaoQueries {
    pub fn new(queries: Box<dyn Queries + Send + Sync>) -> Self {
        Self(queries)
    }
}

impl Default for DaoQueries {
    fn default() -> Self {
        Self(Box::new(GenericQueries))
    }
}

impl Deref for DaoQueries {
    type Target = dyn Queries;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

/// Generic queries generation struct.
pub struct GenericQueries;
impl Queries for GenericQueries {}

// Queries<DB: sqlx::Database> we can do this to generalize the queries.
// Or we could also just have sqliteQueries, mysqlQueries, etc. And use DaoQueries dyn.
pub trait Queries {}

pub trait Insert<'a> {
    fn insert<D: DataAccessObject>(
        &self,
        dao: &'a D,
    ) -> Result<sqlx::QueryBuilder<'a, sqlx::Sqlite>, QueryError>;
}

impl<'a, Q> Insert<'a> for Q
where
    Q: Deref<Target = dyn Queries>,
{
    fn insert<D: DataAccessObject>(
        &self,
        dao: &'a D,
    ) -> Result<sqlx::QueryBuilder<'a, sqlx::Sqlite>, QueryError> {
        let table = D::sql_table();
        let fields = D::fields();
        let sql = format!("INSERT INTO {} ({}) ", table, fields.join(", "));

        let query_builder = dao.values_query_builder(sql, fields);

        trace!("insert_{}: sql: {}", table, query_builder.sql());
        Ok(query_builder)
    }
}

/// Macro to generate the WHERE clause for the query functions.
/// The macro is recursive and uses a muncher to process the input.
/// The input is a list of columns to filter by. The columns can be single values or arrays.
/// The arrays are used to create OR groups. For example, ([E1, E2]) will generate a E1 OR E2 clause.
/// The macro will generate a WHERE clause with AND groups for the single values and OR groups for the arrays.
/// For example, ([E1, E2], E3) will generate (E1 OR E2) AND E3.
/// It returns whether the WHERE clause was empty or not, so subsequent WHERE clauses can be added.
#[macro_export]
macro_rules! gen_where_clause {
    // Cases for when there's no condition to add (empty input).
    ($query_builder:expr_2021, $D:ty, ) => { true };
    ($query_builder:expr_2021, $D:ty, $vect:ident: []) => { true };

    // Case for when there's at least one condition.
    ($query_builder:expr_2021, $D:ty, $($rest:tt)+) => {{
        $query_builder.push(" WHERE ");
        let mut first = true;
        gen_where_clause!(@munch $query_builder, $D, first, $($rest)+);
        first
    }};

    // Binding
    (@bind $query_builder:expr_2021, $D:ty, $E:ident) => {{
        let column = <$D>::sql_field_for_type($E.type_name())
            .ok_or(QueryError::TypeNotFound(
                std::any::type_name::<$E>().to_string(),
                <$D>::sql_table().to_string(),
            ))?;
        $E.push_bind($query_builder.push(format!("{} = ", column)));
    }};

    // Base case: nothing to do here
    (@munch $query_builder:expr_2021, $D:ty, $first:ident) => {};

    // Single identifier (normal case). AND group.
    (@munch $query_builder:expr_2021, $D:ty, $first:ident, $E:ident $(, $($rest:tt)*)?) => {{
        if !$first { $query_builder.push(" AND "); }
        $first = false;
        gen_where_clause!(@bind $query_builder, $D, $E);
        gen_where_clause!(@munch $query_builder, $D, $first $(, $($rest)*)?);
    }};

    // Case for an empty array (no expansion needed)
    (@munch $query_builder:expr_2021, $first:ident, []) => {};

    // AND/OR group. Joining arrays.
    (@munch $query_builder:expr_2021, $D:ty, $first:ident, $vect:ident: [ $($inner:ident),* ] $(, $($rest:tt)*)?) => {{
        if !$first { $query_builder.push(" AND "); }
        $first = false;

        let mut or_first = true;
        for ($($inner),*) in $vect.iter() {
            if !or_first { $query_builder.push(" OR "); }
            or_first = false;

            $query_builder.push("(");
            let mut and_first = true;
            $(
                if !and_first { $query_builder.push(" AND "); }
                and_first = false;
                gen_where_clause!(@bind $query_builder, $D, $inner);
            )*
            $query_builder.push(")");
        }
        gen_where_clause!(@munch $query_builder, $D, $first $(, $($rest)*)?);
    }};
}

pub trait SelectBy<'a, E> {
    fn select_by<D: DataAccessObject>(
        &self,
        e: &'a E,
    ) -> Result<sqlx::QueryBuilder<'a, sqlx::Sqlite>, QueryError>;
}

macro_rules! impl_select_by {
    (
        [$($E:ident),*]
    ) => {
        #[allow(non_snake_case, unused_parens, unused_variables, unused_mut, unused_assignments)]
        impl<'a, Q, $($E),*> SelectBy<'a, ($(&'a $E),*)> for Q
        where
            Q: Deref<Target = dyn Queries>,
            $($E: SqlEntity),*
        {
            fn select_by<D: DataAccessObject>(&self, ($($E),*): &'a ($(&'a $E),*)) -> Result<sqlx::QueryBuilder<'a, sqlx::Sqlite>, QueryError> {
                let table = D::sql_table();
                let fields = D::fields();
                let sql = format!("SELECT {} FROM {}", fields.join(", "), table);
                let mut query_builder = sqlx::QueryBuilder::new(sql);
                gen_where_clause!(query_builder, D, $($E),*);
                query_builder.push(" ");
                query_builder.push(D::order_by());
                trace!("select_{}: sql: {}", table, query_builder.sql());
                Ok(query_builder)
            }
        }
    };
}

all_the_tuples!(impl_select_by);

pub trait FindBy<'a, E> {
    fn find_by<D: DataAccessObject>(
        &self,
        e: &'a [E],
    ) -> Result<sqlx::QueryBuilder<'a, sqlx::Sqlite>, QueryError>;
}

macro_rules! impl_find_by {
    (
        [$($E:ident),*]
    ) => {
        #[allow(non_snake_case, unused_parens, unused_variables, unused_mut, unused_assignments)]
        impl<'a, Q, $($E),*> FindBy<'a, ($(&'a $E),*)> for Q
        where
            Q: Deref<Target = dyn Queries>,
            $($E: SqlEntity),*
        {
            fn find_by<D: DataAccessObject>(&self, e: &'a [ ($(&'a $E),*) ]) -> Result<sqlx::QueryBuilder<'a, sqlx::Sqlite>, QueryError> {
                let table = D::sql_table();
                let fields = D::fields();
                let sql = format!("SELECT {} FROM {}", fields.join(", "), table);
                let mut query_builder = sqlx::QueryBuilder::new(sql);
                if e.is_empty() {
                    // Safeguard so empty lookups don't find all rows
                    query_builder.push(" WHERE 1 = 0");
                } else {
                    gen_where_clause!(query_builder, D, e: [ $($E),* ]);
                }
                trace!("find_by_{}: sql: {}", table, query_builder.sql());
                Ok(query_builder)
            }
        }
    };
}

all_the_tuples!(impl_find_by);

#[async_trait]
pub trait ListFilterGenerator: Send + Sync {
    async fn where_clause<'a, D: DataAccessObject>(
        &'a self,
        first: bool,
        query_builder: &mut sqlx::QueryBuilder<'a, sqlx::Sqlite>,
    ) -> Result<bool, QueryError>;
}

pub type NoListFilter = ();

#[async_trait]
impl ListFilterGenerator for NoListFilter {
    async fn where_clause<'a, D: DataAccessObject>(
        &'a self,
        first: bool,
        _query_builder: &mut sqlx::QueryBuilder<'a, sqlx::Sqlite>,
    ) -> Result<bool, QueryError> {
        Ok(first)
    }
}

#[async_trait]
pub trait ListBy<'a, E> {
    async fn list_by<T, F>(
        &self,
        list_query_params: &'a ListQueryParams<T>,
        list_filter_generator: &'a F,
        e: &'a E,
    ) -> Result<sqlx::QueryBuilder<'a, sqlx::Sqlite>, QueryError>
    where
        T: ListQuery + 'a,
        F: ListFilterGenerator + 'a;

    async fn list_by_at<T, F>(
        &self,
        list_query_params: &'a ListQueryParams<T>,
        natural_order_by: Option<&'a <<T as ListQuery>::Dao as VersionedAt>::Order>,
        condition: Option<&'a [&'a <<T as ListQuery>::Dao as VersionedAt>::Condition]>,
        list_filter_generator: &'a F,
        e: &'a E,
    ) -> Result<sqlx::QueryBuilder<'a, sqlx::Sqlite>, QueryError>
    where
        T: ListQuery + 'a,
        F: ListFilterGenerator + 'a,
        T::Dao: VersionedAt;

    async fn list_versions_by_at<T, F>(
        &self,
        list_query_params: &'a ListQueryParams<T>,
        natural_order_by: Option<&'a <<T as ListQuery>::Dao as VersionedAt>::Order>,
        status: Option<&'a [&'a <<T as ListQuery>::Dao as VersionedAt>::Condition]>,
        list_filter_generator: &'a F,
        e: &'a E,
    ) -> Result<sqlx::QueryBuilder<'a, sqlx::Sqlite>, QueryError>
    where
        T: ListQuery + 'a,
        F: ListFilterGenerator + 'a,
        T::Dao: PartitionBy + VersionedAt;
}

// TODO remove duplicate code and tests for versions
macro_rules! impl_list_by {
    (
        [$($E:ident),*]
    ) => {
        #[async_trait]
        #[allow(non_snake_case, unused_parens, unused_variables, unused_mut, unused_assignments)]
        impl<'a, Q, $($E),*> ListBy<'a, ($(&'a $E),*)> for Q
        where
            Q: Deref<Target = dyn Queries> + Send + Sync,
            $($E: SqlEntity),*
        {
            async fn list_by<T, F>(
                &self,
                query_params: &'a ListQueryParams<T>,
                list_filter_generator: &'a F,
                ($($E),*): &'a ($(&'a $E),*),
            ) -> Result<sqlx::QueryBuilder<'a, sqlx::Sqlite>, QueryError>
            where
                T: ListQuery + 'a,
                F: ListFilterGenerator + 'a,
            {
                let table = T::list_on();
                let fields = T::fields();
                let sql = format!("SELECT {} FROM {}", fields.join(", "), table);
                let mut query_builder = sqlx::QueryBuilder::new(sql);

                let mut first = gen_where_clause!(query_builder, T::Dao, $($E),*);
                first = list_filter_generator.where_clause::<T::Dao>(first, &mut query_builder).await?;
                first = query_params_where(first, query_params, &mut query_builder);

                trace!("list_{}: sql: {}", table, query_builder.sql());
                Ok(query_builder)
            }

            async fn list_by_at<T, F>(
                &self,
                query_params: &'a ListQueryParams<T>,
                natural_order_by: Option<&'a <<T as ListQuery>::Dao as VersionedAt>::Order>,
                status: Option<&'a [&'a <<T as ListQuery>::Dao as VersionedAt>::Condition]>,
                list_filter_generator: &'a F,
                ($($E),*): &'a ($(&'a $E),*),
            ) -> Result<sqlx::QueryBuilder<'a, sqlx::Sqlite>, QueryError>
            where
                T: ListQuery + 'a,
                T::Dao: VersionedAt,
                F: ListFilterGenerator + 'a,
            {
                let table = T::list_on();
                let fields = T::fields();
                let sql = format!("SELECT {} FROM {}", fields.join(", "), table);
                let mut query_builder = sqlx::QueryBuilder::new(sql);

                let mut first = gen_where_clause!(query_builder, T::Dao, $($E),*);

                if let Some(natural_order_by) = natural_order_by {
                    if first {
                        query_builder.push(" WHERE ");
                    } else {
                        query_builder.push(" AND ");
                    }
                    first = false;

                    query_builder.push(format!(
                        "{} <= ",
                        <T::Dao as VersionedAt>::order_by()
                    ));
                    natural_order_by.push_bind(&mut query_builder);
                }

                if let Some(status) = status {
                    if first {
                        query_builder.push(" WHERE ");
                    } else {
                        query_builder.push(" AND ");
                    }
                    first = false;

                    query_builder.push("(");
                    let mut separated = query_builder.separated(" OR ");
                    for status in status {
                        separated.push(format!("{} = ", T::Dao::condition_by()));
                        status.push_bind_unseparated(&mut separated);
                    }
                    query_builder.push(")");
                }

                first = list_filter_generator.where_clause::<T::Dao>(first, &mut query_builder).await?;
                first = query_params_where(first, &query_params, &mut query_builder);

                trace!("list_at_{}: sql: {}", table, query_builder.sql());
                Ok(query_builder)
            }

            async fn list_versions_by_at<T, F>(
                &self,
                query_params: &'a ListQueryParams<T>,
                natural_order_by: Option<&'a <<T as ListQuery>::Dao as VersionedAt>::Order>,
                status: Option<&'a [&'a <<T as ListQuery>::Dao as VersionedAt>::Condition]>,
                list_filter_generator: &'a F,
                ($($E),*): &'a ($(&'a $E),*),
            ) -> Result<sqlx::QueryBuilder<'a, sqlx::Sqlite>, QueryError>
            where
                T: ListQuery + 'a,
                T::Dao: PartitionBy + VersionedAt,
                F: ListFilterGenerator + 'a,
            {
                let mut query_builder = sqlx::QueryBuilder::default();

                // Build CTEs to find needed data (note natural order is needed to find the latest version, before listing)
                query_builder.push("WITH ");
                ranked_versions_at::<T::Dao>(LATEST_VERSIONS_CTE, &mut query_builder, natural_order_by);
                select_ranked_versions_at::<T::Dao>(LATEST_VERSIONS_CTE, &mut query_builder, status);


                let fields = T::fields();
                let select = format!("SELECT {} FROM {}", fields.join(", "), LATEST_VERSIONS_CTE);
                query_builder.push(select);

                let mut first = gen_where_clause!(query_builder, T::Dao, $($E),*);
                first = list_filter_generator.where_clause::<T::Dao>(first, &mut query_builder).await?;
                first = query_params_where(first, &query_params, &mut query_builder);

                trace!("list_versions_at_{}: sql: {}", T::list_on(), query_builder.sql());
                Ok(query_builder)
            }
        }
    };
}

fn query_params_where<'a, T>(
    first: bool,
    query_params: &'a ListQueryParams<T>,
    query_builder: &mut sqlx::QueryBuilder<'a, sqlx::Sqlite>,
) -> bool
where
    T: ListQuery,
{
    let mut first = first;
    query_params
        .conditions() // And
        .conditions() // Or
        .iter()
        .for_each(|or| {
            if first {
                query_builder.push(" WHERE ");
            } else {
                query_builder.push(" AND ");
            }
            first = false;

            query_builder.push("(");
            let mut or_separated = query_builder.separated(" OR ");
            for cond in or.conditions() {
                // no SQL injection here, as the values are bound to the fields of the struct
                or_separated.push(format!("{} {} ", cond.field(), cond.operator()));
                let mut value = cond.values();

                match cond.cardinality() {
                    1 => {
                        let value = value.pop().unwrap();
                        value.push_bind_unseparated(&mut or_separated);
                        if !cond.connector().is_empty() {
                            let x = format!(" {} ", cond.connector());
                            or_separated.push_unseparated(x);
                        }
                    }
                    2 => {
                        let max = value.pop().unwrap();
                        let min = value.pop().unwrap();
                        min.push_bind_unseparated(&mut or_separated);
                        or_separated.push_unseparated(format!(" {} ", cond.connector()));
                        max.push_bind_unseparated(&mut or_separated);
                    }
                    _ => {}
                }
            }
            query_builder.push(")");
        });

    let mut order = query_params.order().clone();
    let mut natural_order = query_params.natural_order().clone();
    if let Some(pagination) = query_params.pagination() {
        if first {
            query_builder.push(" WHERE ");
        } else {
            query_builder.push(" AND ");
        }
        first = false;

        query_builder.push("(");

        let pagination_field = query_params
            .order()
            .as_ref()
            .unwrap_or(query_params.natural_order());
        let range_operator = match (pagination_field, pagination) {
            (Order::Asc(_), Pagination::Previous(_, _)) => "<",
            (Order::Asc(_), Pagination::Next(_, _)) => ">",
            (Order::Desc(_), Pagination::Previous(_, _)) => ">",
            (Order::Desc(_), Pagination::Next(_, _)) => "<",
        };

        if matches!(pagination, Pagination::Previous(_, _)) {
            natural_order = natural_order.invert();
            order = order.map(|o| o.invert());
        }

        // field OP value
        query_builder.push(format!(
            "{} {} ",
            T::map_dao_field(pagination_field.field()),
            range_operator
        ));
        pagination.column_value().push_bind(query_builder);

        query_builder.push(" OR ");
        query_builder.push("(");

        // field = value
        query_builder.push(format!("{} = ", T::map_dao_field(pagination_field.field()),));
        pagination.column_value().push_bind(query_builder);

        // natural_field OP value
        query_builder.push(format!(
            " AND {} {} ",
            T::map_dao_field(natural_order.field()),
            range_operator
        ));
        pagination.pagination_id().push_bind(query_builder);
        query_builder.push(")");

        query_builder.push(")");
    }

    query_builder.push(" ORDER BY ");
    let mut separated = query_builder.separated(", ");

    if let Some(order) = order {
        separated.push(format!(
            "{} {}",
            T::map_dao_field(order.field()),
            order.direction()
        ));
    }

    separated.push(format!(
        "{} {}",
        T::map_dao_field(natural_order.field()),
        natural_order.direction()
    ));

    query_builder
        .push(" LIMIT ")
        .push_bind(*query_params.len() as i64);

    first
}

all_the_tuples!(impl_list_by);

// D is needed to get the fields types for the WHERE clauses
pub trait UpdateBy<'a, E> {
    fn update_by<U: DataAccessObject, D: DataAccessObject>(
        &self,
        dao: &'a U,
        e: &'a E,
    ) -> Result<sqlx::QueryBuilder<'a, sqlx::Sqlite>, QueryError>;

    fn update_all_by<U: DataAccessObject, D: DataAccessObject>(
        &self,
        dao: &'a U,
        e: &'a [E],
    ) -> Result<sqlx::QueryBuilder<'a, sqlx::Sqlite>, QueryError>;
}

macro_rules! impl_update_by {
    (
        [$($E:ident),*]
    ) => {
        #[allow(non_snake_case, unused_parens, unused_variables, unused_mut, unused_assignments)]
        impl<'a, Q, $($E),*> UpdateBy<'a, ($(&'a $E),*)> for Q
        where
            Q: Deref<Target = dyn Queries>,
            $($E: SqlEntity),*
        {
            fn update_by<U: DataAccessObject, D: DataAccessObject>(&self, dao: &'a U, ($($E),*): &'a ($(&'a $E),*)) -> Result<sqlx::QueryBuilder<'a, sqlx::Sqlite>, QueryError> {
                let table = U::sql_table();
                let fields = U::fields();
                let sql = format!("UPDATE {} SET ", table);
                let mut query_builder = dao.tuples_query_builder(sql, fields);
                gen_where_clause!(query_builder, D, $($E),*);
                trace!("update_{}: sql: {}", table, query_builder.sql());
                Ok(query_builder)
            }

            fn update_all_by<U: DataAccessObject, D: DataAccessObject>(&self, dao: &'a U, e: &'a [ ($(&'a $E),*) ]) -> Result<sqlx::QueryBuilder<'a, sqlx::Sqlite>, QueryError> {
                let table = D::sql_table();
                let fields = U::fields();
                let sql = format!("UPDATE {} SET ", table);
                let mut query_builder = dao.tuples_query_builder(sql, fields);
                if e.is_empty() {
                    // Safeguard so empty lookups don't update all rows
                    query_builder.push(" WHERE 1 = 0");
                } else {
                    gen_where_clause!(query_builder, D, e: [ $($E),* ]);
                }
                trace!("update_all_{}: sql: {}", table, query_builder.sql());
                Ok(query_builder)
            }
        }
    };
}

all_the_tuples!(impl_update_by);

pub trait DeleteBy<'a, E> {
    fn delete_by<D: DataAccessObject>(
        &self,
        e: &'a E,
    ) -> Result<sqlx::QueryBuilder<'a, sqlx::Sqlite>, QueryError>;
}

macro_rules! impl_delete_by {
    (
        [$($E:ident),*]
    ) => {
        #[allow(non_snake_case, unused_parens, unused_variables, unused_mut, unused_assignments)]
        impl<'a, Q, $($E),*> DeleteBy<'a, ($(&'a $E),*)> for Q
        where
            Q: Deref<Target = dyn Queries>,
            $($E: SqlEntity),*
        {
            fn delete_by<D: DataAccessObject>(&self, ($($E),*): &'a ($(&'a $E),*)) -> Result<sqlx::QueryBuilder<'a, sqlx::Sqlite>, QueryError> {
                let table = D::sql_table();
                let sql = format!("DELETE FROM {}", table);
                let mut query_builder = sqlx::QueryBuilder::new(sql);
                gen_where_clause!(query_builder, D, $($E),*);
                trace!("delete_{}: sql: {}", table, query_builder.sql());
                Ok(query_builder)
            }
        }
    };
}

all_the_tuples!(impl_delete_by);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crudl::ListParams;
    use lazy_static::lazy_static;
    use sqlx::Execute;
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_type::Dao;

    #[td_type::typed(string)]
    struct TestId;

    #[td_type::typed(string)]
    struct TestName;

    #[td_type::typed(i64)]
    struct TestModifiedOn;

    #[Dao]
    #[dao(sql_table = "test_table")]
    struct TestDao {
        id: TestId,
        name: TestName,
        modified_on: TestModifiedOn,
    }

    lazy_static! {
        static ref TEST_QUERIES: DaoQueries = DaoQueries::default();
    }

    mod default {
        use super::*;

        lazy_static! {
            static ref FIXTURE_DAOS: Vec<TestDao> = vec![
                TestDao {
                    id: TestId::try_from("00000000000000000000000004").unwrap(),
                    name: TestName::try_from("mario").unwrap(),
                    modified_on: TestModifiedOn::try_from(1234).unwrap(),
                },
                TestDao {
                    id: TestId::try_from("00000000000000000000000008").unwrap(),
                    name: TestName::try_from("luigi").unwrap(),
                    modified_on: TestModifiedOn::try_from(6789).unwrap(),
                },
            ];
        }

        #[td_test::test(sqlx(fixture = "test_queries"))]
        async fn test_dao_insert(db: DbPool) -> Result<(), TdError> {
            let dao = TestDao::builder()
                .id(TestId::try_from("")?)
                .try_name("bowser")?
                .try_modified_on(123)?
                .build()?;

            let mut query_builder = TEST_QUERIES.insert(&dao)?;
            let query = query_builder.build();

            let query_str = query.sql();
            assert_eq!(
                query_str,
                "INSERT INTO test_table (id, name, modified_on) VALUES (?, ?, ?)"
            );

            let result = query.execute(&db).await.unwrap();
            assert_eq!(result.rows_affected(), 1);

            let db_data: Vec<TestDao> =
                sqlx::query_as("SELECT * FROM test_table WHERE name = 'bowser'")
                    .fetch_all(&db)
                    .await
                    .unwrap();
            assert_eq!(db_data.len(), 1);
            assert_eq!(db_data[0], dao);
            Ok(())
        }

        #[td_test::test(sqlx(fixture = "test_queries"))]
        async fn test_dao_select_by(db: DbPool) -> Result<(), TdError> {
            let mut query_builder = TEST_QUERIES.select_by::<TestDao>(&())?;
            let query = query_builder.build_query_as();

            let query_str = query.sql();
            assert_eq!(
                query_str,
                "SELECT id, name, modified_on FROM test_table ORDER BY 1 DESC"
            );

            let result: Vec<TestDao> = query.fetch_all(&db).await.unwrap();
            assert_eq!(result.len(), 2);
            // Due to DESC id order
            assert_eq!(result[0], FIXTURE_DAOS[1]);
            assert_eq!(result[1], FIXTURE_DAOS[0]);
            Ok(())
        }

        #[td_test::test(sqlx(fixture = "test_queries"))]
        async fn test_dao_select_by_order_by(db: DbPool) -> Result<(), TdError> {
            #[Dao]
            #[dao(sql_table = "test_table", order_by = "modified_on")]
            struct OrderedTestDao {
                id: TestId,
                name: TestName,
                modified_on: TestModifiedOn,
            }

            let mut query_builder = TEST_QUERIES.select_by::<OrderedTestDao>(&())?;
            let query = query_builder.build_query_as();

            let query_str = query.sql();
            assert_eq!(
                query_str,
                "SELECT id, name, modified_on FROM test_table ORDER BY modified_on"
            );

            let result: Vec<TestDao> = query.fetch_all(&db).await.unwrap();
            assert_eq!(result.len(), 2);
            assert_eq!(result[0], FIXTURE_DAOS[0]);
            assert_eq!(result[1], FIXTURE_DAOS[1]);
            Ok(())
        }

        #[td_test::test(sqlx(fixture = "test_queries"))]
        async fn test_dao_select_by_where(db: DbPool) -> Result<(), TdError> {
            let by = &(TestName::try_from("mario")?);
            let mut query_builder = TEST_QUERIES.select_by::<TestDao>(&by)?;
            let query = query_builder.build_query_as();

            let query_str = query.sql();
            assert_eq!(
                query_str,
                "SELECT id, name, modified_on FROM test_table WHERE name = ? ORDER BY 1 DESC"
            );

            let result: Vec<TestDao> = query.fetch_all(&db).await.unwrap();
            assert_eq!(result.len(), 1);
            assert_eq!(result[0], FIXTURE_DAOS[0]);
            Ok(())
        }

        #[td_test::test(sqlx(fixture = "test_queries"))]
        async fn test_dao_select_by_where_tuple(db: DbPool) -> Result<(), TdError> {
            let by = (
                &TestId::try_from("00000000000000000000000004")?,
                &TestName::try_from("mario")?,
            );
            let mut query_builder = TEST_QUERIES.select_by::<TestDao>(&by)?;
            let query = query_builder.build_query_as();

            let query_str = query.sql();
            assert_eq!(
                query_str,
                "SELECT id, name, modified_on FROM test_table WHERE id = ? AND name = ? ORDER BY 1 DESC"
            );

            let result: Vec<TestDao> = query.fetch_all(&db).await.unwrap();
            assert_eq!(result.len(), 1);
            assert_eq!(result[0], FIXTURE_DAOS[0]);
            Ok(())
        }

        #[td_test::test(sqlx(fixture = "test_queries"))]
        async fn test_dao_find_by(db: DbPool) -> Result<(), TdError> {
            let find_by: [&TestId; 0] = [];
            let mut query_builder = TEST_QUERIES.find_by::<TestDao>(&find_by)?;
            let query = query_builder.build_query_as();

            let query_str = query.sql();
            assert_eq!(
                query_str,
                "SELECT id, name, modified_on FROM test_table WHERE 1 = 0"
            );

            let result: Vec<TestDao> = query.fetch_all(&db).await.unwrap();
            assert_eq!(result.len(), 0);
            Ok(())
        }

        #[td_test::test(sqlx(fixture = "test_queries"))]
        async fn test_dao_find_by_where(db: DbPool) -> Result<(), TdError> {
            let by = [&TestName::try_from("mario")?];
            let mut query_builder = TEST_QUERIES.find_by::<TestDao>(&by)?;
            let query = query_builder.build_query_as();

            let query_str = query.sql();
            assert_eq!(
                query_str,
                "SELECT id, name, modified_on FROM test_table WHERE (name = ?)"
            );

            let result: Vec<TestDao> = query.fetch_all(&db).await.unwrap();
            assert_eq!(result.len(), 1);
            assert_eq!(result[0], FIXTURE_DAOS[0]);
            Ok(())
        }

        #[td_test::test(sqlx(fixture = "test_queries"))]
        async fn test_dao_find_by_where_tuple(db: DbPool) -> Result<(), TdError> {
            let by = [(
                &TestId::try_from("00000000000000000000000004")?,
                &TestName::try_from("mario")?,
            )];
            let mut query_builder = TEST_QUERIES.find_by::<TestDao>(&by)?;
            let query = query_builder.build_query_as();

            let query_str = query.sql();
            assert_eq!(
                query_str,
                "SELECT id, name, modified_on FROM test_table WHERE (id = ? AND name = ?)"
            );

            let result: Vec<TestDao> = query.fetch_all(&db).await.unwrap();
            assert_eq!(result.len(), 1);
            assert_eq!(result[0], FIXTURE_DAOS[0]);
            Ok(())
        }

        #[td_test::test(sqlx(fixture = "test_queries"))]
        async fn test_dao_find_by_where_multiple_tuple(db: DbPool) -> Result<(), TdError> {
            let by = [
                (
                    &TestId::try_from("00000000000000000000000004")?,
                    &TestName::try_from("mario")?,
                ),
                (
                    &TestId::try_from("00000000000000000000000008")?,
                    &TestName::try_from("luigi")?,
                ),
            ];
            let mut query_builder = TEST_QUERIES.find_by::<TestDao>(&by)?;
            let query = query_builder.build_query_as();

            let query_str = query.sql();
            assert_eq!(
                query_str,
                "SELECT id, name, modified_on FROM test_table WHERE (id = ? AND name = ?) OR (id = ? AND name = ?)"
            );

            let result: Vec<TestDao> = query.fetch_all(&db).await.unwrap();
            assert_eq!(result.len(), 2);
            assert_eq!(result[0], FIXTURE_DAOS[0]);
            assert_eq!(result[1], FIXTURE_DAOS[1]);
            Ok(())
        }

        #[Dao]
        #[dao(sql_table = "test_table")]
        struct UpdateDao {
            name: TestName,
        }

        #[td_test::test(sqlx(fixture = "test_queries"))]
        async fn test_dao_update_by(db: DbPool) -> Result<(), TdError> {
            let update_dao = UpdateDao::builder().try_name("peach")?.build()?;
            let mut query_builder = TEST_QUERIES.update_by::<_, TestDao>(&update_dao, &())?;
            let query = query_builder.build();

            let query_str = query.sql();
            assert_eq!(query_str, "UPDATE test_table SET name = COALESCE(?, name)");

            let result = query.execute(&db).await.unwrap();
            assert_eq!(result.rows_affected(), 2);

            // All rows names got changed.
            let db_data: Vec<TestDao> =
                sqlx::query_as("SELECT * FROM test_table WHERE name != 'peach'")
                    .fetch_all(&db)
                    .await
                    .unwrap();
            assert_eq!(db_data.len(), 0);

            let db_data: Vec<TestDao> =
                sqlx::query_as("SELECT * FROM test_table WHERE name = 'peach'")
                    .fetch_all(&db)
                    .await
                    .unwrap();
            assert_eq!(db_data.len(), 2);
            Ok(())
        }

        #[td_test::test(sqlx(fixture = "test_queries"))]
        async fn test_dao_update_by_where(db: DbPool) -> Result<(), TdError> {
            let by = &(TestName::try_from("mario")?);
            let update_dao = UpdateDao::builder().try_name("peach")?.build()?;
            let mut query_builder = TEST_QUERIES.update_by::<_, TestDao>(&update_dao, &by)?;
            let query = query_builder.build();

            let query_str = query.sql();
            assert_eq!(
                query_str,
                "UPDATE test_table SET name = COALESCE(?, name) WHERE name = ?"
            );

            let result = query.execute(&db).await.unwrap();
            assert_eq!(result.rows_affected(), 1);

            // Only one row changed.
            let db_data: Vec<TestDao> =
                sqlx::query_as("SELECT * FROM test_table WHERE name != 'peach'")
                    .fetch_all(&db)
                    .await
                    .unwrap();
            assert_eq!(db_data.len(), 1);

            let db_data: Vec<TestDao> =
                sqlx::query_as("SELECT * FROM test_table WHERE name = 'peach'")
                    .fetch_all(&db)
                    .await
                    .unwrap();
            assert_eq!(db_data.len(), 1);
            Ok(())
        }

        #[td_test::test(sqlx(fixture = "test_queries"))]
        async fn test_dao_update_by_where_tuple(db: DbPool) -> Result<(), TdError> {
            let by = (
                &TestId::try_from("00000000000000000000000004")?,
                &TestName::try_from("mario")?,
            );
            let update_dao = UpdateDao::builder().try_name("peach")?.build()?;
            let mut query_builder = TEST_QUERIES.update_by::<_, TestDao>(&update_dao, &by)?;
            let query = query_builder.build();

            let query_str = query.sql();
            assert_eq!(
                query_str,
                "UPDATE test_table SET name = COALESCE(?, name) WHERE id = ? AND name = ?"
            );

            let result = query.execute(&db).await.unwrap();
            assert_eq!(result.rows_affected(), 1);

            // Only one row changed.
            let db_data: Vec<TestDao> =
                sqlx::query_as("SELECT * FROM test_table WHERE name != 'peach'")
                    .fetch_all(&db)
                    .await
                    .unwrap();
            assert_eq!(db_data.len(), 1);

            let db_data: Vec<TestDao> =
                sqlx::query_as("SELECT * FROM test_table WHERE name = 'peach'")
                    .fetch_all(&db)
                    .await
                    .unwrap();
            assert_eq!(db_data.len(), 1);
            Ok(())
        }

        #[td_test::test(sqlx(fixture = "test_queries"))]
        async fn test_dao_delete_by(db: DbPool) -> Result<(), TdError> {
            let mut query_builder = TEST_QUERIES.delete_by::<TestDao>(&())?;
            let query = query_builder.build();

            let query_str = query.sql();
            assert_eq!(query_str, "DELETE FROM test_table");

            let result = query.execute(&db).await.unwrap();
            assert_eq!(result.rows_affected(), 2);

            // All rows names got deleted.
            let db_data: Vec<TestDao> = sqlx::query_as("SELECT * FROM test_table")
                .fetch_all(&db)
                .await
                .unwrap();
            assert_eq!(db_data.len(), 0);
            Ok(())
        }

        #[td_test::test(sqlx(fixture = "test_queries"))]
        async fn test_dao_delete_by_where(db: DbPool) -> Result<(), TdError> {
            let by = &(TestName::try_from("mario")?);
            let mut query_builder = TEST_QUERIES.delete_by::<TestDao>(&by)?;
            let query = query_builder.build();

            let query_str = query.sql();
            assert_eq!(query_str, "DELETE FROM test_table WHERE name = ?");

            let result = query.execute(&db).await.unwrap();
            assert_eq!(result.rows_affected(), 1);

            // Only one row got deleted.
            let db_data: Vec<TestDao> =
                sqlx::query_as("SELECT * FROM test_table WHERE name == 'mario'")
                    .fetch_all(&db)
                    .await
                    .unwrap();
            assert_eq!(db_data.len(), 0);

            let db_data: Vec<TestDao> =
                sqlx::query_as("SELECT * FROM test_table WHERE name != 'mario'")
                    .fetch_all(&db)
                    .await
                    .unwrap();
            assert_eq!(db_data.len(), 1);
            Ok(())
        }

        #[td_test::test(sqlx(fixture = "test_queries"))]
        async fn test_dao_delete_by_where_tuple(db: DbPool) -> Result<(), TdError> {
            let by = (
                &TestId::try_from("00000000000000000000000004")?,
                &TestName::try_from("mario")?,
            );
            let mut query_builder = TEST_QUERIES.delete_by::<TestDao>(&by)?;
            let query = query_builder.build();

            let query_str = query.sql();
            assert_eq!(
                query_str,
                "DELETE FROM test_table WHERE id = ? AND name = ?"
            );

            let result = query.execute(&db).await.unwrap();
            assert_eq!(result.rows_affected(), 1);

            // Only one row got deleted.
            let db_data: Vec<TestDao> =
                sqlx::query_as("SELECT * FROM test_table WHERE name == 'mario'")
                    .fetch_all(&db)
                    .await
                    .unwrap();
            assert_eq!(db_data.len(), 0);

            let db_data: Vec<TestDao> =
                sqlx::query_as("SELECT * FROM test_table WHERE name != 'mario'")
                    .fetch_all(&db)
                    .await
                    .unwrap();
            assert_eq!(db_data.len(), 1);
            Ok(())
        }
    }

    mod list {
        use super::*;
        use crate::crudl::ListParamsBuilder;
        use td_type::Dto;

        lazy_static! {
            static ref FIXTURE_DAOS: Vec<TestDao> = vec![
                TestDao {
                    id: TestId::try_from("00000000000000000000000004").unwrap(),
                    name: TestName::try_from("B").unwrap(),
                    modified_on: TestModifiedOn::try_from(1).unwrap(),
                },
                TestDao {
                    id: TestId::try_from("00000000000000000000000008").unwrap(),
                    name: TestName::try_from("A").unwrap(),
                    modified_on: TestModifiedOn::try_from(2).unwrap(),
                },
                TestDao {
                    id: TestId::try_from("0000000000000000000000000C").unwrap(),
                    name: TestName::try_from("A").unwrap(),
                    modified_on: TestModifiedOn::try_from(3).unwrap(),
                },
                TestDao {
                    id: TestId::try_from("0000000000000000000000000G").unwrap(),
                    name: TestName::try_from("C").unwrap(),
                    modified_on: TestModifiedOn::try_from(4).unwrap(),
                },
            ];
        }

        #[td_test::test(sqlx(fixture = "test_list_queries"))]
        async fn test_dao_list(db: DbPool) -> Result<(), TdError> {
            #[Dto]
            #[dto(list(on = TestDao))]
            #[td_type(builder(try_from = TestDao))]
            struct TestDto {
                id: TestId,
                #[dto(list(filter, filter_like, order_by))]
                name: TestName,
                #[dto(list(filter, pagination_by = "+"))]
                modified_on: TestModifiedOn,
            }

            let list_params = ListParamsBuilder::default()
                .len(4usize)
                .filter(vec![
                    "modified_on:gt:0".to_string(),
                    "name:lk:*".to_string(),
                ])
                .order_by("name-".to_string())
                .next("C".to_string())
                .pagination_id("4".to_string())
                .build()
                .unwrap();
            let where_clause = &TestName::try_from("A")?;
            let list_query_params = ListQueryParams::<TestDto>::try_from(&list_params)?;
            let mut query_builder = TEST_QUERIES
                .list_by::<TestDto, NoListFilter>(&list_query_params, &(), &where_clause)
                .await?;
            let query = query_builder.build_query_as();

            let query_str = query.sql();
            assert!(
                query_str
                    == r#"SELECT id, name, modified_on FROM test_table WHERE name = ? AND (modified_on > ?) AND (name LIKE ? ESCAPE '\' ) AND (name < ? OR (name = ? AND modified_on < ?)) ORDER BY name DESC, modified_on DESC LIMIT ?"#
                    || query_str
                        == r#"SELECT id, name, modified_on FROM test_table WHERE name = ? AND (name LIKE ? ESCAPE '\' ) AND (modified_on > ?) AND (name < ? OR (name = ? AND modified_on < ?)) ORDER BY name DESC, modified_on DESC LIMIT ?"#
            );

            let result: Vec<TestDao> = query.fetch_all(&db).await.unwrap();
            assert_eq!(result.len(), 2);
            assert_eq!(result[0], FIXTURE_DAOS[2]);
            assert_eq!(result[1], FIXTURE_DAOS[1]);
            Ok(())
        }

        #[td_test::test(sqlx(fixture = "test_list_queries"))]
        async fn test_dao_list_default(db: DbPool) -> Result<(), TdError> {
            #[Dto]
            #[dto(list(on = TestDao))]
            #[td_type(builder(try_from = TestDao))]
            struct TestDto {
                #[dto(list(pagination_by = "+"))]
                id: TestId,
                name: TestName,
                modified_on: TestModifiedOn,
            }

            let list_params = ListParams::default();
            let list_query_params = ListQueryParams::<TestDto>::try_from(&list_params)?;
            let mut query_builder = TEST_QUERIES
                .list_by::<TestDto, NoListFilter>(&list_query_params, &(), &())
                .await?;
            let query = query_builder.build_query_as();

            let query_str = query.sql();
            assert_eq!(
                query_str,
                "SELECT id, name, modified_on FROM test_table ORDER BY id ASC LIMIT ?"
            );

            let result: Vec<TestDao> = query.fetch_all(&db).await.unwrap();
            assert_eq!(result, *FIXTURE_DAOS);
            Ok(())
        }

        #[td_test::test(sqlx(fixture = "test_list_queries"))]
        async fn test_dao_list_natural_order_by(db: DbPool) -> Result<(), TdError> {
            #[Dto]
            #[dto(list(on = TestDao))]
            #[td_type(builder(try_from = TestDao))]
            struct TestDto {
                id: TestId,
                name: TestName,
                #[dto(list(pagination_by = "-"))]
                modified_on: TestModifiedOn,
            }

            let list_params = ListParams::default();
            let list_query_params = ListQueryParams::<TestDto>::try_from(&list_params)?;
            let mut query_builder = TEST_QUERIES
                .list_by::<TestDto, NoListFilter>(&list_query_params, &(), &())
                .await?;
            let query = query_builder.build_query_as();

            let query_str = query.sql();
            assert_eq!(
                query_str,
                "SELECT id, name, modified_on FROM test_table ORDER BY modified_on DESC LIMIT ?"
            );

            let result: Vec<TestDao> = query.fetch_all(&db).await.unwrap();
            let mut expected = FIXTURE_DAOS.clone();
            expected.sort_by(|a, b| b.modified_on.cmp(&a.modified_on));
            assert_eq!(result, expected);
            Ok(())
        }

        #[td_test::test(sqlx(fixture = "test_list_queries"))]
        async fn test_dao_list_order_by(db: DbPool) -> Result<(), TdError> {
            #[Dto]
            #[dto(list(on = TestDao))]
            #[td_type(builder(try_from = TestDao))]
            struct TestDto {
                id: TestId,
                #[dto(list(order_by))]
                name: TestName,
                #[dto(list(pagination_by = "-"))]
                modified_on: TestModifiedOn,
            }

            let list_params = ListParamsBuilder::default()
                .order_by("name".to_string())
                .build()
                .unwrap();
            let list_query_params = ListQueryParams::<TestDto>::try_from(&list_params)?;
            let mut query_builder = TEST_QUERIES
                .list_by::<TestDto, NoListFilter>(&list_query_params, &(), &())
                .await?;
            let query = query_builder.build_query_as();

            let query_str = query.sql();
            assert_eq!(
                query_str,
                "SELECT id, name, modified_on FROM test_table ORDER BY name ASC, modified_on ASC LIMIT ?"
            );

            let result: Vec<TestDao> = query.fetch_all(&db).await.unwrap();
            let mut expected = FIXTURE_DAOS.clone();
            expected.sort_by(|a, b| a.name.cmp(&b.name).then(a.modified_on.cmp(&b.modified_on)));
            assert_eq!(result, expected);
            Ok(())
        }

        #[td_test::test(sqlx(fixture = "test_list_queries"))]
        async fn test_dao_list_order_by_mapping(db: DbPool) -> Result<(), TdError> {
            #[Dto]
            #[dto(list(on = TestDao))]
            #[td_type(builder(try_from = TestDao))]
            struct TestDto {
                id: TestId,
                #[dto(list(order_by))]
                name: TestName,
                #[td_type(builder(field = "modified_on"))]
                #[dto(list(pagination_by = "-"))]
                created_at: TestModifiedOn,
            }

            let list_params = ListParamsBuilder::default()
                .order_by("name".to_string())
                .build()
                .unwrap();
            let list_query_params = ListQueryParams::<TestDto>::try_from(&list_params)?;
            let mut query_builder = TEST_QUERIES
                .list_by::<TestDto, NoListFilter>(&list_query_params, &(), &())
                .await?;
            let query = query_builder.build_query_as();

            let query_str = query.sql();
            assert_eq!(
                query_str,
                "SELECT id, name, modified_on FROM test_table ORDER BY name ASC, modified_on ASC LIMIT ?"
            );

            let result: Vec<TestDao> = query.fetch_all(&db).await.unwrap();
            let mut expected = FIXTURE_DAOS.clone();
            expected.sort_by(|a, b| a.name.cmp(&b.name).then(a.modified_on.cmp(&b.modified_on)));
            assert_eq!(result, expected);
            Ok(())
        }

        #[td_test::test(sqlx(fixture = "test_list_queries"))]
        async fn test_dao_list_filter(db: DbPool) -> Result<(), TdError> {
            #[Dto]
            #[dto(list(on = TestDao))]
            #[td_type(builder(try_from = TestDao))]
            struct TestDto {
                #[dto(list(pagination_by = "+"))]
                id: TestId,
                #[dto(list(filter))]
                name: TestName,
                modified_on: TestModifiedOn,
            }

            let list_params = ListParamsBuilder::default()
                .filter(vec!["name:eq:A".to_string()])
                .build()?;
            let list_query_params = ListQueryParams::<TestDto>::try_from(&list_params)?;
            let mut query_builder = TEST_QUERIES
                .list_by::<TestDto, NoListFilter>(&list_query_params, &(), &())
                .await?;
            let query = query_builder.build_query_as();

            let query_str = query.sql();
            assert_eq!(
                query_str,
                "SELECT id, name, modified_on FROM test_table WHERE (name = ?) ORDER BY id ASC LIMIT ?"
            );

            let result: Vec<TestDao> = query.fetch_all(&db).await.unwrap();
            assert_eq!(result.len(), 2);
            assert_eq!(result[0], FIXTURE_DAOS[1]);
            assert_eq!(result[1], FIXTURE_DAOS[2]);
            Ok(())
        }

        #[td_test::test(sqlx(fixture = "test_list_queries"))]
        async fn test_dao_list_filter_like(db: DbPool) -> Result<(), TdError> {
            #[Dto]
            #[dto(list(on = TestDao))]
            #[td_type(builder(try_from = TestDao))]
            struct TestDto {
                #[dto(list(pagination_by = "+"))]
                id: TestId,
                #[dto(list(filter_like))]
                name: TestName,
                modified_on: TestModifiedOn,
            }

            let list_params = ListParamsBuilder::default()
                .filter(vec!["name:lk:A".to_string()])
                .build()
                .unwrap();
            let list_query_params = ListQueryParams::<TestDto>::try_from(&list_params)?;
            let mut query_builder = TEST_QUERIES
                .list_by::<TestDto, NoListFilter>(&list_query_params, &(), &())
                .await?;
            let query = query_builder.build_query_as();

            let query_str = query.sql();
            assert_eq!(
                query_str,
                "SELECT id, name, modified_on FROM test_table WHERE (name LIKE ? ESCAPE '\\' ) ORDER BY id ASC LIMIT ?"
            );

            let result: Vec<TestDao> = query.fetch_all(&db).await.unwrap();
            assert_eq!(result.len(), 2);
            assert_eq!(result[0], FIXTURE_DAOS[1]);
            assert_eq!(result[1], FIXTURE_DAOS[2]);
            Ok(())
        }

        #[td_test::test(sqlx(fixture = "test_list_queries"))]
        async fn test_dao_list_filter_like_wildcard(db: DbPool) -> Result<(), TdError> {
            #[Dto]
            #[dto(list(on = TestDao))]
            #[td_type(builder(try_from = TestDao))]
            struct TestDto {
                #[dto(list(pagination_by = "+"))]
                id: TestId,
                #[dto(list(filter_like))]
                name: TestName,
                modified_on: TestModifiedOn,
            }

            let list_params = ListParamsBuilder::default()
                .filter(vec!["name:lk:*".to_string()])
                .build()
                .unwrap();
            let list_query_params = ListQueryParams::<TestDto>::try_from(&list_params)?;
            let mut query_builder = TEST_QUERIES
                .list_by::<TestDto, NoListFilter>(&list_query_params, &(), &())
                .await?;
            let query = query_builder.build_query_as();

            let query_str = query.sql();
            assert_eq!(
                query_str,
                "SELECT id, name, modified_on FROM test_table WHERE (name LIKE ? ESCAPE '\\' ) ORDER BY id ASC LIMIT ?"
            );

            let result: Vec<TestDao> = query.fetch_all(&db).await.unwrap();
            assert_eq!(result, *FIXTURE_DAOS);
            Ok(())
        }

        #[td_test::test(sqlx(fixture = "test_list_queries"))]
        async fn test_dao_list_filter_between(db: DbPool) -> Result<(), TdError> {
            #[Dto]
            #[dto(list(on = TestDao))]
            #[td_type(builder(try_from = TestDao))]
            struct TestDto {
                #[dto(list(pagination_by = "+"))]
                id: TestId,
                #[dto(list(filter))]
                name: TestName,
                modified_on: TestModifiedOn,
            }

            let list_params = ListParamsBuilder::default()
                .filter(vec!["name:btw:B::C".to_string()])
                .build()
                .unwrap();
            let list_query_params = ListQueryParams::<TestDto>::try_from(&list_params)?;
            let mut query_builder = TEST_QUERIES
                .list_by::<TestDto, NoListFilter>(&list_query_params, &(), &())
                .await?;
            let query = query_builder.build_query_as();

            let query_str = query.sql();
            assert_eq!(
                query_str,
                "SELECT id, name, modified_on FROM test_table WHERE (name BETWEEN ? AND ?) ORDER BY id ASC LIMIT ?"
            );

            let result: Vec<TestDao> = query.fetch_all(&db).await.unwrap();
            assert_eq!(result.len(), 2);
            assert_eq!(result[0], FIXTURE_DAOS[0]);
            assert_eq!(result[1], FIXTURE_DAOS[3]);
            Ok(())
        }

        #[td_test::test(sqlx(fixture = "test_list_queries"))]
        async fn test_dao_list_len(db: DbPool) -> Result<(), TdError> {
            #[Dto]
            #[dto(list(on = TestDao))]
            #[td_type(builder(try_from = TestDao))]
            struct TestDto {
                #[dto(list(pagination_by = "+"))]
                id: TestId,
                name: TestName,
                modified_on: TestModifiedOn,
            }

            let list_params = ListParamsBuilder::default().len(1usize).build().unwrap();
            let list_query_params = ListQueryParams::<TestDto>::try_from(&list_params)?;
            let mut query_builder = TEST_QUERIES
                .list_by::<TestDto, NoListFilter>(&list_query_params, &(), &())
                .await?;
            let query = query_builder.build_query_as();

            let query_str = query.sql();
            assert_eq!(
                query_str,
                "SELECT id, name, modified_on FROM test_table ORDER BY id ASC LIMIT ?"
            );

            let result: Vec<TestDao> = query.fetch_all(&db).await.unwrap();
            assert_eq!(result.len(), 1);
            assert_eq!(result[0], FIXTURE_DAOS[0]);
            Ok(())
        }

        #[td_test::test(sqlx(fixture = "test_list_queries"))]
        async fn test_dao_list_pagination_previous_asc_natural_order(
            db: DbPool,
        ) -> Result<(), TdError> {
            #[Dto]
            #[dto(list(on = TestDao))]
            #[td_type(builder(try_from = TestDao))]
            struct TestDto {
                #[dto(list(pagination_by = "+"))]
                id: TestId,
                name: TestName,
                modified_on: TestModifiedOn,
            }

            let list_params = ListParamsBuilder::default()
                .previous(FIXTURE_DAOS[1].id().to_string())
                .pagination_id(FIXTURE_DAOS[1].id().to_string())
                .build()
                .unwrap();
            let list_query_params = ListQueryParams::<TestDto>::try_from(&list_params)?;
            let mut query_builder = TEST_QUERIES
                .list_by::<TestDto, NoListFilter>(&list_query_params, &(), &())
                .await?;
            let query = query_builder.build_query_as();

            let query_str = query.sql();
            assert_eq!(
                query_str,
                "SELECT id, name, modified_on FROM test_table WHERE (id < ? OR (id = ? AND id < ?)) ORDER BY id DESC LIMIT ?"
            );

            let result: Vec<TestDao> = query.fetch_all(&db).await.unwrap();
            assert_eq!(result.len(), 1);
            assert_eq!(result[0], FIXTURE_DAOS[0]);
            Ok(())
        }

        #[td_test::test(sqlx(fixture = "test_list_queries"))]
        async fn test_dao_list_pagination_next_asc(db: DbPool) -> Result<(), TdError> {
            #[Dto]
            #[dto(list(on = TestDao))]
            #[td_type(builder(try_from = TestDao))]
            struct TestDto {
                #[dto(list(pagination_by = "+"))]
                id: TestId,
                name: TestName,
                modified_on: TestModifiedOn,
            }

            let list_params = ListParamsBuilder::default()
                .next(FIXTURE_DAOS[2].id().to_string())
                .pagination_id(FIXTURE_DAOS[2].id().to_string())
                .build()
                .unwrap();
            let list_query_params = ListQueryParams::<TestDto>::try_from(&list_params)?;
            let mut query_builder = TEST_QUERIES
                .list_by::<TestDto, NoListFilter>(&list_query_params, &(), &())
                .await?;
            let query = query_builder.build_query_as();

            let query_str = query.sql();
            assert_eq!(
                query_str,
                "SELECT id, name, modified_on FROM test_table WHERE (id > ? OR (id = ? AND id > ?)) ORDER BY id ASC LIMIT ?"
            );

            let result: Vec<TestDao> = query.fetch_all(&db).await.unwrap();
            assert_eq!(result.len(), 1);
            assert_eq!(result[0], FIXTURE_DAOS[3]);
            Ok(())
        }

        #[td_test::test(sqlx(fixture = "test_list_queries"))]
        async fn test_dao_list_pagination_next_desc(db: DbPool) -> Result<(), TdError> {
            #[Dto]
            #[dto(list(on = TestDao))]
            #[td_type(builder(try_from = TestDao))]
            struct TestDto {
                #[dto(list(pagination_by = "-"))]
                id: TestId,
                name: TestName,
                modified_on: TestModifiedOn,
            }

            let list_params = ListParamsBuilder::default()
                .next(FIXTURE_DAOS[1].id().to_string())
                .pagination_id(FIXTURE_DAOS[1].id().to_string())
                .build()
                .unwrap();
            let list_query_params = ListQueryParams::<TestDto>::try_from(&list_params)?;
            let mut query_builder = TEST_QUERIES
                .list_by::<TestDto, NoListFilter>(&list_query_params, &(), &())
                .await?;
            let query = query_builder.build_query_as();

            let query_str = query.sql();
            assert_eq!(
                query_str,
                "SELECT id, name, modified_on FROM test_table WHERE (id < ? OR (id = ? AND id < ?)) ORDER BY id DESC LIMIT ?"
            );

            let result: Vec<TestDao> = query.fetch_all(&db).await.unwrap();
            assert_eq!(result.len(), 1);
            assert_eq!(result[0], FIXTURE_DAOS[0]);
            Ok(())
        }

        #[td_test::test(sqlx(fixture = "test_list_queries"))]
        async fn test_dao_list_pagination_previous_desc(db: DbPool) -> Result<(), TdError> {
            #[Dto]
            #[dto(list(on = TestDao))]
            #[td_type(builder(try_from = TestDao))]
            struct TestDto {
                #[dto(list(pagination_by = "-"))]
                id: TestId,
                name: TestName,
                modified_on: TestModifiedOn,
            }

            let list_params = ListParamsBuilder::default()
                .previous(FIXTURE_DAOS[2].id().to_string())
                .pagination_id(FIXTURE_DAOS[2].id().to_string())
                .build()?;
            let list_query_params = ListQueryParams::<TestDto>::try_from(&list_params)?;
            let mut query_builder = TEST_QUERIES
                .list_by::<TestDto, NoListFilter>(&list_query_params, &(), &())
                .await?;
            let query = query_builder.build_query_as();

            let query_str = query.sql();
            assert_eq!(
                query_str,
                "SELECT id, name, modified_on FROM test_table WHERE (id > ? OR (id = ? AND id > ?)) ORDER BY id ASC LIMIT ?"
            );

            let result: Vec<TestDao> = query.fetch_all(&db).await.unwrap();
            assert_eq!(result.len(), 1);
            assert_eq!(result[0], FIXTURE_DAOS[3]);
            Ok(())
        }

        #[td_test::test(sqlx(fixture = "test_list_queries"))]
        async fn test_dao_list_pagination_order_by_next_asc(db: DbPool) -> Result<(), TdError> {
            #[Dto]
            #[dto(list(on = TestDao))]
            #[td_type(builder(try_from = TestDao))]
            struct TestDto {
                #[dto(list(pagination_by = "+"))]
                id: TestId,
                #[dto(list(order_by))]
                name: TestName,
                modified_on: TestModifiedOn,
            }

            let list_params = ListParamsBuilder::default()
                .order_by("name+".to_string())
                .next("A".to_string())
                .pagination_id(FIXTURE_DAOS[1].id().to_string())
                .build()
                .unwrap();
            let list_query_params = ListQueryParams::<TestDto>::try_from(&list_params)?;
            let mut query_builder = TEST_QUERIES
                .list_by::<TestDto, NoListFilter>(&list_query_params, &(), &())
                .await?;
            let query = query_builder.build_query_as();

            let query_str = query.sql();
            assert_eq!(
                query_str,
                "SELECT id, name, modified_on FROM test_table WHERE (name > ? OR (name = ? AND id > ?)) ORDER BY name ASC, id ASC LIMIT ?"
            );

            let result: Vec<TestDao> = query.fetch_all(&db).await.unwrap();
            assert_eq!(result.len(), 3);
            assert_eq!(result[0], FIXTURE_DAOS[2]);
            assert_eq!(result[1], FIXTURE_DAOS[0]);
            assert_eq!(result[2], FIXTURE_DAOS[3]);
            Ok(())
        }

        #[td_test::test(sqlx(fixture = "test_list_queries"))]
        async fn test_dao_list_pagination_order_by_previous_asc_natural_order(
            db: DbPool,
        ) -> Result<(), TdError> {
            #[Dto]
            #[dto(list(on = TestDao))]
            #[td_type(builder(try_from = TestDao))]
            struct TestDto {
                #[dto(list(pagination_by = "+"))]
                id: TestId,
                #[dto(list(order_by))]
                name: TestName,
                modified_on: TestModifiedOn,
            }

            let list_params = ListParamsBuilder::default()
                .order_by("id+".to_string())
                .previous(FIXTURE_DAOS[0].id().to_string())
                .pagination_id(FIXTURE_DAOS[0].id().to_string())
                .build()
                .unwrap();
            let list_query_params = ListQueryParams::<TestDto>::try_from(&list_params)?;
            let mut query_builder = TEST_QUERIES
                .list_by::<TestDto, NoListFilter>(&list_query_params, &(), &())
                .await?;
            let query = query_builder.build_query_as();

            let query_str = query.sql();
            assert_eq!(
                query_str,
                "SELECT id, name, modified_on FROM test_table WHERE (id < ? OR (id = ? AND id < ?)) ORDER BY id DESC, id DESC LIMIT ?"
            );

            let result: Vec<TestDao> = query.fetch_all(&db).await.unwrap();
            assert_eq!(result.len(), 0);
            Ok(())
        }

        #[td_test::test(sqlx(fixture = "test_list_queries"))]
        async fn test_dao_list_pagination_order_by_previous_asc_order(
            db: DbPool,
        ) -> Result<(), TdError> {
            #[Dto]
            #[dto(list(on = TestDao))]
            #[td_type(builder(try_from = TestDao))]
            struct TestDto {
                #[dto(list(pagination_by = "+"))]
                id: TestId,
                #[dto(list(order_by))]
                name: TestName,
                modified_on: TestModifiedOn,
            }

            let list_params = ListParamsBuilder::default()
                .order_by("name+".to_string())
                .previous("A".to_string())
                .pagination_id(FIXTURE_DAOS[1].id().to_string())
                .build()
                .unwrap();
            let list_query_params = ListQueryParams::<TestDto>::try_from(&list_params)?;
            let mut query_builder = TEST_QUERIES
                .list_by::<TestDto, NoListFilter>(&list_query_params, &(), &())
                .await?;
            let query = query_builder.build_query_as();

            let query_str = query.sql();
            assert_eq!(
                query_str,
                "SELECT id, name, modified_on FROM test_table WHERE (name < ? OR (name = ? AND id < ?)) ORDER BY name DESC, id DESC LIMIT ?"
            );

            let result: Vec<TestDao> = query.fetch_all(&db).await.unwrap();
            assert_eq!(result.len(), 0);
            Ok(())
        }

        #[td_test::test(sqlx(fixture = "test_list_queries"))]
        async fn test_dao_list_pagination_order_by_next_desc(db: DbPool) -> Result<(), TdError> {
            #[Dto]
            #[dto(list(on = TestDao))]
            #[td_type(builder(try_from = TestDao))]
            struct TestDto {
                #[dto(list(pagination_by = "+"))]
                id: TestId,
                #[dto(list(order_by))]
                name: TestName,
                modified_on: TestModifiedOn,
            }

            let list_params = ListParamsBuilder::default()
                .order_by("name-".to_string())
                .next("B".to_string())
                .pagination_id(FIXTURE_DAOS[0].id().to_string())
                .build()
                .unwrap();
            let list_query_params = ListQueryParams::<TestDto>::try_from(&list_params)?;
            let mut query_builder = TEST_QUERIES
                .list_by::<TestDto, NoListFilter>(&list_query_params, &(), &())
                .await?;
            let query = query_builder.build_query_as();

            let query_str = query.sql();
            assert_eq!(
                query_str,
                "SELECT id, name, modified_on FROM test_table WHERE (name < ? OR (name = ? AND id < ?)) ORDER BY name DESC, id DESC LIMIT ?"
            );

            let result: Vec<TestDao> = query.fetch_all(&db).await.unwrap();
            assert_eq!(result.len(), 2);
            assert_eq!(result[0], FIXTURE_DAOS[2]);
            assert_eq!(result[1], FIXTURE_DAOS[1]);
            Ok(())
        }

        #[td_test::test(sqlx(fixture = "test_list_queries"))]
        async fn test_dao_list_pagination_order_by_previous_desc_natural_order(
            db: DbPool,
        ) -> Result<(), TdError> {
            #[Dto]
            #[dto(list(on = TestDao))]
            #[td_type(builder(try_from = TestDao))]
            struct TestDto {
                #[dto(list(pagination_by = "+"))]
                id: TestId,
                #[dto(list(order_by))]
                name: TestName,
                modified_on: TestModifiedOn,
            }

            let list_params = ListParamsBuilder::default()
                .order_by("id-".to_string())
                .previous(FIXTURE_DAOS[0].id().to_string())
                .pagination_id(FIXTURE_DAOS[0].id().to_string())
                .build()
                .unwrap();
            let list_query_params = ListQueryParams::<TestDto>::try_from(&list_params)?;
            let mut query_builder = TEST_QUERIES
                .list_by::<TestDto, NoListFilter>(&list_query_params, &(), &())
                .await?;
            let query = query_builder.build_query_as();

            let query_str = query.sql();
            assert_eq!(
                query_str,
                "SELECT id, name, modified_on FROM test_table WHERE (id > ? OR (id = ? AND id > ?)) ORDER BY id ASC, id ASC LIMIT ?"
            );

            let result: Vec<TestDao> = query.fetch_all(&db).await.unwrap();
            assert_eq!(result.len(), 3);
            assert_eq!(result[0], FIXTURE_DAOS[1]);
            Ok(())
        }

        #[td_test::test(sqlx(fixture = "test_list_queries"))]
        async fn test_dao_list_pagination_order_by_previous_desc_order(
            db: DbPool,
        ) -> Result<(), TdError> {
            #[Dto]
            #[dto(list(on = TestDao))]
            #[td_type(builder(try_from = TestDao))]
            struct TestDto {
                #[dto(list(pagination_by = "+"))]
                id: TestId,
                #[dto(list(order_by))]
                name: TestName,
                modified_on: TestModifiedOn,
            }

            let list_params = ListParamsBuilder::default()
                .order_by("name-".to_string())
                .previous("B".to_string())
                .pagination_id(FIXTURE_DAOS[0].id().to_string())
                .build()
                .unwrap();
            let list_query_params = ListQueryParams::<TestDto>::try_from(&list_params)?;
            let mut query_builder = TEST_QUERIES
                .list_by::<TestDto, NoListFilter>(&list_query_params, &(), &())
                .await?;
            let query = query_builder.build_query_as();

            let query_str = query.sql();
            assert_eq!(
                query_str,
                "SELECT id, name, modified_on FROM test_table WHERE (name > ? OR (name = ? AND id > ?)) ORDER BY name ASC, id ASC LIMIT ?"
            );

            let result: Vec<TestDao> = query.fetch_all(&db).await.unwrap();
            assert_eq!(result.len(), 1);
            assert_eq!(result[0], FIXTURE_DAOS[3]);
            Ok(())
        }

        #[td_test::test(sqlx(fixture = "test_list_queries"))]
        async fn test_dao_list_filters(db: DbPool) -> Result<(), TdError> {
            #[Dto]
            #[dto(list(on = TestDao))]
            #[td_type(builder(try_from = TestDao))]
            struct TestDto {
                #[dto(list(pagination_by = "+", filter))]
                id: TestId,
                #[dto(list(filter))]
                name: TestName,
                modified_on: TestModifiedOn,
            }

            let list_params = ListParamsBuilder::default()
                .filter(vec![
                    "name:eq:B".to_string(),
                    "id:gt:00000000000000000000000000".to_string(),
                    "name:ge:Z".to_string(),
                ])
                .build()
                .unwrap();
            let list_query_params = ListQueryParams::<TestDto>::try_from(&list_params)?;
            let mut query_builder = TEST_QUERIES
                .list_by::<TestDto, NoListFilter>(&list_query_params, &(), &())
                .await?;
            let query = query_builder.build_query_as();

            let query_str = query.sql();
            assert!(
                query_str
                    == "SELECT id, name, modified_on FROM test_table WHERE (name = ? OR name >= ?) AND (id > ?) ORDER BY id ASC LIMIT ?"
                    || query_str
                        == "SELECT id, name, modified_on FROM test_table WHERE (id > ?) AND (name = ? OR name >= ?) ORDER BY id ASC LIMIT ?"
            );

            let result: Vec<TestDao> = query.fetch_all(&db).await.unwrap();
            assert_eq!(result.len(), 1);
            assert_eq!(result[0], FIXTURE_DAOS[0]);
            Ok(())
        }

        #[td_test::test(sqlx(fixture = "test_list_queries"))]
        async fn test_dao_list_filter_order_by(db: DbPool) -> Result<(), TdError> {
            #[Dto]
            #[dto(list(on = TestDao))]
            #[td_type(builder(try_from = TestDao))]
            struct TestDto {
                id: TestId,
                #[dto(list(order_by, filter))]
                name: TestName,
                #[dto(list(pagination_by = "+"))]
                modified_on: TestModifiedOn,
            }

            let list_params = ListParamsBuilder::default()
                .len(1usize)
                .filter(vec!["name:eq:C".to_string()])
                .order_by("name-".to_string())
                .build()
                .unwrap();
            let list_query_params = ListQueryParams::<TestDto>::try_from(&list_params)?;
            let mut query_builder = TEST_QUERIES
                .list_by::<TestDto, NoListFilter>(&list_query_params, &(), &())
                .await?;
            let query = query_builder.build_query_as();

            let query_str = query.sql();
            assert_eq!(
                query_str,
                "SELECT id, name, modified_on FROM test_table WHERE (name = ?) ORDER BY name DESC, modified_on DESC LIMIT ?"
            );

            let result: Vec<TestDao> = query.fetch_all(&db).await.unwrap();
            assert_eq!(result.len(), 1);
            assert_eq!(result[0], FIXTURE_DAOS[3]);
            Ok(())
        }
    }
}
