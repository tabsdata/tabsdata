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
mod cte;
pub mod recursive;

use crate::crudl::ListParams;
use crate::types::{DataAccessObject, SqlEntity};
use std::ops::Deref;
use td_error::td_error;

#[cfg(feature = "td-test")]
use std::println as trace;
#[cfg(not(feature = "td-test"))]
use tracing::trace;

#[td_error]
pub enum QueryError {
    #[error("Type not found: {0:?}")]
    TypeNotFound(String) = 5000,
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

/// Utility trait for Send + Sync Deref to Queries
pub trait DerefQueries: Deref<Target = dyn Queries> + Send + Sync {}
impl<Q> DerefQueries for Q where Q: Deref<Target = dyn Queries> + Send + Sync {}

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
#[macro_export]
macro_rules! gen_where_clause {
    // Cases for when there's no condition to add (empty input).
    ($query_builder:expr, $D:ident, ) => {};
    ($query_builder:expr, $D:ident, $vect:ident: []) => {};

    // Case for when there's at least one condition.
    ($query_builder:expr, $D:ident, $($rest:tt)+) => {{
        $query_builder.push(" WHERE ");
        let mut first = true;
        gen_where_clause!(@munch $query_builder, $D, first, $($rest)+);
    }};

    // Binding
    (@bind $query_builder:expr, $D:ident, $E:ident) => {{
        let column = $D::sql_field_for_type::<$E>()
            .ok_or(QueryError::TypeNotFound(std::any::type_name::<$E>().to_string()))?;
        $query_builder
            .push(format!("{} = ", column))
            .push_bind($E.value());
    }};

    // Base case: nothing to do here
    (@munch $query_builder:expr, $D:ident, $first:ident) => {};

    // Single identifier (normal case). AND group.
    (@munch $query_builder:expr, $D:ident, $first:ident, $E:ident $(, $($rest:tt)*)?) => {{
        if !$first { $query_builder.push(" AND "); }
        $first = false;
        gen_where_clause!(@bind $query_builder, $D, $E);
        gen_where_clause!(@munch $query_builder, $D, $first $(, $($rest)*)?);
    }};

    // Case for an empty array (no expansion needed)
    (@munch $query_builder:expr, $first:ident, []) => {};

    // AND/OR group. Joining arrays.
    (@munch $query_builder:expr, $D:ident, $first:ident,  $vect:ident: [ $($inner:ident),* ] $(, $($rest:tt)*)?) => {{
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
                if !e.is_empty() {
                    gen_where_clause!(query_builder, D, e: [ $($E),* ]);
                }
                trace!("select_{}: sql: {}", table, query_builder.sql());
                Ok(query_builder)
            }
        }
    };
}

all_the_tuples!(impl_find_by);

pub trait ListBy<'a, E> {
    fn list_by<D: DataAccessObject>(
        &self,
        list_params: &ListParams,
        e: &'a E,
    ) -> Result<sqlx::QueryBuilder<'a, sqlx::Sqlite>, QueryError>;
}

macro_rules! impl_list_by {
    (
        [$($E:ident),*]
    ) => {
        #[allow(non_snake_case, unused_parens, unused_variables, unused_mut, unused_assignments)]
        impl<'a, Q, $($E),*> ListBy<'a, ($(&'a $E),*)> for Q
        where
            Q: Deref<Target = dyn Queries>,
            $($E: SqlEntity),*
        {
            fn list_by<D: DataAccessObject>(
                &self,
                list_params: &ListParams,
                ($($E),*): &'a ($(&'a $E),*),
            ) -> Result<sqlx::QueryBuilder<'a, sqlx::Sqlite>, QueryError> {
                let table = D::sql_table();
                let fields = D::fields();
                let sql = format!("SELECT {} FROM {}", fields.join(", "), table);

                let mut query_builder = sqlx::QueryBuilder::new(sql);
                gen_where_clause!(query_builder, D, $($E),*);
                query_builder
                    .push(" LIMIT ")
                    .push_bind((list_params.len() + 1) as i64);
                query_builder
                    .push(" OFFSET ")
                    .push_bind(*list_params.offset() as i64);

                trace!("list_{}: sql: {}", table, query_builder.sql());
                Ok(query_builder)
            }
        }
    };
}

all_the_tuples!(impl_list_by);

// D is needed to get the fields types for the WHERE clauses
pub trait UpdateBy<'a, E> {
    fn update_by<U: DataAccessObject, D: DataAccessObject>(
        &self,
        dao: &'a U,
        e: &'a E,
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
                let table = D::sql_table();
                let fields = U::fields();
                let sql = format!("UPDATE {} SET ", table);
                let mut query_builder = dao.tuples_query_builder(sql, fields);
                gen_where_clause!(query_builder, D, $($E),*);
                trace!("update_{}: sql: {}", table, query_builder.sql());
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
    use crate::crudl::ListParamsBuilder;
    use lazy_static::lazy_static;
    use sqlx::Execute;
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_type::Dao;

    #[td_type::typed(id)]
    struct TestId;

    #[td_type::typed(string)]
    struct TestName;

    #[td_type::typed(i64)]
    struct TestModifiedOn;

    #[Dao(sql_table = "test_table")]
    struct TestDao {
        id: TestId,
        name: TestName,
        modified_on: TestModifiedOn,
    }

    lazy_static! {
        static ref TEST_QUERIES: DaoQueries = DaoQueries::default();
    }

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
            .id(TestId::default())
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
        #[Dao(sql_table = "test_table", order_by = "modified_on")]
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
        assert_eq!(query_str, "SELECT id, name, modified_on FROM test_table");

        let result: Vec<TestDao> = query.fetch_all(&db).await.unwrap();
        assert_eq!(result, *FIXTURE_DAOS);
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

    #[td_test::test(sqlx(fixture = "test_queries"))]
    async fn test_dao_list_by(db: DbPool) -> Result<(), TdError> {
        let list_params = ListParams::default();
        let mut query_builder = TEST_QUERIES.list_by::<TestDao>(&list_params, &())?;
        let query = query_builder.build_query_as();

        let query_str = query.sql();
        assert_eq!(
            query_str,
            "SELECT id, name, modified_on FROM test_table LIMIT ? OFFSET ?"
        );

        let result: Vec<TestDao> = query.fetch_all(&db).await.unwrap();
        assert_eq!(result, *FIXTURE_DAOS);
        Ok(())
    }

    #[td_test::test(sqlx(fixture = "test_queries"))]
    async fn test_dao_list_by_where(db: DbPool) -> Result<(), TdError> {
        let list_params = ListParams::default();
        let by = &(TestName::try_from("mario")?);
        let mut query_builder = TEST_QUERIES.list_by::<TestDao>(&list_params, &by)?;
        let query = query_builder.build_query_as();

        let query_str = query.sql();
        assert_eq!(
            query_str,
            "SELECT id, name, modified_on FROM test_table WHERE name = ? LIMIT ? OFFSET ?"
        );

        let result: Vec<TestDao> = query.fetch_all(&db).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], FIXTURE_DAOS[0]);
        Ok(())
    }

    #[td_test::test(sqlx(fixture = "test_queries"))]
    async fn test_dao_list_by_where_tuple(db: DbPool) -> Result<(), TdError> {
        let list_params = ListParams::default();
        let by = (
            &TestId::try_from("00000000000000000000000004")?,
            &TestName::try_from("mario")?,
        );
        let mut query_builder = TEST_QUERIES.list_by::<TestDao>(&list_params, &by)?;
        let query = query_builder.build_query_as();

        let query_str = query.sql();
        assert_eq!(
            query_str,
            "SELECT id, name, modified_on FROM test_table WHERE id = ? AND name = ? LIMIT ? OFFSET ?"
        );

        let result: Vec<TestDao> = query.fetch_all(&db).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], FIXTURE_DAOS[0]);
        Ok(())
    }

    #[td_test::test(sqlx(fixture = "test_queries"))]
    async fn test_dao_list_by_list_params(db: DbPool) -> Result<(), TdError> {
        let list_params = ListParamsBuilder::default()
            .offset(0usize)
            .len(0usize)
            .build()
            .unwrap();
        let mut query_builder = TEST_QUERIES.list_by::<TestDao>(&list_params, &())?;
        let query = query_builder.build_query_as();

        let query_str = query.sql();
        assert_eq!(
            query_str,
            "SELECT id, name, modified_on FROM test_table LIMIT ? OFFSET ?"
        );

        let result: Vec<TestDao> = query.fetch_all(&db).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], FIXTURE_DAOS[0]);
        Ok(())
    }

    #[Dao]
    struct UpdateDao {
        name: TestName,
    }

    #[td_test::test(sqlx(fixture = "test_queries"))]
    async fn test_dao_update_by(db: DbPool) -> Result<(), TdError> {
        let update_dao = UpdateDao::builder().try_name("peach")?.build()?;
        let mut query_builder = TEST_QUERIES.update_by::<_, TestDao>(&update_dao, &())?;
        let query = query_builder.build();

        let query_str = query.sql();
        assert_eq!(query_str, "UPDATE test_table SET name = ?");

        let result = query.execute(&db).await.unwrap();
        assert_eq!(result.rows_affected(), 2);

        // All rows names got changed.
        let db_data: Vec<TestDao> =
            sqlx::query_as("SELECT * FROM test_table WHERE name != 'peach'")
                .fetch_all(&db)
                .await
                .unwrap();
        assert_eq!(db_data.len(), 0);

        let db_data: Vec<TestDao> = sqlx::query_as("SELECT * FROM test_table WHERE name = 'peach'")
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
        assert_eq!(query_str, "UPDATE test_table SET name = ? WHERE name = ?");

        let result = query.execute(&db).await.unwrap();
        assert_eq!(result.rows_affected(), 1);

        // Only one row changed.
        let db_data: Vec<TestDao> =
            sqlx::query_as("SELECT * FROM test_table WHERE name != 'peach'")
                .fetch_all(&db)
                .await
                .unwrap();
        assert_eq!(db_data.len(), 1);

        let db_data: Vec<TestDao> = sqlx::query_as("SELECT * FROM test_table WHERE name = 'peach'")
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
            "UPDATE test_table SET name = ? WHERE id = ? AND name = ?"
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

        let db_data: Vec<TestDao> = sqlx::query_as("SELECT * FROM test_table WHERE name = 'peach'")
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
