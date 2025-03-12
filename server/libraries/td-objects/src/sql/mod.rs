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
pub mod dependency;
pub mod function;
pub mod permission;
pub mod roles;
pub mod table;
pub mod trigger;
pub mod users_roles;

use crate::types::{DataAccessObject, SqlEntity};
use getset::Getters;
use std::marker::PhantomData;
use td_database::sql::create_bindings_literal;
use td_error::td_error;
use tracing::trace;

#[td_error]
pub enum QueryError {
    #[error("Type not found: {0:?}")]
    TypeNotFound(String) = 5000,
}

// pub trait Queries<DB: sqlx::Database> we can do this to generalize the queries
pub trait Queries {}

#[rustfmt::skip]
macro_rules! all_the_tuples {
    ($name:ident) => {
        $name!([E1]);
        $name!([E1, E2]);
        $name!([E1, E2, E3]);
        $name!([E1, E2, E3, E4]);
        $name!([E1, E2, E3, E4, E5]);
        $name!([E1, E2, E3, E4, E5, E6]);
        $name!([E1, E2, E3, E4, E5, E6, E7]);
        $name!([E1, E2, E3, E4, E5, E6, E7, E8]);
        $name!([E1, E2, E3, E4, E5, E6, E7, E8, E9]);
        $name!([E1, E2, E3, E4, E5, E6, E7, E8, E9, E10]);
    };
}

pub trait Insert<'a> {
    fn insert<D: DataAccessObject>(
        &self,
        dao: &'a D,
    ) -> Result<sqlx::QueryBuilder<'a, sqlx::Sqlite>, QueryError>;
}

impl<'a, Q> Insert<'a> for Q
where
    Q: Queries,
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

pub trait SelectBy<'a, E> {
    fn select_by<D: DataAccessObject>(
        &self,
        e: &'a E,
    ) -> Result<sqlx::QueryBuilder<'a, sqlx::Sqlite>, QueryError>;
}

macro_rules! generate_select_by {
    (
        [$($E:ident),*]
    ) => {
        #[allow(non_snake_case, unused_parens)]
        impl<'a, Q, $($E),*> SelectBy<'a, ($($E),*) > for Q
        where
            Q: Queries,
            $($E: SqlEntity),*
        {
            fn select_by<D: DataAccessObject>(&self, ($($E),*): &'a ($($E),*)) -> Result<sqlx::QueryBuilder<'a, sqlx::Sqlite>, QueryError> {
                let table = D::sql_table();
                let fields = D::fields();
                let sql = format!("SELECT {} FROM {}", fields.join(", "), table);
                let mut query_builder = sqlx::QueryBuilder::new(sql);

                query_builder.push(" WHERE ");
                let mut separated = query_builder.separated(" AND ");
                $(
                    let column = D::sql_field_for_type::<$E>()
                        .ok_or(QueryError::TypeNotFound(std::any::type_name::<$E>().to_string()))?;
                    separated
                        .push(format!("{} = ", column))
                        .push_bind_unseparated($E.value());
                )*

                trace!("select_{}: sql: {}", table, query_builder.sql());
                Ok(query_builder)
            }
        }
    };
}

all_the_tuples!(generate_select_by);

pub trait DeleteBy<'a, E> {
    fn delete_by<D: DataAccessObject>(
        &self,
        e: &'a E,
    ) -> Result<sqlx::QueryBuilder<'a, sqlx::Sqlite>, QueryError>;
}

macro_rules! generate_delete_by {
    (
        [$($E:ident),*]
    ) => {
        #[allow(non_snake_case, unused_parens)]
        impl<'a, Q, $($E),*> DeleteBy<'a, ($($E),*) > for Q
        where
            Q: Queries,
            $($E: SqlEntity),*
        {
            fn delete_by<D: DataAccessObject>(&self, ($($E),*): &'a ($($E),*)) -> Result<sqlx::QueryBuilder<'a, sqlx::Sqlite>, QueryError> {
                let table = D::sql_table();
                let sql = format!("DELETE FROM {} ", table);
                let mut query_builder = sqlx::QueryBuilder::new(sql);

                query_builder.push(" WHERE ");
                let mut separated = query_builder.separated(" AND ");
                $(
                    let column = D::sql_field_for_type::<$E>()
                        .ok_or(QueryError::TypeNotFound(std::any::type_name::<$E>().to_string()))?;
                    separated
                        .push(format!("{} = ", column))
                        .push_bind_unseparated($E.value());
                )*

                trace!("delete_{}: sql: {}", table, query_builder.sql());
                Ok(query_builder)
            }
        }
    };
}

all_the_tuples!(generate_delete_by);

/// A SQL statement with parameters created by query functions.
#[derive(Debug, Clone, Getters)]
#[getset(get = "pub")]
pub struct Statement {
    /// The parameterized SQL statement.
    sql: String,
    /// The parameter names ordered by their position in the parameterized SQL.
    params: Vec<String>,
}

impl Statement {
    pub fn new<S, V>(sql: S, params: V) -> Self
    where
        S: AsRef<str>,
        V: IntoIterator,
        V::Item: AsRef<str>,
    {
        Self {
            sql: sql.as_ref().to_string(),
            params: params.into_iter().map(|s| s.as_ref().to_string()).collect(),
        }
    }
}

/// A typed column to use in query function conditions
///
/// Note: Use the variant constructors to create Which instances as they get typed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Which<T> {
    All(PhantomData<T>) = 0,
    One(PhantomData<T>) = 1,
    Like(PhantomData<T>) = 2,
    Set(usize, PhantomData<T>) = 3,
}

impl<T> Which<T> {
    /// It means the query is not filtered by this column.
    pub fn all() -> Self {
        Which::All(PhantomData)
    }

    /// It means the query is filtered by this column with a single value.
    pub fn one() -> Self {
        Which::One(PhantomData)
    }

    /// It means the query is filtered by this column with a set of values (an SQL IN clause).
    pub fn set(n: usize) -> Self {
        Which::Set(n, PhantomData)
    }
}

impl<T> Which<T> {
    //TODO: <T> should implement a SqlLikeType marker trait to retrict use o like() to typed strings.

    /// It means the query is filtered by this column with a LIKE value expression.
    pub fn like() -> Self {
        Which::Like(PhantomData)
    }
}

/// Used internally by query functions to build conditions for WHERE clauses.
#[derive(Debug)]
struct Condition {
    expr: Option<String>,
    params: Vec<String>,
    param_offset: usize,
}

impl Condition {
    fn new(expr: Option<String>, params: Vec<String>, param_offset: usize) -> Self {
        Condition {
            expr,
            params,
            param_offset,
        }
    }
}

fn condition_builder<T>(
    table_alias: Option<&str>,
    column: &str,
    param_offset: usize,
    values: &Which<T>,
) -> Condition {
    let table_alias = table_alias
        .map(|alias| format!("{}.", alias))
        .unwrap_or_default();
    match values {
        Which::All(_) => Condition::new(None, vec![], param_offset),
        Which::One(_) => {
            let eq_condition = format!("{table_alias}{column} = ?{}", param_offset + 1);
            Condition::new(
                Some(eq_condition),
                vec![column.to_string()],
                param_offset + 1,
            )
        }
        Which::Like(_) => {
            let like_condition = format!("{table_alias}{column} LIKE ?{}", param_offset + 1);
            Condition::new(
                Some(like_condition),
                vec![column.to_string()],
                param_offset + 1,
            )
        }
        Which::Set(n, _) => {
            let in_condition = format!(
                "{table_alias}{column} IN ({})",
                create_bindings_literal(param_offset, *n)
            );
            let mut params = vec![];
            for i in 0..*n {
                params.push(format!("{column}#{i}"));
            }
            Condition::new(Some(in_condition), params, param_offset + n)
        }
    }
}

/// To indicate if the query is for a table or its `_with_names` view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum With {
    Ids = 0,
    Names = 1,
}

impl With {
    /// Returns the name of the table or its __with_names view based on the variant.
    fn table_name(&self, table: &str) -> String {
        match self {
            With::Ids => table.to_string(),
            With::Names => format!("{}__with_names", table),
        }
    }
}

/// To indicate the columns in the returned result set.
pub enum Columns<'a> {
    All,
    One(&'a str),
    Some(&'a [&'a str]),
    Dyn(&'a Vec<String>),
}

fn select_cols(columns: &Columns) -> String {
    match columns {
        Columns::All => "*".to_string(),
        Columns::One(column) => column.to_string(),
        Columns::Some(columns) => columns.join(", "),
        Columns::Dyn(columns) => columns.join(", "),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_which() {
        use super::Which;
        assert_eq!(Which::<String>::all(), Which::All(std::marker::PhantomData));
        assert_eq!(Which::<String>::one(), Which::One(std::marker::PhantomData));
        assert_eq!(
            Which::<String>::like(),
            Which::Like(std::marker::PhantomData)
        );
        assert_eq!(
            Which::<String>::set(3),
            Which::Set(3, std::marker::PhantomData)
        );
    }
    #[test]
    fn test_condition_builder() {
        use super::{condition_builder, Which};
        let condition = condition_builder::<String>(Some("t"), "name", 0, &Which::all());
        assert_eq!(condition.expr, None);
        assert_eq!(condition.params, Vec::<String>::new());
        assert_eq!(condition.param_offset, 0);
        let condition = condition_builder::<String>(Some("t"), "name", 0, &Which::one());
        assert_eq!(condition.expr, Some("t.name = ?1".to_string()));
        assert_eq!(condition.params, vec!["name".to_string()]);
        assert_eq!(condition.param_offset, 1);
        let condition = condition_builder::<String>(None, "name", 0, &Which::like());
        assert_eq!(condition.expr, Some("name LIKE ?1".to_string()));
        assert_eq!(condition.params, vec!["name".to_string()]);
        assert_eq!(condition.param_offset, 1);
        let condition = condition_builder::<String>(Some("t"), "name", 0, &Which::set(3));
        assert_eq!(condition.expr, Some("t.name IN (?1,?2,?3)".to_string()));
        assert_eq!(
            condition.params,
            vec![
                "name#0".to_string(),
                "name#1".to_string(),
                "name#2".to_string()
            ]
        );
        assert_eq!(condition.param_offset, 3);
    }

    #[test]
    fn test_select_cols() {
        use super::Columns;
        assert_eq!(super::select_cols(&Columns::All), "*");
        assert_eq!(super::select_cols(&Columns::One("id")), "id");
        assert_eq!(
            super::select_cols(&Columns::Some(&["id", "name"])),
            "id, name"
        );
        assert_eq!(
            super::select_cols(&Columns::Dyn(&vec!["id".to_string(), "name".to_string()])),
            "id, name"
        );
    }
}
