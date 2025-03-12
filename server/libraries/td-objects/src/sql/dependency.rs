//
// Copyright 2025 Tabs Data Inc.
//

use crate::sql::{condition_builder, select_cols, Columns, Statement, Which, With};
use crate::types::basic::{CollectionId, FunctionId};
use tracing::trace;

/// Function Queries.
pub struct Queries {}

impl Queries {
    /// Constructor.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {}
    }

    /// Select current function(s) dependencies.
    pub fn select_dependencies_current(
        self,
        select: &Columns,
        collections: &Which<CollectionId>,
        functions: &Which<FunctionId>,
        with: &With,
    ) -> Statement {
        let select_columns = select_cols(select);
        let table = with.table_name("dependencies");

        let mut sql = format!("SELECT {select_columns} FROM {table}");
        let mut params = vec![];
        let mut conditions = 0;

        let condition = condition_builder(None, "collection_id", conditions, collections);
        if let Some(expr) = condition.expr {
            sql += &format!(" WHERE {}", expr);
            params.extend(condition.params);
            conditions = condition.param_offset;
        }

        let connector = if conditions > 0 { "AND" } else { "WHERE" };

        let condition = condition_builder(None, "function_id", conditions, functions);
        if let Some(expr) = condition.expr {
            sql += &format!(" {connector} {expr}");
            params.extend(condition.params);
        }

        trace!("select_current_dependencies: sql: {}", sql);
        Statement { sql, params }
    }

    /// Select to get existing function(s) dependencies at a particular time.
    ///
    /// The statement's first parameter is the time at which to select the dependencies.
    ///
    /// IMPORTANT: provided [`FunctionId`] must be valid at the `at_time` to be used,
    /// use the [`super::function::Queries`] to get the function(s) at the time.
    pub fn select_dependencies_at_time(
        self,
        select: &Columns,
        collections: &Which<CollectionId>,
        functions: &Which<FunctionId>,
        with: &With,
    ) -> Statement {
        let select_columns = select_cols(select);
        let table = with.table_name("dependency_versions");

        let mut conditions = vec!["dv.defined_on <= ?1".to_string()];
        let mut params = vec!["at_time".to_string()];
        let mut param_count = 1;

        let condition = condition_builder(Some("dv"), "collection_id", param_count, collections);
        params.extend(condition.params);
        param_count = condition.param_offset;
        let collection_condition = condition.expr;
        if let Some(c) = collection_condition {
            conditions.push(c)
        }

        let condition = condition_builder(Some("dv"), "function_id", param_count, functions);
        params.extend(condition.params);
        let function_condition = condition.expr;
        if let Some(c) = function_condition {
            conditions.push(c)
        }

        let where_ = conditions.join(" AND ");

        let sql = format!(
            r#"
            SELECT {select_columns} FROM {table}
                WHERE
                    id IN (
                        SELECT MAX(dv.id) FROM dependency_versions dv
                            WHERE {where_}
                            GROUP BY dv.dependency_id
                    )
                    AND
                    status != 'D'
        "#
        );

        trace!("select_dependencies_at_time: sql: {}", sql);
        Statement { sql, params }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::Which;
    use sqlx::types::chrono;
    use td_database::test_utils;

    #[test]
    fn test_select_current_dependencies_table_view() {
        let statement = Queries::new().select_dependencies_current(
            &Columns::All,
            &Which::all(),
            &Which::all(),
            &With::Ids,
        );
        assert_eq!(statement.sql(), "SELECT * FROM dependencies");
        assert_eq!(statement.params(), &Vec::<String>::new());

        let statement = Queries::new().select_dependencies_current(
            &Columns::All,
            &Which::all(),
            &Which::all(),
            &With::Names,
        );
        assert_eq!(statement.sql(), "SELECT * FROM dependencies__with_names");
        assert_eq!(statement.params(), &Vec::<String>::new());
    }

    #[test]
    fn test_select_current_dependencies_columns() {
        let statement = Queries::new().select_dependencies_current(
            &Columns::All,
            &Which::all(),
            &Which::all(),
            &With::Ids,
        );
        assert_eq!(statement.sql(), "SELECT * FROM dependencies");
        assert_eq!(statement.params(), &Vec::<String>::new());

        let statement = Queries::new().select_dependencies_current(
            &Columns::One("id"),
            &Which::all(),
            &Which::all(),
            &With::Ids,
        );
        assert_eq!(statement.sql(), "SELECT id FROM dependencies");
        assert_eq!(statement.params(), &Vec::<String>::new());

        let statement = Queries::new().select_dependencies_current(
            &Columns::Some(&["id", "table_id"]),
            &Which::all(),
            &Which::all(),
            &With::Ids,
        );
        assert_eq!(statement.sql(), "SELECT id, table_id FROM dependencies");
        assert_eq!(statement.params(), &Vec::<String>::new());

        let statement = Queries::new().select_dependencies_current(
            &Columns::Dyn(&vec!["id".to_string(), "table_id".to_string()]),
            &Which::all(),
            &Which::all(),
            &With::Ids,
        );
        assert_eq!(statement.sql(), "SELECT id, table_id FROM dependencies");
        assert_eq!(statement.params(), &Vec::<String>::new());
    }

    #[test]
    fn test_select_current_dependencies() {
        let statement = Queries::new().select_dependencies_current(
            &Columns::All,
            &Which::all(),
            &Which::all(),
            &With::Ids,
        );
        assert_eq!(statement.sql(), "SELECT * FROM dependencies");
        assert_eq!(statement.params(), &Vec::<String>::new());
    }

    #[test]
    fn test_select_current_dependencies_collections() {
        let statement = Queries::new().select_dependencies_current(
            &Columns::All,
            &Which::one(),
            &Which::all(),
            &With::Ids,
        );
        assert_eq!(
            statement.sql,
            "SELECT * FROM dependencies WHERE collection_id = ?1"
        );
        assert_eq!(statement.params, vec!["collection_id".to_string()]);

        let statement = Queries::new().select_dependencies_current(
            &Columns::All,
            &Which::set(3),
            &Which::all(),
            &With::Ids,
        );
        assert_eq!(
            statement.sql,
            "SELECT * FROM dependencies WHERE collection_id IN (?1,?2,?3)"
        );
        assert_eq!(
            statement.params,
            vec![
                "collection_id#0".to_string(),
                "collection_id#1".to_string(),
                "collection_id#2".to_string(),
            ]
        );
    }

    #[test]
    fn test_select_current_dependencies_functions() {
        let statement = Queries::new().select_dependencies_current(
            &Columns::All,
            &Which::all(),
            &Which::one(),
            &With::Ids,
        );
        assert_eq!(
            statement.sql,
            "SELECT * FROM dependencies WHERE function_id = ?1"
        );
        assert_eq!(statement.params, vec!["function_id".to_string()]);

        let statement = Queries::new().select_dependencies_current(
            &Columns::All,
            &Which::all(),
            &Which::set(2),
            &With::Ids,
        );
        assert_eq!(
            statement.sql,
            "SELECT * FROM dependencies WHERE function_id IN (?1,?2)"
        );
        assert_eq!(
            statement.params,
            vec!["function_id#0".to_string(), "function_id#1".to_string()]
        );
    }

    #[test]
    fn test_select_current_dependencies_collections_functions() {
        let statement = Queries::new().select_dependencies_current(
            &Columns::All,
            &Which::one(),
            &Which::one(),
            &With::Ids,
        );
        assert_eq!(
            statement.sql,
            "SELECT * FROM dependencies WHERE collection_id = ?1 AND function_id = ?2"
        );
        assert_eq!(
            statement.params,
            vec!["collection_id".to_string(), "function_id".to_string()]
        );

        let statement = Queries::new().select_dependencies_current(
            &Columns::All,
            &Which::set(3),
            &Which::set(2),
            &With::Ids,
        );
        assert_eq!(
            statement.sql,
            "SELECT * FROM dependencies WHERE collection_id IN (?1,?2,?3) AND function_id IN (?4,?5)"
        );
        assert_eq!(
            statement.params,
            vec![
                "collection_id#0".to_string(),
                "collection_id#1".to_string(),
                "collection_id#2".to_string(),
                "function_id#0".to_string(),
                "function_id#1".to_string()
            ]
        );
    }

    //-----

    #[test]
    fn test_select_functions_at_time_from_table_view() {
        let statement = Queries::new().select_dependencies_at_time(
            &Columns::All,
            &Which::all(),
            &Which::all(),
            &With::Ids,
        );
        assert_eq!(
            statement.sql().trim(),
            r#"
        SELECT * FROM dependency_versions
                WHERE
                    id IN (
                        SELECT MAX(dv.id) FROM dependency_versions dv
                            WHERE dv.defined_on <= ?1
                            GROUP BY dv.dependency_id
                    )
                    AND
                    status != 'D'
        "#
            .trim()
        );
        assert_eq!(statement.params(), &["at_time".to_string()]);

        let statement = Queries::new().select_dependencies_at_time(
            &Columns::All,
            &Which::all(),
            &Which::all(),
            &With::Names,
        );
        assert_eq!(
            statement.sql().trim(),
            r#"
        SELECT * FROM dependency_versions__with_names
                WHERE
                    id IN (
                        SELECT MAX(dv.id) FROM dependency_versions dv
                            WHERE dv.defined_on <= ?1
                            GROUP BY dv.dependency_id
                    )
                    AND
                    status != 'D'
        "#
            .trim()
        );
        assert_eq!(statement.params(), &["at_time".to_string()]);
    }

    #[test]
    fn test_select_functions_at_time_columns() {
        let statement = Queries::new().select_dependencies_at_time(
            &Columns::All,
            &Which::all(),
            &Which::all(),
            &With::Ids,
        );
        assert_eq!(
            statement.sql().trim(),
            r#"
        SELECT * FROM dependency_versions
                WHERE
                    id IN (
                        SELECT MAX(dv.id) FROM dependency_versions dv
                            WHERE dv.defined_on <= ?1
                            GROUP BY dv.dependency_id
                    )
                    AND
                    status != 'D'
        "#
            .trim()
        );
        assert_eq!(statement.params(), &["at_time".to_string()]);

        let statement = Queries::new().select_dependencies_at_time(
            &Columns::One("id"),
            &Which::all(),
            &Which::all(),
            &With::Ids,
        );
        assert_eq!(
            statement.sql().trim(),
            r#"
        SELECT id FROM dependency_versions
                WHERE
                    id IN (
                        SELECT MAX(dv.id) FROM dependency_versions dv
                            WHERE dv.defined_on <= ?1
                            GROUP BY dv.dependency_id
                    )
                    AND
                    status != 'D'
        "#
            .trim()
        );
        assert_eq!(statement.params(), &["at_time".to_string()]);

        let statement = Queries::new().select_dependencies_at_time(
            &Columns::Some(&["id", "table_id"]),
            &Which::all(),
            &Which::all(),
            &With::Ids,
        );
        assert_eq!(
            statement.sql().trim(),
            r#"
        SELECT id, table_id FROM dependency_versions
                WHERE
                    id IN (
                        SELECT MAX(dv.id) FROM dependency_versions dv
                            WHERE dv.defined_on <= ?1
                            GROUP BY dv.dependency_id
                    )
                    AND
                    status != 'D'
        "#
            .trim()
        );
        assert_eq!(statement.params(), &["at_time".to_string()]);

        let statement = Queries::new().select_dependencies_at_time(
            &Columns::Dyn(&vec!["id".to_string(), "table_id".to_string()]),
            &Which::all(),
            &Which::all(),
            &With::Ids,
        );
        assert_eq!(
            statement.sql().trim(),
            r#"
        SELECT id, table_id FROM dependency_versions
                WHERE
                    id IN (
                        SELECT MAX(dv.id) FROM dependency_versions dv
                            WHERE dv.defined_on <= ?1
                            GROUP BY dv.dependency_id
                    )
                    AND
                    status != 'D'
        "#
            .trim()
        );
        assert_eq!(statement.params(), &["at_time".to_string()]);
    }

    #[test]
    fn test_select_functions_at_time() {
        let statement = Queries::new().select_dependencies_at_time(
            &Columns::All,
            &Which::all(),
            &Which::all(),
            &With::Ids,
        );
        assert_eq!(
            statement.sql().trim(),
            r#"
        SELECT * FROM dependency_versions
                WHERE
                    id IN (
                        SELECT MAX(dv.id) FROM dependency_versions dv
                            WHERE dv.defined_on <= ?1
                            GROUP BY dv.dependency_id
                    )
                    AND
                    status != 'D'
        "#
            .trim()
        );
        assert_eq!(statement.params(), &["at_time".to_string()]);
    }

    #[test]
    fn test_select_functions_at_time_collections() {
        let statement = Queries::new().select_dependencies_at_time(
            &Columns::All,
            &Which::one(),
            &Which::all(),
            &With::Ids,
        );
        assert_eq!(
            statement.sql().trim(),
            r#"
        SELECT * FROM dependency_versions
                WHERE
                    id IN (
                        SELECT MAX(dv.id) FROM dependency_versions dv
                            WHERE dv.defined_on <= ?1 AND dv.collection_id = ?2
                            GROUP BY dv.dependency_id
                    )
                    AND
                    status != 'D'
        "#
            .trim()
        );
        assert_eq!(
            statement.params(),
            &["at_time".to_string(), "collection_id".to_string()]
        );

        let statement = Queries::new().select_dependencies_at_time(
            &Columns::All,
            &Which::set(2),
            &Which::all(),
            &With::Ids,
        );
        assert_eq!(
            statement.sql().trim(),
            r#"
        SELECT * FROM dependency_versions
                WHERE
                    id IN (
                        SELECT MAX(dv.id) FROM dependency_versions dv
                            WHERE dv.defined_on <= ?1 AND dv.collection_id IN (?2,?3)
                            GROUP BY dv.dependency_id
                    )
                    AND
                    status != 'D'
        "#
            .trim()
        );
        assert_eq!(
            statement.params(),
            &[
                "at_time".to_string(),
                "collection_id#0".to_string(),
                "collection_id#1".to_string()
            ]
        );
    }

    #[test]
    fn test_select_functions_at_time_functions() {
        let statement = Queries::new().select_dependencies_at_time(
            &Columns::All,
            &Which::all(),
            &Which::one(),
            &With::Ids,
        );
        assert_eq!(
            statement.sql().trim(),
            r#"
        SELECT * FROM dependency_versions
                WHERE
                    id IN (
                        SELECT MAX(dv.id) FROM dependency_versions dv
                            WHERE dv.defined_on <= ?1 AND dv.function_id = ?2
                            GROUP BY dv.dependency_id
                    )
                    AND
                    status != 'D'
        "#
            .trim()
        );
        assert_eq!(
            statement.params(),
            &["at_time".to_string(), "function_id".to_string()]
        );

        let statement = Queries::new().select_dependencies_at_time(
            &Columns::All,
            &Which::all(),
            &Which::set(2),
            &With::Ids,
        );
        assert_eq!(
            statement.sql().trim(),
            r#"
        SELECT * FROM dependency_versions
                WHERE
                    id IN (
                        SELECT MAX(dv.id) FROM dependency_versions dv
                            WHERE dv.defined_on <= ?1 AND dv.function_id IN (?2,?3)
                            GROUP BY dv.dependency_id
                    )
                    AND
                    status != 'D'
        "#
            .trim()
        );
        assert_eq!(
            statement.params(),
            &[
                "at_time".to_string(),
                "function_id#0".to_string(),
                "function_id#1".to_string()
            ]
        );
    }

    #[test]
    fn test_select_functions_at_time_collections_functions() {
        let statement = Queries::new().select_dependencies_at_time(
            &Columns::All,
            &Which::one(),
            &Which::one(),
            &With::Ids,
        );
        assert_eq!(
            statement.sql().trim(),
            r#"
        SELECT * FROM dependency_versions
                WHERE
                    id IN (
                        SELECT MAX(dv.id) FROM dependency_versions dv
                            WHERE dv.defined_on <= ?1 AND dv.collection_id = ?2 AND dv.function_id = ?3
                            GROUP BY dv.dependency_id
                    )
                    AND
                    status != 'D'
        "#
            .trim()
        );
        assert_eq!(
            statement.params(),
            &[
                "at_time".to_string(),
                "collection_id".to_string(),
                "function_id".to_string(),
            ]
        );
    }

    //-----

    #[tokio::test]
    async fn test_sql_syntax() {
        let db = test_utils::db().await.unwrap();
        let mut conn = db.acquire().await.unwrap();

        let queries = vec![
            (
                false,
                Queries::new().select_dependencies_current(
                    &Columns::All,
                    &Which::all(),
                    &Which::all(),
                    &With::Ids,
                ),
            ),
            (
                false,
                Queries::new().select_dependencies_current(
                    &Columns::All,
                    &Which::all(),
                    &Which::all(),
                    &With::Names,
                ),
            ),
            (
                false,
                Queries::new().select_dependencies_current(
                    &Columns::One("id"),
                    &Which::all(),
                    &Which::all(),
                    &With::Ids,
                ),
            ),
            (
                false,
                Queries::new().select_dependencies_current(
                    &Columns::Some(&["id"]),
                    &Which::all(),
                    &Which::all(),
                    &With::Ids,
                ),
            ),
            (
                false,
                Queries::new().select_dependencies_current(
                    &Columns::All,
                    &Which::one(),
                    &Which::all(),
                    &With::Ids,
                ),
            ),
            (
                false,
                Queries::new().select_dependencies_current(
                    &Columns::All,
                    &Which::set(1),
                    &Which::all(),
                    &With::Ids,
                ),
            ),
            (
                false,
                Queries::new().select_dependencies_current(
                    &Columns::All,
                    &Which::set(2),
                    &Which::all(),
                    &With::Ids,
                ),
            ),
            (
                false,
                Queries::new().select_dependencies_current(
                    &Columns::All,
                    &Which::all(),
                    &Which::one(),
                    &With::Ids,
                ),
            ),
            (
                false,
                Queries::new().select_dependencies_current(
                    &Columns::All,
                    &Which::all(),
                    &Which::set(1),
                    &With::Ids,
                ),
            ),
            (
                false,
                Queries::new().select_dependencies_current(
                    &Columns::All,
                    &Which::all(),
                    &Which::set(2),
                    &With::Ids,
                ),
            ),
            (
                false,
                Queries::new().select_dependencies_current(
                    &Columns::All,
                    &Which::one(),
                    &Which::one(),
                    &With::Ids,
                ),
            ),
            (
                true,
                Queries::new().select_dependencies_at_time(
                    &Columns::All,
                    &Which::all(),
                    &Which::all(),
                    &With::Ids,
                ),
            ),
            (
                true,
                Queries::new().select_dependencies_at_time(
                    &Columns::All,
                    &Which::all(),
                    &Which::all(),
                    &With::Names,
                ),
            ),
            (
                true,
                Queries::new().select_dependencies_at_time(
                    &Columns::One("id"),
                    &Which::all(),
                    &Which::all(),
                    &With::Ids,
                ),
            ),
            (
                true,
                Queries::new().select_dependencies_at_time(
                    &Columns::Some(&["id"]),
                    &Which::all(),
                    &Which::all(),
                    &With::Ids,
                ),
            ),
            (
                true,
                Queries::new().select_dependencies_at_time(
                    &Columns::All,
                    &Which::one(),
                    &Which::all(),
                    &With::Ids,
                ),
            ),
            (
                true,
                Queries::new().select_dependencies_at_time(
                    &Columns::All,
                    &Which::set(1),
                    &Which::all(),
                    &With::Ids,
                ),
            ),
            (
                true,
                Queries::new().select_dependencies_at_time(
                    &Columns::All,
                    &Which::set(2),
                    &Which::all(),
                    &With::Ids,
                ),
            ),
            (
                true,
                Queries::new().select_dependencies_at_time(
                    &Columns::All,
                    &Which::all(),
                    &Which::all(),
                    &With::Ids,
                ),
            ),
            (
                true,
                Queries::new().select_dependencies_at_time(
                    &Columns::All,
                    &Which::all(),
                    &Which::set(1),
                    &With::Ids,
                ),
            ),
            (
                true,
                Queries::new().select_dependencies_at_time(
                    &Columns::All,
                    &Which::all(),
                    &Which::set(2),
                    &With::Ids,
                ),
            ),
            (
                true,
                Queries::new().select_dependencies_at_time(
                    &Columns::All,
                    &Which::one(),
                    &Which::one(),
                    &With::Ids,
                ),
            ),
        ];

        for (first_param_is_time, statement) in queries {
            let mut query = sqlx::query(statement.sql());
            if first_param_is_time {
                query = query.bind(chrono::Utc::now().to_utc());
            }
            for _ in 0..statement.params().len() {
                query = query.bind("dummy".to_string());
            }
            assert!(
                query.fetch_all(&mut *conn).await.is_ok(),
                "failed on statement: {:?}",
                statement
            );
        }
    }
}
