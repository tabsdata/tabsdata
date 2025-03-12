//
// Copyright 2025 Tabs Data Inc.
//

use crate::sql::{condition_builder, select_cols, Columns, Statement, Which, With};
use crate::types::basic::{CollectionId, TableName};
use tracing::trace;

/// Table Queries.
pub struct Queries {}

impl Queries {
    /// Constructor.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {}
    }

    /// Select current functions.
    pub fn select_tables_current(
        self,
        select: &Columns,
        collections: &Which<CollectionId>,
        tables: &Which<TableName>,
        with: &With,
    ) -> Statement {
        let select_columns = select_cols(select);
        let table = with.table_name("tables");

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

        let condition = condition_builder(None, "name", conditions, tables);
        if let Some(expr) = condition.expr {
            sql += &format!(" {connector} {expr}");
            params.extend(condition.params);
        }

        trace!("select_current_tables: sql: {}", sql);
        Statement { sql, params }
    }

    /// Select to get existing tables at a particular time.
    ///
    /// The statement's first parameter is the time at which to select the tables.
    pub fn select_tables_at_time(
        self,
        select: &Columns,
        collections: &Which<CollectionId>,
        tables: &Which<TableName>,
        with: &With,
    ) -> Statement {
        let select_columns = select_cols(select);
        let table = with.table_name("table_versions");

        let mut conditions = vec!["tv.defined_on <= ?1".to_string()];
        let mut params = vec!["at_time".to_string()];
        let mut param_count = 1;

        let condition = condition_builder(Some("tv"), "collection_id", param_count, collections);
        params.extend(condition.params);
        param_count = condition.param_offset;
        let collection_condition = condition.expr;
        if let Some(c) = collection_condition {
            conditions.push(c)
        }

        let condition = condition_builder(Some("tv"), "name", param_count, tables);
        params.extend(condition.params);
        let table_condition = condition.expr;
        if let Some(c) = table_condition {
            conditions.push(c)
        }

        let where_ = conditions.join(" AND ");

        let sql = format!(
            r#"
            SELECT {select_columns} FROM {table}
                WHERE
                    id IN (
                        SELECT MAX(tv.id) FROM table_versions tv
                            WHERE {where_}
                            GROUP BY tv.table_id
                    )
                    AND
                    status != 'D'
        "#
        );
        trace!("select_tables_at_time: sql: {}", sql);
        Statement { sql, params }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::Which;
    use sqlx::types::chrono;
    use td_database::test_utils::db;

    #[test]
    fn test_select_current_tables_table_view() {
        let statement = Queries::new().select_tables_current(
            &Columns::All,
            &Which::all(),
            &Which::all(),
            &With::Ids,
        );
        assert_eq!(statement.sql(), "SELECT * FROM tables");
        assert_eq!(statement.params(), &Vec::<String>::new());

        let statement = Queries::new().select_tables_current(
            &Columns::All,
            &Which::all(),
            &Which::all(),
            &With::Names,
        );
        assert_eq!(statement.sql(), "SELECT * FROM tables__with_names");
        assert_eq!(statement.params(), &Vec::<String>::new());
    }

    #[test]
    fn test_select_current_tables_columns() {
        let statement = Queries::new().select_tables_current(
            &Columns::All,
            &Which::all(),
            &Which::all(),
            &With::Ids,
        );
        assert_eq!(statement.sql(), "SELECT * FROM tables");
        assert_eq!(statement.params(), &Vec::<String>::new());

        let statement = Queries::new().select_tables_current(
            &Columns::One("id"),
            &Which::all(),
            &Which::all(),
            &With::Ids,
        );
        assert_eq!(statement.sql(), "SELECT id FROM tables");
        assert_eq!(statement.params(), &Vec::<String>::new());

        let statement = Queries::new().select_tables_current(
            &Columns::Some(&["id", "name"]),
            &Which::all(),
            &Which::all(),
            &With::Ids,
        );
        assert_eq!(statement.sql(), "SELECT id, name FROM tables");
        assert_eq!(statement.params(), &Vec::<String>::new());

        let statement = Queries::new().select_tables_current(
            &Columns::Dyn(&vec!["id".to_string(), "name".to_string()]),
            &Which::all(),
            &Which::all(),
            &With::Ids,
        );
        assert_eq!(statement.sql(), "SELECT id, name FROM tables");
        assert_eq!(statement.params(), &Vec::<String>::new());
    }

    #[test]
    fn test_select_current_tables() {
        let statement = Queries::new().select_tables_current(
            &Columns::All,
            &Which::all(),
            &Which::all(),
            &With::Ids,
        );
        assert_eq!(statement.sql(), "SELECT * FROM tables");
        assert_eq!(statement.params(), &Vec::<String>::new());
    }

    #[test]
    fn test_select_current_tables_collections() {
        let statement = Queries::new().select_tables_current(
            &Columns::All,
            &Which::one(),
            &Which::all(),
            &With::Ids,
        );
        assert_eq!(
            statement.sql,
            "SELECT * FROM tables WHERE collection_id = ?1"
        );
        assert_eq!(statement.params, vec!["collection_id".to_string()]);

        let statement = Queries::new().select_tables_current(
            &Columns::All,
            &Which::set(3),
            &Which::all(),
            &With::Ids,
        );
        assert_eq!(
            statement.sql,
            "SELECT * FROM tables WHERE collection_id IN (?1,?2,?3)"
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
    fn test_select_current_tables_functions() {
        let statement = Queries::new().select_tables_current(
            &Columns::All,
            &Which::all(),
            &Which::one(),
            &With::Ids,
        );
        assert_eq!(statement.sql, "SELECT * FROM tables WHERE name = ?1");
        assert_eq!(statement.params, vec!["name".to_string()]);

        let statement = Queries::new().select_tables_current(
            &Columns::All,
            &Which::all(),
            &Which::set(2),
            &With::Ids,
        );
        assert_eq!(statement.sql, "SELECT * FROM tables WHERE name IN (?1,?2)");
        assert_eq!(
            statement.params,
            vec!["name#0".to_string(), "name#1".to_string()]
        );

        let statement = Queries::new().select_tables_current(
            &Columns::All,
            &Which::all(),
            &Which::like(),
            &With::Ids,
        );
        assert_eq!(statement.sql, "SELECT * FROM tables WHERE name LIKE ?1");
        assert_eq!(statement.params, vec!["name".to_string()]);
    }

    #[test]
    fn test_select_current_tables_collections_functions() {
        let statement = Queries::new().select_tables_current(
            &Columns::All,
            &Which::one(),
            &Which::one(),
            &With::Ids,
        );
        assert_eq!(
            statement.sql,
            "SELECT * FROM tables WHERE collection_id = ?1 AND name = ?2"
        );
        assert_eq!(
            statement.params,
            vec!["collection_id".to_string(), "name".to_string()]
        );

        let statement = Queries::new().select_tables_current(
            &Columns::All,
            &Which::set(3),
            &Which::set(2),
            &With::Ids,
        );
        assert_eq!(
            statement.sql,
            "SELECT * FROM tables WHERE collection_id IN (?1,?2,?3) AND name IN (?4,?5)"
        );
        assert_eq!(
            statement.params,
            vec![
                "collection_id#0".to_string(),
                "collection_id#1".to_string(),
                "collection_id#2".to_string(),
                "name#0".to_string(),
                "name#1".to_string()
            ]
        );
    }

    //-----

    #[test]
    fn test_select_tables_at_time_from_table() {
        let statement = Queries::new().select_tables_at_time(
            &Columns::All,
            &Which::all(),
            &Which::all(),
            &With::Ids,
        );
        assert_eq!(
            statement.sql().trim(),
            r#"
        SELECT * FROM table_versions
                WHERE
                    id IN (
                        SELECT MAX(tv.id) FROM table_versions tv
                            WHERE tv.defined_on <= ?1
                            GROUP BY tv.table_id
                    )
                    AND
                    status != 'D'
        "#
            .trim()
        );
        assert_eq!(statement.params(), &["at_time".to_string()]);

        let statement = Queries::new().select_tables_at_time(
            &Columns::All,
            &Which::all(),
            &Which::all(),
            &With::Names,
        );
        assert_eq!(
            statement.sql().trim(),
            r#"
        SELECT * FROM table_versions__with_names
                WHERE
                    id IN (
                        SELECT MAX(tv.id) FROM table_versions tv
                            WHERE tv.defined_on <= ?1
                            GROUP BY tv.table_id
                    )
                    AND
                    status != 'D'
        "#
            .trim()
        );
        assert_eq!(statement.params(), &["at_time".to_string()]);
    }

    #[test]
    fn test_select_tables_at_time_columns() {
        let statement = Queries::new().select_tables_at_time(
            &Columns::All,
            &Which::all(),
            &Which::all(),
            &With::Ids,
        );
        assert_eq!(
            statement.sql().trim(),
            r#"
        SELECT * FROM table_versions
                WHERE
                    id IN (
                        SELECT MAX(tv.id) FROM table_versions tv
                            WHERE tv.defined_on <= ?1
                            GROUP BY tv.table_id
                    )
                    AND
                    status != 'D'
        "#
            .trim()
        );
        assert_eq!(statement.params(), &["at_time".to_string()]);

        let statement = Queries::new().select_tables_at_time(
            &Columns::One("id"),
            &Which::all(),
            &Which::all(),
            &With::Ids,
        );
        assert_eq!(
            statement.sql().trim(),
            r#"
        SELECT id FROM table_versions
                WHERE
                    id IN (
                        SELECT MAX(tv.id) FROM table_versions tv
                            WHERE tv.defined_on <= ?1
                            GROUP BY tv.table_id
                    )
                    AND
                    status != 'D'
        "#
            .trim()
        );
        assert_eq!(statement.params(), &["at_time".to_string()]);

        let statement = Queries::new().select_tables_at_time(
            &Columns::Some(&["id", "name"]),
            &Which::all(),
            &Which::all(),
            &With::Ids,
        );
        assert_eq!(
            statement.sql().trim(),
            r#"
        SELECT id, name FROM table_versions
                WHERE
                    id IN (
                        SELECT MAX(tv.id) FROM table_versions tv
                            WHERE tv.defined_on <= ?1
                            GROUP BY tv.table_id
                    )
                    AND
                    status != 'D'
        "#
            .trim()
        );
        assert_eq!(statement.params(), &["at_time".to_string()]);

        let statement = Queries::new().select_tables_at_time(
            &Columns::Dyn(&vec!["id".to_string(), "name".to_string()]),
            &Which::all(),
            &Which::all(),
            &With::Ids,
        );
        assert_eq!(
            statement.sql().trim(),
            r#"
        SELECT id, name FROM table_versions
                WHERE
                    id IN (
                        SELECT MAX(tv.id) FROM table_versions tv
                            WHERE tv.defined_on <= ?1
                            GROUP BY tv.table_id
                    )
                    AND
                    status != 'D'
        "#
            .trim()
        );
        assert_eq!(statement.params(), &["at_time".to_string()]);
    }

    #[test]
    fn test_select_tables_at_time() {
        let statement = Queries::new().select_tables_at_time(
            &Columns::All,
            &Which::all(),
            &Which::all(),
            &With::Ids,
        );
        assert_eq!(
            statement.sql().trim(),
            r#"
        SELECT * FROM table_versions
                WHERE
                    id IN (
                        SELECT MAX(tv.id) FROM table_versions tv
                            WHERE tv.defined_on <= ?1
                            GROUP BY tv.table_id
                    )
                    AND
                    status != 'D'
        "#
            .trim()
        );
        assert_eq!(statement.params(), &["at_time".to_string()]);
    }

    #[test]
    fn test_select_tables_at_time_collections() {
        let statement = Queries::new().select_tables_at_time(
            &Columns::All,
            &Which::one(),
            &Which::all(),
            &With::Ids,
        );
        assert_eq!(
            statement.sql().trim(),
            r#"
        SELECT * FROM table_versions
                WHERE
                    id IN (
                        SELECT MAX(tv.id) FROM table_versions tv
                            WHERE tv.defined_on <= ?1 AND tv.collection_id = ?2
                            GROUP BY tv.table_id
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

        let statement = Queries::new().select_tables_at_time(
            &Columns::All,
            &Which::set(2),
            &Which::all(),
            &With::Ids,
        );
        assert_eq!(
            statement.sql().trim(),
            r#"
        SELECT * FROM table_versions
                WHERE
                    id IN (
                        SELECT MAX(tv.id) FROM table_versions tv
                            WHERE tv.defined_on <= ?1 AND tv.collection_id IN (?2,?3)
                            GROUP BY tv.table_id
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
    fn test_select_tables_at_time_tables() {
        let statement = Queries::new().select_tables_at_time(
            &Columns::All,
            &Which::all(),
            &Which::one(),
            &With::Ids,
        );
        assert_eq!(
            statement.sql().trim(),
            r#"
        SELECT * FROM table_versions
                WHERE
                    id IN (
                        SELECT MAX(tv.id) FROM table_versions tv
                            WHERE tv.defined_on <= ?1 AND tv.name = ?2
                            GROUP BY tv.table_id
                    )
                    AND
                    status != 'D'
        "#
            .trim()
        );
        assert_eq!(
            statement.params(),
            &["at_time".to_string(), "name".to_string()]
        );

        let statement = Queries::new().select_tables_at_time(
            &Columns::All,
            &Which::all(),
            &Which::set(2),
            &With::Ids,
        );
        assert_eq!(
            statement.sql().trim(),
            r#"
        SELECT * FROM table_versions
                WHERE
                    id IN (
                        SELECT MAX(tv.id) FROM table_versions tv
                            WHERE tv.defined_on <= ?1 AND tv.name IN (?2,?3)
                            GROUP BY tv.table_id
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
                "name#0".to_string(),
                "name#1".to_string()
            ]
        );

        let statement = Queries::new().select_tables_at_time(
            &Columns::All,
            &Which::all(),
            &Which::like(),
            &With::Ids,
        );
        assert_eq!(
            statement.sql().trim(),
            r#"
        SELECT * FROM table_versions
                WHERE
                    id IN (
                        SELECT MAX(tv.id) FROM table_versions tv
                            WHERE tv.defined_on <= ?1 AND tv.name LIKE ?2
                            GROUP BY tv.table_id
                    )
                    AND
                    status != 'D'
        "#
            .trim()
        );
        assert_eq!(
            statement.params(),
            &["at_time".to_string(), "name".to_string(),]
        );
    }

    #[test]
    fn test_select_tables_at_time_collections_tables() {
        let statement = Queries::new().select_tables_at_time(
            &Columns::All,
            &Which::one(),
            &Which::one(),
            &With::Ids,
        );
        assert_eq!(
            statement.sql().trim(),
            r#"
        SELECT * FROM table_versions
                WHERE
                    id IN (
                        SELECT MAX(tv.id) FROM table_versions tv
                            WHERE tv.defined_on <= ?1 AND tv.collection_id = ?2 AND tv.name = ?3
                            GROUP BY tv.table_id
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
                "name".to_string(),
            ]
        );
    }

    //-----

    #[tokio::test]
    async fn test_sql_syntax() {
        let db = db().await.unwrap();
        let mut conn = db.acquire().await.unwrap();

        let statements = vec![
            (
                false,
                Queries::new().select_tables_current(
                    &Columns::All,
                    &Which::all(),
                    &Which::all(),
                    &With::Ids,
                ),
            ),
            (
                false,
                Queries::new().select_tables_current(
                    &Columns::All,
                    &Which::all(),
                    &Which::all(),
                    &With::Names,
                ),
            ),
            (
                false,
                Queries::new().select_tables_current(
                    &Columns::One("id"),
                    &Which::all(),
                    &Which::all(),
                    &With::Ids,
                ),
            ),
            (
                false,
                Queries::new().select_tables_current(
                    &Columns::Some(&["id"]),
                    &Which::all(),
                    &Which::all(),
                    &With::Ids,
                ),
            ),
            (
                false,
                Queries::new().select_tables_current(
                    &Columns::All,
                    &Which::one(),
                    &Which::all(),
                    &With::Ids,
                ),
            ),
            (
                false,
                Queries::new().select_tables_current(
                    &Columns::All,
                    &Which::set(1),
                    &Which::all(),
                    &With::Ids,
                ),
            ),
            (
                false,
                Queries::new().select_tables_current(
                    &Columns::All,
                    &Which::set(2),
                    &Which::all(),
                    &With::Ids,
                ),
            ),
            (
                false,
                Queries::new().select_tables_current(
                    &Columns::All,
                    &Which::all(),
                    &Which::one(),
                    &With::Ids,
                ),
            ),
            (
                false,
                Queries::new().select_tables_current(
                    &Columns::All,
                    &Which::all(),
                    &Which::set(1),
                    &With::Ids,
                ),
            ),
            (
                false,
                Queries::new().select_tables_current(
                    &Columns::All,
                    &Which::all(),
                    &Which::set(2),
                    &With::Ids,
                ),
            ),
            (
                false,
                Queries::new().select_tables_current(
                    &Columns::All,
                    &Which::one(),
                    &Which::one(),
                    &With::Ids,
                ),
            ),
            (
                true,
                Queries::new().select_tables_at_time(
                    &Columns::All,
                    &Which::all(),
                    &Which::all(),
                    &With::Ids,
                ),
            ),
            (
                true,
                Queries::new().select_tables_at_time(
                    &Columns::All,
                    &Which::all(),
                    &Which::all(),
                    &With::Names,
                ),
            ),
            (
                true,
                Queries::new().select_tables_at_time(
                    &Columns::One("id"),
                    &Which::all(),
                    &Which::all(),
                    &With::Ids,
                ),
            ),
            (
                true,
                Queries::new().select_tables_at_time(
                    &Columns::Some(&["id"]),
                    &Which::all(),
                    &Which::all(),
                    &With::Ids,
                ),
            ),
            (
                true,
                Queries::new().select_tables_at_time(
                    &Columns::All,
                    &Which::one(),
                    &Which::all(),
                    &With::Ids,
                ),
            ),
            (
                true,
                Queries::new().select_tables_at_time(
                    &Columns::All,
                    &Which::set(1),
                    &Which::all(),
                    &With::Ids,
                ),
            ),
            (
                true,
                Queries::new().select_tables_at_time(
                    &Columns::All,
                    &Which::set(2),
                    &Which::all(),
                    &With::Ids,
                ),
            ),
            (
                true,
                Queries::new().select_tables_at_time(
                    &Columns::All,
                    &Which::all(),
                    &Which::all(),
                    &With::Ids,
                ),
            ),
            (
                true,
                Queries::new().select_tables_at_time(
                    &Columns::All,
                    &Which::all(),
                    &Which::set(1),
                    &With::Ids,
                ),
            ),
            (
                true,
                Queries::new().select_tables_at_time(
                    &Columns::All,
                    &Which::all(),
                    &Which::set(2),
                    &With::Ids,
                ),
            ),
            (
                true,
                Queries::new().select_tables_at_time(
                    &Columns::All,
                    &Which::one(),
                    &Which::one(),
                    &With::Ids,
                ),
            ),
        ];

        for (first_param_is_time, statement) in statements {
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
