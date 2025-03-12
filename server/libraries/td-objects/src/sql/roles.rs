//
// Copyright 2025 Tabs Data Inc.
//

use crate::sql::{condition_builder, placeholders, select_cols, Columns, Statement, Which, With};
use crate::types::basic::RoleName;
use crate::types::role::RoleDB;
use tracing::trace;

/// Roles Queries.
pub struct Queries {}

impl Queries {
    /// Constructor.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {}
    }

    /// SQL statement: ?1 = name
    pub fn select_roles(self, select: &Columns, roles: Which<RoleName>, with: With) -> Statement {
        let select_columns = select_cols(select);
        let table = with.table_name("roles");

        let mut sql = format!("SELECT {select_columns} FROM {table}");
        let mut params = vec![];

        let conditions = 0;
        let condition = condition_builder(None, "name", conditions, roles);
        if let Some(expr) = condition.expr {
            sql += &format!(" WHERE {}", expr);
            params.extend(condition.params);
        }

        trace!("select_roles: sql: {}", sql);
        Statement::new(sql, params)
    }

    /// SQL statement: ?1 = id, ?2 = name, ?3 = description, ?4 = created_on,
    ///                ?5 = created_by_id, ?6 = modified_on, ?7 = modified_by_id, ?8 = fixed
    pub fn insert_roles(self) -> Statement {
        let fields = RoleDB::fields();
        let sql = format!(
            "INSERT INTO roles ({}) VALUES ({})",
            fields.join(", "),
            placeholders(fields.len())
        );
        trace!("insert_roles: sql: {}", sql);
        Statement::new(sql, fields)
    }

    /// SQL statement: ?1 = id
    pub fn delete_role(self) -> Statement {
        let sql = r#"
            DELETE FROM roles WHERE id = ?1
        "#;
        let params = vec!["id"];
        trace!("delete_role: sql: {}", sql);
        Statement::new(sql, params)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::Which;
    use td_common::time::UniqueUtc;
    use td_database::test_utils::db;

    #[tokio::test]
    async fn test_select_sql_syntax() {
        let db = db().await.unwrap();
        let mut conn = db.acquire().await.unwrap();

        let statements: Vec<(Statement, Vec<&str>)> = vec![
            (
                Queries::new().select_roles(&Columns::All, Which::all(), With::Ids),
                vec![],
            ),
            (
                Queries::new().select_roles(&Columns::All, Which::all(), With::Names),
                vec![],
            ),
            (
                Queries::new().select_roles(&Columns::All, Which::one(), With::Ids),
                vec!["r"],
            ),
            (
                Queries::new().select_roles(&Columns::All, Which::set(2), With::Ids),
                vec!["r0", "r2"],
            ),
            (
                Queries::new().select_roles(
                    &Columns::Some(RoleDB::fields()),
                    Which::set(2),
                    With::Ids,
                ),
                vec!["r0", "r2"],
            ),
            (
                Queries::new().select_roles(
                    &Columns::Some(&[RoleDB::fields().first().unwrap()]),
                    Which::set(2),
                    With::Ids,
                ),
                vec!["r0", "r2"],
            ),
        ];

        for (statement, params) in statements {
            let mut query = sqlx::query(statement.sql());
            for param in params {
                query = query.bind(param);
            }
            assert!(
                query.fetch_all(&mut *conn).await.is_ok(),
                "failed on statement: {:?}",
                statement
            );
        }
    }

    #[tokio::test]
    async fn test_insert_sql_syntax() {
        let db = db().await.unwrap();
        let mut trx = db.begin().await.unwrap();

        let statement = Queries::new().insert_roles();
        let mut query = sqlx::query(statement.sql());
        query = query.bind("r0");
        query = query.bind("role0");
        query = query.bind("role0");
        query = query.bind(UniqueUtc::now_millis().await);
        query = query.bind("u0");
        query = query.bind(UniqueUtc::now_millis().await);
        query = query.bind("u0");
        query = query.bind(true);

        assert!(
            query.execute(&mut *trx).await.is_ok(),
            "failed on statement: {:?}",
            statement.sql()
        );

        trx.commit().await.unwrap()
    }

    #[tokio::test]
    async fn test_delete_sql_syntax() {
        let db = db().await.unwrap();
        let mut trx = db.begin().await.unwrap();

        let statement = Queries::new().delete_role();
        let mut query = sqlx::query(statement.sql());
        query = query.bind("role0");

        assert!(
            query.execute(&mut *trx).await.is_ok(),
            "failed on statement: {:?}",
            statement.sql()
        );

        trx.commit().await.unwrap()
    }
}
