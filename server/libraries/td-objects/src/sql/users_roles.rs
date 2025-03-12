//
// Copyright 2025 Tabs Data Inc.
//

use crate::sql::{condition_builder, placeholders, select_cols, Columns, Statement, Which, With};
use crate::types::basic::RoleName;
use crate::types::role::UsersRolesDB;
use crate::types::DataAccessObject;
use tracing::trace;

/// Roles Queries.
pub struct Queries {}

impl Queries {
    /// Constructor.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {}
    }

    /// SQL statement: ?1 = role_id
    pub fn select_users_roles(
        self,
        select: &Columns,
        roles: Which<RoleName>,
        with: With,
    ) -> Statement {
        let select_columns = select_cols(select);
        let table = with.table_name("users_roles");

        let mut sql = format!("SELECT {select_columns} FROM {table}");
        let mut params = vec![];

        let conditions = 0;
        let condition = condition_builder(None, "role_id", conditions, roles);
        if let Some(expr) = condition.expr {
            sql += &format!(" WHERE {}", expr);
            params.extend(condition.params);
        }

        trace!("select_users_roles: sql: {}", sql);
        Statement::new(sql, params)
    }

    /// SQL statement: ?1 = id, ?2 = user_id, ?3 = role_id, ?4 = added_on,
    ///                ?5 = added_by_id, ?6 = fixed
    pub fn insert_users_roles(self) -> Statement {
        let fields = UsersRolesDB::fields();
        let sql = format!(
            "INSERT INTO users_roles ({}) VALUES ({})",
            fields.join(", "),
            placeholders(fields.len())
        );
        trace!("insert_users_roles: sql: {}", sql);
        Statement::new(sql, fields)
    }

    /// SQL statement: ?1 = id
    pub fn delete_users_roles(self) -> Statement {
        let sql = r#"
            DELETE FROM users_roles WHERE id = ?1
        "#;
        let params = vec!["id"];
        trace!("delete_users_roles: sql: {}", sql);
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
                Queries::new().select_users_roles(&Columns::All, Which::all(), With::Ids),
                vec![],
            ),
            (
                Queries::new().select_users_roles(&Columns::All, Which::all(), With::Names),
                vec![],
            ),
            (
                Queries::new().select_users_roles(&Columns::All, Which::one(), With::Ids),
                vec!["r"],
            ),
            (
                Queries::new().select_users_roles(&Columns::All, Which::set(2), With::Ids),
                vec!["r0", "r2"],
            ),
            (
                Queries::new().select_users_roles(
                    &Columns::Some(UsersRolesDB::fields()),
                    Which::set(2),
                    With::Ids,
                ),
                vec!["r0", "r2"],
            ),
            (
                Queries::new().select_users_roles(
                    &Columns::Some(&[UsersRolesDB::fields().first().unwrap()]),
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

        sqlx::query(
            r#"
            INSERT INTO roles
            (id, name, description, created_on, created_by_id, modified_on, modified_by_id, fixed)
            VALUES ('r0', 'role0', 'role0', datetime('now'), 'u0', datetime('now'), 'u0', true)
        "#,
        )
        .execute(&mut *trx)
        .await
        .unwrap();

        let statement = Queries::new().insert_users_roles();
        let mut query = sqlx::query(statement.sql());
        query = query.bind("ur0");
        query = query.bind("u0");
        query = query.bind("r0");
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

        let statement = Queries::new().delete_users_roles();
        let mut query = sqlx::query(statement.sql());
        query = query.bind("user_role0");

        assert!(
            query.execute(&mut *trx).await.is_ok(),
            "failed on statement: {:?}",
            statement.sql()
        );

        trx.commit().await.unwrap()
    }
}
