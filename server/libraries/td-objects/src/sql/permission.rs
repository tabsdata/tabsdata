//
// Copyright 2025 Tabs Data Inc.
//

use crate::sql::{condition_builder, select_cols, Columns, Queries, Statement, Which, With};
use crate::types::basic::RoleId;
use tracing::trace;

/// Permission Queries.
pub struct PermissionQueries {}
impl Queries for PermissionQueries {}

impl PermissionQueries {
    /// Constructor.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {}
    }

    /// SQL statement: ?1 = role_id
    pub fn select_permissions(
        &self,
        select: &Columns,
        roles: &Which<RoleId>,
        with: &With,
    ) -> Statement {
        let select_columns = select_cols(select);
        let table = with.table_name("permissions");

        let mut sql = format!("SELECT {select_columns} FROM {table}");
        let mut params = vec![];
        let conditions = 0;

        let condition = condition_builder(None, "role_id", conditions, roles);
        if let Some(expr) = condition.expr {
            sql += &format!(" WHERE {}", expr);
            params.extend(condition.params);
        }

        trace!("select_permissions: sql: {}", sql);
        Statement { sql, params }
    }

    /// SQL statement: ?1 = id, ?2 = role_id, ?3 = permission_type, ?4 = entity_type,
    ///                ?5 = entity_id_on, ?6 = granted_by_id, ?7 = granted_on, ?8 = fixed
    pub fn insert_permission(&self) -> Statement {
        let sql = r#"
        INSERT INTO permissions
            (id, role_id, permission_type, entity_type, entity_id, granted_by_id, granted_on, fixed)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
        "#
        .to_string();
        let params = vec![
            "id".to_string(),
            "role_id".to_string(),
            "permission_type".to_string(),
            "entity_type".to_string(),
            "entity_id".to_string(),
            "granted_by_id".to_string(),
            "granted_on".to_string(),
            "fixed".to_string(),
        ];
        trace!("insert_permissions: sql: {}", sql);
        Statement { sql, params }
    }

    /// SQL statement: ?1 = id, ?2 = role_id
    pub fn delete_permission(&self) -> Statement {
        let sql = r#"
        DELETE FROM permissions
            WHERE id = ?1 AND role_id = ?2
        "#
        .to_string();
        let params = vec!["id".to_string(), "role_id".to_string()];
        trace!("delete_permissions: sql: {}", sql);
        Statement { sql, params }
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
                PermissionQueries::new().select_permissions(
                    &Columns::All,
                    &Which::all(),
                    &With::Ids,
                ),
                vec![],
            ),
            (
                PermissionQueries::new().select_permissions(
                    &Columns::All,
                    &Which::all(),
                    &With::Names,
                ),
                vec![],
            ),
            (
                PermissionQueries::new().select_permissions(
                    &Columns::All,
                    &Which::one(),
                    &With::Ids,
                ),
                vec!["r"],
            ),
            (
                PermissionQueries::new().select_permissions(
                    &Columns::All,
                    &Which::set(2),
                    &With::Ids,
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
    async fn test_insert_permission_syntax() {
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

        let statement = PermissionQueries::new().insert_permission();
        let mut query = sqlx::query(statement.sql());
        query = query.bind("p0");
        query = query.bind("r0");
        query = query.bind("ss");
        query = query.bind("S");
        query = query.bind("*");
        query = query.bind("u0");
        query = query.bind(UniqueUtc::now_millis().await);
        query = query.bind(true);

        assert!(
            query.execute(&mut *trx).await.is_ok(),
            "failed on statement: {:?}",
            statement.sql()
        );

        trx.commit().await.unwrap()
    }

    #[tokio::test]
    async fn test_delete_permission() {
        let db = db().await.unwrap();
        let mut trx = db.begin().await.unwrap();

        let statement = PermissionQueries::new().delete_permission();
        let mut query = sqlx::query(statement.sql());
        query = query.bind("p0");
        query = query.bind("r0");

        assert!(
            query.execute(&mut *trx).await.is_ok(),
            "failed on statement: {:?}",
            statement.sql()
        );

        trx.commit().await.unwrap()
    }
}
