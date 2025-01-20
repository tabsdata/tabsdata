//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::users::error::UserError;
use sqlx::sqlite::SqliteRow;
use td_common::error::TdError;
use td_objects::crudl::{handle_select_error, RequestContext};
use td_tower::extractors::{Connection, Context, IntoMutSqlConnection};

/// function to use in other services to assert the user making the request is active
pub async fn assert_user_enabled(
    Connection(connection): Connection,
    Context(context): Context<RequestContext>,
) -> Result<(), TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    const SELECT_ASSERT_USER_ACTIVE: &str = r#"
            SELECT 1
            FROM users
            WHERE id = ?1 AND enabled = true
        "#;

    let enabled: Option<SqliteRow> = sqlx::query(SELECT_ASSERT_USER_ACTIVE)
        .bind(context.user_id())
        .fetch_optional(conn)
        .await
        .map_err(handle_select_error)?;
    enabled.map(|_| ()).ok_or(UserError::UserNotEnabled.into())
}

#[cfg(test)]
mod tests {}
