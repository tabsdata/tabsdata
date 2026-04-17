//
// Copyright 2026 Tabs Data Inc.
//

use sea_query::{Expr, ExprTrait, Query};
use ta_objects::DaoCore;
use ta_objects::SeaValue;
use ta_objects::errors::SqlError;
use td_database::sea_query::builder::SeaQueryExt;
use td_database::sea_query::executor::SeaExecutor;
use td_objects::dxo::auth::{SessionDB, SessionDBIden};
use td_objects::types::basic::AtTime;
use tracing::debug;

pub trait SessionQueries {
    fn purge_sessions(self) -> impl Future<Output = Result<(), SqlError>>;
}

pub(crate) fn purge_sessions_query(now: &AtTime) -> sea_query::DeleteStatement {
    Query::delete()
        .from_table(SessionDB::table())
        .cond_where(Expr::col(SessionDBIden::ExpiresOn).lt(Expr::val(now.as_sea_value())))
        .take()
}

impl<E> SessionQueries for E
where
    E: SeaExecutor + Send,
{
    async fn purge_sessions(self) -> Result<(), SqlError> {
        let now = AtTime::now();
        let purged = purge_sessions_query(&now)
            .build_query()
            .execute(self)
            .await
            .map_err(|e| SqlError::on_entity("Session", e))?
            .rows_affected();
        debug!("Purged {purged} expired sessions");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_query::SqliteQueryBuilder;

    #[test]
    fn test_purge_sessions_query_sql() {
        let now = AtTime::now();
        let sql = purge_sessions_query(&now).to_string(SqliteQueryBuilder);
        assert!(sql.starts_with(r#"DELETE FROM "sessions""#), "{sql}");
        assert!(
            sql.contains(r#""expires_on" <"#),
            "should filter by expires_on: {sql}"
        );
    }
}
