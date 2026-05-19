//
// Copyright 2026 Tabs Data Inc.
//

use crate::QueryExecutor;
use sea_query::{Expr, ExprTrait, Query};
use ta_objects::DaoCore;
use ta_objects::SeaValue;
use ta_objects::errors::SqlError;
use td_database::sea_query::builder::SeaQueryExt;
use td_objects::dxo::auth::{SessionDB, SessionDBIden};
use td_objects::types::basic::AtTime;
use tracing::debug;

query_handle! {
    /// Entry trait providing [`sessions`](SessionsEntry::sessions) on every
    /// [`QueryExecutor`](crate::QueryExecutor). Returns a [`Sessions`] handle.
    pub trait SessionsEntry {
        fn sessions(self) -> Sessions<Self> {
            Sessions { executor: self }
        }
    }
}

/// `sessions`-table operations. Reach via [`SessionsEntry::sessions`]:
///
/// ```ignore
/// conn.sessions().purge().await?;
/// ```
pub struct Sessions<E> {
    pub(crate) executor: E,
}

impl<E> Sessions<E>
where
    E: QueryExecutor,
{
    /// Delete every session whose `expires_on` is in the past.
    pub async fn purge(self) -> Result<(), SqlError> {
        let now = AtTime::now();
        let purged = purge_query(&now)
            .build_query()
            .execute(self.executor)
            .await
            .map_err(SqlError::on_read::<SessionDB>)?
            .rows_affected();
        debug!("Purged {purged} expired sessions");
        Ok(())
    }
}

pub(crate) fn purge_query(now: &AtTime) -> sea_query::DeleteStatement {
    Query::delete()
        .from_table(SessionDB::table())
        .cond_where(Expr::col(SessionDBIden::ExpiresOn).lt(Expr::val(now.as_sea_value())))
        .take()
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_query::SqliteQueryBuilder;

    #[test]
    fn test_purge_query_sql() {
        let now = AtTime::now();
        let sql = purge_query(&now).to_string(SqliteQueryBuilder);
        assert!(sql.starts_with(r#"DELETE FROM "sessions""#), "{sql}");
        assert!(
            sql.contains(r#""expires_on" <"#),
            "should filter by expires_on: {sql}"
        );
    }
}
