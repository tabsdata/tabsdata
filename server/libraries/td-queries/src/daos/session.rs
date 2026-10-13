//
// Copyright 2026 Tabsdata Inc.
//

use crate::QueryExecutor;
use crate::Writes;
use sea_query::{DeleteStatement, Expr, ExprTrait, LockBehavior, LockType, Query};
use ta_objects::Dao;
use ta_objects::SeaValue;
use ta_objects::errors::SqlError;
use td_database::sea_query::builder::SeaQueryExt;
use td_objects::dxo::auth::{SessionDB, SessionDBIden};
use td_objects::types::basic::AtTime;

/// `sessions`-table operations.
pub struct Sessions<E> {
    pub(crate) executor: E,
}

impl<E> Sessions<E>
where
    E: QueryExecutor + Writes,
{
    /// Deletes one batch of sessions whose `expires_on` is in the past,
    /// passing over any another transaction holds, so a purge never waits on
    /// a request; returns how many were deleted.
    pub async fn purge(self) -> Result<u64, SqlError> {
        let now = AtTime::now();
        Ok(purge_query(&now)
            .build_query()
            .execute(self.executor)
            .await
            .map_err(SqlError::on_read::<SessionDB>)?
            .rows_affected())
    }
}

/// The most sessions one [`Sessions::purge`] deletes.
pub const PURGE_BATCH: u64 = 1000;

pub(crate) fn purge_query(now: &AtTime) -> DeleteStatement {
    let expired = Query::select()
        .column(SessionDBIden::AccessTokenId)
        .from(SessionDB::TABLE)
        .and_where(Expr::col(SessionDBIden::ExpiresOn).lt(now.as_sea_expr()))
        .limit(PURGE_BATCH)
        .lock_with_behavior(LockType::Update, LockBehavior::SkipLocked)
        .take();
    Query::delete()
        .from_table(SessionDB::TABLE)
        .cond_where(Expr::col(SessionDBIden::AccessTokenId).in_subquery(expired))
        .take()
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_query::PostgresQueryBuilder;
    use td_common::datetime::Timestamp;

    #[test]
    fn test_purge_query() {
        let now = AtTime::from(Timestamp::try_from(1_700_000_000_000_i64).unwrap());
        let sql = purge_query(&now).to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"DELETE FROM "sessions" WHERE "access_token_id" IN (SELECT "access_token_id" FROM "sessions" WHERE "expires_on" < '2023-11-14 22:13:20.000000 +00:00' LIMIT 1000 FOR UPDATE SKIP LOCKED)"#
        );
    }
}
