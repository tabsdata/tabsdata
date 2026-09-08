//
// Copyright 2026 Tabsdata Inc.
//

//! The test-only project-scoped executor: `db.scoped(p)` runs **each**
//! statement in its own transaction (begin → `SET LOCAL app.project_id` → run →
//! commit). Production never needs it — serving requests acquire an
//! already-scoped connection from their `DbPool` handle.
//!
//! For a multi-statement scope, scope one connection with
//! [`PgConnectionExt::scope_in_project`]; a plain `db.begin()` otherwise runs
//! as the connection's session role, which in test builds is the
//! cross-project one.

use crate::connection::PgConnectionExt;
use crate::pool::TdPool;
use crate::pool::scoped::Scoped;
use crate::sea_query::builder::SeaQuerySqlxBuild;
use crate::sea_query::executor::{RunOp, SeaExecutor};
use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use sqlx::Error;
use std::fmt::Display;

impl TdPool {
    pub fn scoped<P>(&self, project: P) -> Scoped<&TdPool, P> {
        Scoped {
            project,
            inner: self,
        }
    }
}

impl<'a, P: Display + Clone + Sync> SeaExecutor for &'_ Scoped<&'a TdPool, P> {
    type Reused<'b>
        = &'b Scoped<&'a TdPool, P>
    where
        Self: 'b;

    fn reuse(&mut self) -> Self::Reused<'_> {
        self
    }

    /// Committing on success is uniform across reads and writes: a transaction
    /// that only ran `SELECT`s leaves the same state either way.
    fn run<'e, T: Send + 'e>(
        self,
        query: impl SeaQuerySqlxBuild,
        op: RunOp<T>,
    ) -> BoxFuture<'e, Result<T, Error>>
    where
        Self: 'e,
    {
        let pool = self.inner;
        let project = self.project.to_string();
        let (sql, values) = query.build_sea_query();
        async move {
            let mut trx = pool.begin().await?;
            trx.scope_in_project(&project).await?;
            match op(sql, values, &mut trx).await {
                Ok(value) => {
                    trx.commit().await?;
                    Ok(value)
                }
                Err(err) => {
                    // The statement's error wins over any rollback failure.
                    let _ = trx.rollback().await;
                    Err(err)
                }
            }
        }
        .boxed()
    }
}
