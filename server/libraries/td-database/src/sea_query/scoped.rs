//
// Copyright 2026 Tabs Data Inc.
//

//! The test-only project-scoped executor: `db.scoped(p)` produces a
//! [`Scoped`]`<&TdPool, P>` that implements [`SeaExecutor`] — so
//! `.live()`/`.current()`/`.seed()` come for free — and runs **each** statement
//! in its own transaction (begin → `SET LOCAL app.project_id` → run →
//! commit/rollback). The [`Scoped`] data type itself (and the
//! [`InProject`](crate::scoped::InProject) constructor) lives in
//! [`crate::scoped`]; this module only adds the executor impl, next to the
//! other [`SeaExecutor`]s.
//!
//! Production never uses this: serving requests get an already-scoped
//! connection from the tower `ProjectScopeProvider`, so layers just run on the
//! connection they're handed. `db.scoped(p)` is the test-only convenience for
//! scoping an ad-hoc read or seed without standing up a service. For a
//! multi-statement scope, scope a connection once with
//! [`DynamicConnection::set_project_scope`]; a plain `db.begin()` connection
//! otherwise inherits the test serving pool's default (`app.bypass_rls = on`).

use crate::dynamic::{
    DynamicConnection, DynamicDecode, DynamicFromRow, DynamicQueryResult, DynamicRow, DynamicType,
};
use crate::pool::TdPool;
use crate::scoped::Scoped;
use crate::sea_query::builder::SeaQuerySqlxBuild;
use crate::sea_query::executor::SeaExecutor;
use crate::sea_query::kind::{DatabaseBackend, DatabaseKind};
use crate::sea_query::raw::RawSeaQuery;
use futures_util::future::BoxFuture;
use futures_util::stream::{self as stream, BoxStream};
use futures_util::{FutureExt, StreamExt};
use sea_query::raw_sql::seaql::Query;
use sqlx::{Either, Error};
use std::fmt::Display;

impl TdPool {
    /// A [`Scoped`] executor that runs every statement scoped to `project`
    /// (`SET LOCAL app.project_id`). `project` is anything `Display` — typically
    /// a `ProjectId`. Test-only; see the module docs.
    pub fn scoped<P>(&self, project: P) -> Scoped<&TdPool, P> {
        Scoped {
            project,
            inner: self,
        }
    }
}

impl<P> DatabaseKind for Scoped<&TdPool, P> {
    fn kind(&self) -> DatabaseBackend {
        self.inner.kind()
    }
}

/// Close the per-statement scoped transaction embedded in `conn` and surface the
/// statement's result. Writes (`commit = true`) commit on success; reads roll
/// back (nothing to persist). Any error rolls back, and the original error wins
/// over a rollback failure.
async fn finish<T>(
    conn: DynamicConnection<'_>,
    out: Result<T, Error>,
    commit: bool,
) -> Result<T, Error> {
    let tx = conn
        .into_transaction()
        .expect("Scoped executor always runs on an owned transaction");
    match out {
        Ok(value) => {
            if commit {
                tx.commit().await?;
            } else {
                tx.rollback().await?;
            }
            Ok(value)
        }
        Err(err) => {
            let _ = tx.rollback().await;
            Err(err)
        }
    }
}

/// A future-returning `SeaExecutor` method: build the query up-front (so the
/// non-`Send` builder isn't captured), then on a scoped tx replay it as a
/// `RawSeaQuery` against the `&mut DynamicConnection` executor.
macro_rules! sfut {
    // non-generic (raw rows / query result)
    ($name:ident, $ret:ty, $commit:literal) => {
        fn $name<'e>(
            self,
            query: impl SeaQuerySqlxBuild,
        ) -> BoxFuture<'e, Result<$ret, Error>>
        where
            Self: 'e,
        {
            let pool = self.inner;
            let (sql, values) = query.build_sea_query(pool);
            let raw = RawSeaQuery(Query { sql, values: values.0 });
            let project = self.project.to_string();
            async move {
                let tx = pool.begin().await?;
                let mut conn = tx.into_connection();
                conn.set_project_scope(&project).await?;
                let out = (&mut conn).$name(raw).await;
                finish(conn, out, $commit).await
            }
            .boxed()
        }
    };
    // generic over the row/scalar type `O`
    ($name:ident, $ret:ty, $commit:literal, $first:path $(, $rest:path)*) => {
        fn $name<'e, O: $first $(+ $rest)*>(
            self,
            query: impl SeaQuerySqlxBuild,
        ) -> BoxFuture<'e, Result<$ret, Error>>
        where
            Self: 'e,
        {
            let pool = self.inner;
            let (sql, values) = query.build_sea_query(pool);
            let raw = RawSeaQuery(Query { sql, values: values.0 });
            let project = self.project.to_string();
            async move {
                let tx = pool.begin().await?;
                let mut conn = tx.into_connection();
                conn.set_project_scope(&project).await?;
                let out = (&mut conn).$name::<O>(raw).await;
                finish(conn, out, $commit).await
            }
            .boxed()
        }
    };
}

impl<'a, P: Display + Clone + Sync> SeaExecutor for &'_ Scoped<&'a TdPool, P> {
    type Reused<'b>
        = &'b Scoped<&'a TdPool, P>
    where
        Self: 'b;

    fn reuse(&mut self) -> Self::Reused<'_> {
        self
    }

    sfut!(execute, DynamicQueryResult, true);
    sfut!(fetch_all, Vec<DynamicRow>, false);
    sfut!(fetch_one, DynamicRow, false);
    sfut!(fetch_optional, Option<DynamicRow>, false);
    sfut!(fetch_all_as, Vec<O>, false, Send, Unpin, DynamicFromRow);
    sfut!(fetch_one_as, O, false, Send, Unpin, DynamicFromRow);
    sfut!(
        fetch_optional_as,
        Option<O>,
        false,
        Send,
        Unpin,
        DynamicFromRow
    );
    sfut!(
        fetch_all_scalar,
        Vec<O>,
        false,
        Send,
        Unpin,
        DynamicDecode,
        DynamicType
    );
    sfut!(
        fetch_one_scalar,
        O,
        false,
        Send,
        Unpin,
        DynamicDecode,
        DynamicType
    );
    sfut!(
        fetch_optional_scalar,
        Option<O>,
        false,
        Send,
        Unpin,
        DynamicType,
        DynamicDecode
    );

    // Streaming reads: not needed by the scoped read paths yet. Wire through
    // `async_stream` (scoped tx around the stream) when a caller needs them.
    fn fetch<'e>(self, _query: impl SeaQuerySqlxBuild) -> BoxStream<'e, Result<DynamicRow, Error>>
    where
        Self: 'e,
    {
        stream::once(async { unimplemented!("Scoped executor: streaming `fetch` not supported") })
            .boxed()
    }

    #[allow(deprecated)]
    fn fetch_many<'e>(
        self,
        _query: impl SeaQuerySqlxBuild,
    ) -> BoxStream<'e, Result<Either<DynamicQueryResult, DynamicRow>, Error>>
    where
        Self: 'e,
    {
        stream::once(async {
            unimplemented!("Scoped executor: streaming `fetch_many` not supported")
        })
        .boxed()
    }

    fn fetch_as<'e, O: Send + Unpin + DynamicFromRow + 'e>(
        self,
        _query: impl SeaQuerySqlxBuild,
    ) -> BoxStream<'e, Result<O, Error>>
    where
        Self: 'e,
    {
        stream::once(async {
            unimplemented!("Scoped executor: streaming `fetch_as` not supported")
        })
        .boxed()
    }
}
