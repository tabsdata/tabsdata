//
// Copyright 2026 Tabs Data Inc.
//

use crate::dynamic::{
    DynamicConnection, DynamicDecode, DynamicFromRow, DynamicPool, DynamicQueryResult, DynamicRow,
    DynamicType,
};
use crate::pool::TdPool;
use crate::sea_query::builder::SeaQuerySqlxBuild;
use crate::sea_query::kind::DatabaseKind;
use futures_util::future::BoxFuture;
use futures_util::stream::BoxStream;
use futures_util::{FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use sqlx::{Either, Error};

/// Sqlx executor for SeaQueries.
pub trait SeaExecutor: DatabaseKind {
    /// A short-lived executor obtained by reborrowing `self`.
    type Reused<'a>: SeaExecutor + Send
    where
        Self: 'a;

    /// Obtain a short-lived executor for one query in a loop.
    /// For pools this is a no-op copy; for connections it reborrows.
    fn reuse(&mut self) -> Self::Reused<'_>;

    fn execute<'e>(
        self,
        query: impl SeaQuerySqlxBuild,
    ) -> BoxFuture<'e, Result<DynamicQueryResult, Error>>
    where
        Self: 'e;

    fn fetch<'e>(self, query: impl SeaQuerySqlxBuild) -> BoxStream<'e, Result<DynamicRow, Error>>
    where
        Self: 'e;

    #[deprecated = "As deprecated by sqlx. Remove when needed."]
    fn fetch_many<'e>(
        self,
        query: impl SeaQuerySqlxBuild,
    ) -> BoxStream<'e, Result<Either<DynamicQueryResult, DynamicRow>, Error>>
    where
        Self: 'e;

    fn fetch_all<'e>(
        self,
        query: impl SeaQuerySqlxBuild,
    ) -> BoxFuture<'e, Result<Vec<DynamicRow>, Error>>
    where
        Self: 'e;

    fn fetch_one<'e>(
        self,
        query: impl SeaQuerySqlxBuild,
    ) -> BoxFuture<'e, Result<DynamicRow, Error>>
    where
        Self: 'e;

    fn fetch_optional<'e>(
        self,
        query: impl SeaQuerySqlxBuild,
    ) -> BoxFuture<'e, Result<Option<DynamicRow>, Error>>
    where
        Self: 'e;

    fn fetch_all_as<'e, O: Send + Unpin + DynamicFromRow>(
        self,
        query: impl SeaQuerySqlxBuild,
    ) -> BoxFuture<'e, Result<Vec<O>, Error>>
    where
        Self: 'e;

    fn fetch_one_as<'e, O: Send + Unpin + DynamicFromRow>(
        self,
        query: impl SeaQuerySqlxBuild,
    ) -> BoxFuture<'e, Result<O, Error>>
    where
        Self: 'e;

    fn fetch_optional_as<'e, O: Send + Unpin + DynamicFromRow>(
        self,
        query: impl SeaQuerySqlxBuild,
    ) -> BoxFuture<'e, Result<Option<O>, Error>>
    where
        Self: 'e;

    fn fetch_as<'e, O: Send + Unpin + DynamicFromRow + 'e>(
        self,
        query: impl SeaQuerySqlxBuild,
    ) -> BoxStream<'e, Result<O, Error>>
    where
        Self: 'e;

    fn fetch_all_scalar<'e, O: Send + Unpin + DynamicDecode + DynamicType>(
        self,
        query: impl SeaQuerySqlxBuild,
    ) -> BoxFuture<'e, Result<Vec<O>, Error>>
    where
        Self: 'e;

    fn fetch_one_scalar<'e, O: Send + Unpin + DynamicDecode + DynamicType>(
        self,
        query: impl SeaQuerySqlxBuild,
    ) -> BoxFuture<'e, Result<O, Error>>
    where
        Self: 'e;

    fn fetch_optional_scalar<'e, O: Send + Unpin + DynamicType + DynamicDecode>(
        self,
        query: impl SeaQuerySqlxBuild,
    ) -> BoxFuture<'e, Result<Option<O>, Error>>
    where
        Self: 'e;
}

/// Converts a `Vec` of database-specific rows into `Vec<DynamicRow>`.
#[inline]
fn map_rows<R: Into<DynamicRow>>(rows: Vec<R>) -> Vec<DynamicRow> {
    rows.into_iter().map(Into::into).collect()
}

// ---------------------------------------------------------------------------
// Dispatch macros
// ---------------------------------------------------------------------------
//
// `resolve_executor!` is the single source of truth for per-database branching.
// It matches the pool/connection variant and binds the sqlx executor as `exec`.
// Every other macro calls it once; adding a new database means editing here only.
//
//   pool; $pool, $field  — destructures a DynamicPool struct field
//   conn; $conn          — destructures a DynamicConnection tuple variant
//
// `dispatch_future_raw/typed` and `dispatch_stream_raw/typed` build (sql, values)
// and delegate the per-DB branching to resolve_executor!.
//   raw   — uses query_with + .map_ok() to wrap into Dynamic* types
//   typed — uses $builder directly; sqlx already returns the target type O
//
// `impl_sea_executor!` has a single arm covering all executor types.
// It takes $rctx / $wctx — pre-formed token trees:
//   pool:  $rctx = (pool; self, ro)   $wctx = (pool; self, rw)
//   conn:  $rctx = (conn; self)       $wctx = (conn; self)
//
// `fetch_many` is kept only while sqlx still requires it.
// TODO: remove `fetch_many` from the trait and impl_sea_executor! when sqlx drops it.

macro_rules! resolve_executor {
    (pool; $pool:expr, $field:ident, $exec:ident => $body:expr) => {
        match $pool.as_ref() {
            #[cfg(feature = "sqlite")]
            DynamicPool::Sqlite { $field: $exec, .. } => $body,
            #[cfg(feature = "postgres")]
            DynamicPool::Postgres { $field: $exec, .. } => $body,
            #[cfg(feature = "mysql")]
            DynamicPool::MySql { $field: $exec, .. } => $body,
        }
    };
    (conn; $conn:expr, $exec:ident => $body:expr) => {
        match $conn {
            #[cfg(feature = "sqlite")]
            DynamicConnection::Sqlite($exec) => {
                let $exec = &mut **$exec;
                $body
            }
            #[cfg(feature = "postgres")]
            DynamicConnection::Postgres($exec) => {
                let $exec = &mut **$exec;
                $body
            }
            #[cfg(feature = "mysql")]
            DynamicConnection::MySql($exec) => {
                let $exec = &mut **$exec;
                $body
            }
        }
    };
}

macro_rules! dispatch_future_raw {
    // unwrap context tt forwarded from impl_sea_executor!
    ((pool; $field:ident), $this:expr, $method:ident, $query:expr, $map_fn:expr) => {
        dispatch_future_raw!(pool; $this, $field, $method, $query, $map_fn)
    };
    ((conn), $this:expr, $method:ident, $query:expr, $map_fn:expr) => {
        dispatch_future_raw!(conn; $this, $method, $query, $map_fn)
    };
    // inner: build (sql, values) and dispatch
    (pool; $pool:expr, $field:ident, $method:ident, $query:expr, $map_fn:expr) => {{
        let (sql, values) = $query.build_sea_query($pool.as_ref());
        resolve_executor!(pool; $pool, $field, exec =>
            async move { sqlx::query_with(&sql, values).$method(exec).map_ok($map_fn).await }.boxed()
        )
    }};
    (conn; $conn:expr, $method:ident, $query:expr, $map_fn:expr) => {{
        let (sql, values) = $query.build_sea_query(&$conn);
        resolve_executor!(conn; $conn, exec =>
            async move { sqlx::query_with(&sql, values).$method(exec).map_ok($map_fn).await }.boxed()
        )
    }};
}

macro_rules! dispatch_future_typed {
    // unwrap
    ((pool; $field:ident), $this:expr, $method:ident, $query:expr, $builder:ident) => {
        dispatch_future_typed!(pool; $this, $field, $method, $query, $builder)
    };
    ((conn), $this:expr, $method:ident, $query:expr, $builder:ident) => {
        dispatch_future_typed!(conn; $this, $method, $query, $builder)
    };
    // inner
    (pool; $pool:expr, $field:ident, $method:ident, $query:expr, $builder:ident) => {{
        let (sql, values) = $query.build_sea_query($pool.as_ref());
        resolve_executor!(pool; $pool, $field, exec =>
            async move { sqlx::$builder(&sql, values).$method(exec).await }.boxed()
        )
    }};
    (conn; $conn:expr, $method:ident, $query:expr, $builder:ident) => {{
        let (sql, values) = $query.build_sea_query(&$conn);
        resolve_executor!(conn; $conn, exec =>
            async move { sqlx::$builder(&sql, values).$method(exec).await }.boxed()
        )
    }};
}

macro_rules! dispatch_stream_raw {
    // unwrap
    ((pool; $field:ident), $this:expr, $method:ident, $query:expr, $map_fn:expr) => {
        dispatch_stream_raw!(pool; $this, $field, $method, $query, $map_fn)
    };
    ((conn), $this:expr, $method:ident, $query:expr, $map_fn:expr) => {
        dispatch_stream_raw!(conn; $this, $method, $query, $map_fn)
    };
    // inner
    (pool; $pool:expr, $field:ident, $method:ident, $query:expr, $map_fn:expr) => {{
        let (sql, values) = $query.build_sea_query($pool.as_ref());
        async_stream::try_stream! {
            let stream = resolve_executor!(pool; $pool, $field, exec =>
                sqlx::query_with(&sql, values).$method(exec).map_ok($map_fn).boxed()
            );
            futures_util::pin_mut!(stream);
            while let Some(item) = stream.next().await { yield item?; }
        }.boxed()
    }};
    (conn; $conn:expr, $method:ident, $query:expr, $map_fn:expr) => {{
        let (sql, values) = $query.build_sea_query(&$conn);
        async_stream::try_stream! {
            let stream = resolve_executor!(conn; $conn, exec =>
                sqlx::query_with(&sql, values).$method(exec).map_ok($map_fn).boxed()
            );
            futures_util::pin_mut!(stream);
            while let Some(item) = stream.next().await { yield item?; }
        }.boxed()
    }};
}

macro_rules! dispatch_stream_typed {
    // unwrap
    ((pool; $field:ident), $this:expr, $method:ident, $query:expr, $builder:ident) => {
        dispatch_stream_typed!(pool; $this, $field, $method, $query, $builder)
    };
    ((conn), $this:expr, $method:ident, $query:expr, $builder:ident) => {
        dispatch_stream_typed!(conn; $this, $method, $query, $builder)
    };
    // inner
    (pool; $pool:expr, $field:ident, $method:ident, $query:expr, $builder:ident) => {{
        let (sql, values) = $query.build_sea_query($pool.as_ref());
        async_stream::try_stream! {
            let stream = resolve_executor!(pool; $pool, $field, exec =>
                sqlx::$builder(&sql, values).$method(exec).boxed()
            );
            futures_util::pin_mut!(stream);
            while let Some(item) = stream.next().await { yield item?; }
        }.boxed()
    }};
    (conn; $conn:expr, $method:ident, $query:expr, $builder:ident) => {{
        let (sql, values) = $query.build_sea_query(&$conn);
        async_stream::try_stream! {
            let stream = resolve_executor!(conn; $conn, exec =>
                sqlx::$builder(&sql, values).$method(exec).boxed()
            );
            futures_util::pin_mut!(stream);
            while let Some(item) = stream.next().await { yield item?; }
        }.boxed()
    }};
}

macro_rules! impl_sea_executor {
    ($rctx:tt, $wctx:tt) => {
        fn execute<'e>(
            self,
            query: impl SeaQuerySqlxBuild,
        ) -> BoxFuture<'e, Result<DynamicQueryResult, Error>>
        where
            Self: 'e,
        {
            let this = self;
            dispatch_future_raw!($wctx, this, execute, query, From::from)
        }

        fn fetch<'e>(
            self,
            query: impl SeaQuerySqlxBuild,
        ) -> BoxStream<'e, Result<DynamicRow, Error>>
        where
            Self: 'e,
        {
            let this = self;
            dispatch_stream_raw!($rctx, this, fetch, query, From::from)
        }

        #[allow(deprecated)]
        fn fetch_many<'e>(
            self,
            query: impl SeaQuerySqlxBuild,
        ) -> BoxStream<'e, Result<Either<DynamicQueryResult, DynamicRow>, Error>>
        where
            Self: 'e,
        {
            let this = self;
            dispatch_stream_raw!($rctx, this, fetch_many, query, |r| match r {
                Either::Left(l) => Either::Left(From::from(l)),
                Either::Right(r) => Either::Right(From::from(r)),
            })
        }

        fn fetch_all<'e>(
            self,
            query: impl SeaQuerySqlxBuild,
        ) -> BoxFuture<'e, Result<Vec<DynamicRow>, Error>>
        where
            Self: 'e,
        {
            let this = self;
            dispatch_future_raw!($rctx, this, fetch_all, query, map_rows)
        }

        fn fetch_one<'e>(
            self,
            query: impl SeaQuerySqlxBuild,
        ) -> BoxFuture<'e, Result<DynamicRow, Error>>
        where
            Self: 'e,
        {
            let this = self;
            dispatch_future_raw!($rctx, this, fetch_one, query, From::from)
        }

        fn fetch_optional<'e>(
            self,
            query: impl SeaQuerySqlxBuild,
        ) -> BoxFuture<'e, Result<Option<DynamicRow>, Error>>
        where
            Self: 'e,
        {
            let this = self;
            dispatch_future_raw!($rctx, this, fetch_optional, query, |r| r.map(From::from))
        }

        fn fetch_all_as<'e, O: Send + Unpin + DynamicFromRow>(
            self,
            query: impl SeaQuerySqlxBuild,
        ) -> BoxFuture<'e, Result<Vec<O>, Error>>
        where
            Self: 'e,
        {
            let this = self;
            dispatch_future_typed!($rctx, this, fetch_all, query, query_as_with)
        }

        fn fetch_one_as<'e, O: Send + Unpin + DynamicFromRow>(
            self,
            query: impl SeaQuerySqlxBuild,
        ) -> BoxFuture<'e, Result<O, Error>>
        where
            Self: 'e,
        {
            let this = self;
            dispatch_future_typed!($rctx, this, fetch_one, query, query_as_with)
        }

        fn fetch_optional_as<'e, O: Send + Unpin + DynamicFromRow>(
            self,
            query: impl SeaQuerySqlxBuild,
        ) -> BoxFuture<'e, Result<Option<O>, Error>>
        where
            Self: 'e,
        {
            let this = self;
            dispatch_future_typed!($rctx, this, fetch_optional, query, query_as_with)
        }

        fn fetch_as<'e, O: Send + Unpin + DynamicFromRow + 'e>(
            self,
            query: impl SeaQuerySqlxBuild,
        ) -> BoxStream<'e, Result<O, Error>>
        where
            Self: 'e,
        {
            let this = self;
            dispatch_stream_typed!($rctx, this, fetch, query, query_as_with)
        }

        fn fetch_all_scalar<'e, O: Send + Unpin + DynamicDecode + DynamicType>(
            self,
            query: impl SeaQuerySqlxBuild,
        ) -> BoxFuture<'e, Result<Vec<O>, Error>>
        where
            Self: 'e,
        {
            let this = self;
            dispatch_future_typed!($rctx, this, fetch_all, query, query_scalar_with)
        }

        fn fetch_one_scalar<'e, O: Send + Unpin + DynamicDecode + DynamicType>(
            self,
            query: impl SeaQuerySqlxBuild,
        ) -> BoxFuture<'e, Result<O, Error>>
        where
            Self: 'e,
        {
            let this = self;
            dispatch_future_typed!($rctx, this, fetch_one, query, query_scalar_with)
        }

        fn fetch_optional_scalar<'e, O: Send + Unpin + DynamicType + DynamicDecode>(
            self,
            query: impl SeaQuerySqlxBuild,
        ) -> BoxFuture<'e, Result<Option<O>, Error>>
        where
            Self: 'e,
        {
            let this = self;
            dispatch_future_typed!($rctx, this, fetch_optional, query, query_scalar_with)
        }
    };
}

// Implementations

// `TdPool` delegates to `DynamicPool` via `as_ref()`.
// Writes use the `rw` pool field; reads use `ro`.
impl SeaExecutor for &'_ TdPool {
    type Reused<'a>
        = &'a TdPool
    where
        Self: 'a;
    fn reuse(&mut self) -> Self::Reused<'_> {
        self
    }

    impl_sea_executor!(
        (pool; ro),     // read context
        (pool; rw)      // write context
    );
}

impl SeaExecutor for &'_ DynamicPool {
    type Reused<'a>
        = &'a DynamicPool
    where
        Self: 'a;
    fn reuse(&mut self) -> Self::Reused<'_> {
        self
    }

    impl_sea_executor!(
        (pool; ro),     // read context
        (pool; rw)      // write context
    );
}

impl<'c> SeaExecutor for &'_ mut DynamicConnection<'c> {
    type Reused<'a>
        = &'a mut DynamicConnection<'c>
    where
        Self: 'a;
    fn reuse(&mut self) -> Self::Reused<'_> {
        &mut **self
    }

    impl_sea_executor!(
        (conn), // read context
        (conn)  // write context
    );
}
