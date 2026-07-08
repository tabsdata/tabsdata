//
// Copyright 2026 Tabs Data Inc.
//

//! Database domains: which component's database a table belongs to, carried
//! in the type system. Each DAO declares its [`DbDomain`] and each executor
//! serves one ([`Domained`]); the `td-queries` surfaces require them to
//! match, so a query cannot compile against the wrong pool.

use crate::components::APISERVER_COMPONENT_KEY;
use crate::dynamic::{
    DynamicConnection, DynamicDecode, DynamicFromRow, DynamicPool, DynamicQueryResult, DynamicRow,
    DynamicTransaction, DynamicType,
};
use crate::error::DbError;
use crate::pool::TdPool;
use crate::pool::scoped::Scoped;
use crate::sea_query::builder::SeaQuerySqlxBuild;
use crate::sea_query::executor::SeaExecutor;
use crate::sea_query::kind::{DatabaseBackend, DatabaseKind};
use futures_util::StreamExt;
use futures_util::future::BoxFuture;
use futures_util::stream::BoxStream;
use sqlx::{Either, Error};
use std::collections::BTreeMap;
use std::marker::PhantomData;
use td_error::TdError;

/// One schema component's database, as a zero-sized marker type.
pub trait DbDomain: Send + Sync + Copy + 'static {
    /// The component key its pool registers under (see
    /// [`ServingPools`](crate::components::ServingPools)).
    const SCHEMA_KEY: &'static str;
}

/// The apiserver component's database; the default domain of every DAO.
#[derive(Debug, Clone, Copy)]
pub struct Apiserver;

impl DbDomain for Apiserver {
    const SCHEMA_KEY: &'static str = APISERVER_COMPONENT_KEY;
}

/// The domain an executor serves.
pub trait Domained {
    type Domain: DbDomain;
}

/// What plain executors serve in production builds: no DAO declares it, so a
/// domain-checked query on an unblessed executor does not compile.
#[derive(Debug, Clone, Copy)]
pub struct NoDomain;

impl DbDomain for NoDomain {
    const SCHEMA_KEY: &'static str = "!none";
}

/// What plain executors serve: `Apiserver` in test builds only (the `db` test
/// fixture IS the apiserver pool), `NoDomain` in production.
#[cfg(any(test, feature = "test-utils"))]
type PlainExecutorDomain = Apiserver;
#[cfg(not(any(test, feature = "test-utils")))]
type PlainExecutorDomain = NoDomain;

impl Domained for &'_ mut DynamicConnection<'_> {
    type Domain = PlainExecutorDomain;
}

impl Domained for &'_ TdPool {
    type Domain = PlainExecutorDomain;
}

impl Domained for &'_ DynamicPool {
    type Domain = PlainExecutorDomain;
}

impl<P> Domained for &'_ Scoped<&'_ TdPool, P> {
    type Domain = PlainExecutorDomain;
}

/// An executor over a [`TdPool`] known to be `D`'s.
#[derive(Clone, Copy)]
pub struct DomainPool<'a, D: DbDomain> {
    pool: &'a TdPool,
    _d: PhantomData<D>,
}

impl<'a, D: DbDomain> DomainPool<'a, D> {
    /// Bless `pool` as `D`'s; the caller asserts the provenance.
    pub fn over(pool: &'a TdPool) -> Self {
        Self {
            pool,
            _d: PhantomData,
        }
    }
}

impl<D: DbDomain> Domained for DomainPool<'_, D> {
    type Domain = D;
}

impl<D: DbDomain> DatabaseKind for DomainPool<'_, D> {
    fn kind(&self) -> DatabaseBackend {
        self.pool.kind()
    }
}

/// The [`SeaExecutor`] method set from one signature table: `$future!` /
/// `$stream!` (each taking `(self, query, method)`) supply the bodies.
macro_rules! domain_sea_executor {
    ($future:ident, $stream:ident) => {
        fn execute<'e>(
            self,
            query: impl SeaQuerySqlxBuild,
        ) -> BoxFuture<'e, Result<DynamicQueryResult, Error>>
        where
            Self: 'e,
        {
            let this = self;
            $future!(this, query, execute)
        }

        fn fetch<'e>(
            self,
            query: impl SeaQuerySqlxBuild,
        ) -> BoxStream<'e, Result<DynamicRow, Error>>
        where
            Self: 'e,
        {
            let this = self;
            $stream!(this, query, fetch)
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
            $stream!(this, query, fetch_many)
        }

        fn fetch_all<'e>(
            self,
            query: impl SeaQuerySqlxBuild,
        ) -> BoxFuture<'e, Result<Vec<DynamicRow>, Error>>
        where
            Self: 'e,
        {
            let this = self;
            $future!(this, query, fetch_all)
        }

        fn fetch_one<'e>(
            self,
            query: impl SeaQuerySqlxBuild,
        ) -> BoxFuture<'e, Result<DynamicRow, Error>>
        where
            Self: 'e,
        {
            let this = self;
            $future!(this, query, fetch_one)
        }

        fn fetch_optional<'e>(
            self,
            query: impl SeaQuerySqlxBuild,
        ) -> BoxFuture<'e, Result<Option<DynamicRow>, Error>>
        where
            Self: 'e,
        {
            let this = self;
            $future!(this, query, fetch_optional)
        }

        fn fetch_all_as<'e, O: Send + Unpin + DynamicFromRow>(
            self,
            query: impl SeaQuerySqlxBuild,
        ) -> BoxFuture<'e, Result<Vec<O>, Error>>
        where
            Self: 'e,
        {
            let this = self;
            $future!(this, query, fetch_all_as)
        }

        fn fetch_one_as<'e, O: Send + Unpin + DynamicFromRow>(
            self,
            query: impl SeaQuerySqlxBuild,
        ) -> BoxFuture<'e, Result<O, Error>>
        where
            Self: 'e,
        {
            let this = self;
            $future!(this, query, fetch_one_as)
        }

        fn fetch_optional_as<'e, O: Send + Unpin + DynamicFromRow>(
            self,
            query: impl SeaQuerySqlxBuild,
        ) -> BoxFuture<'e, Result<Option<O>, Error>>
        where
            Self: 'e,
        {
            let this = self;
            $future!(this, query, fetch_optional_as)
        }

        fn fetch_as<'e, O: Send + Unpin + DynamicFromRow + 'e>(
            self,
            query: impl SeaQuerySqlxBuild,
        ) -> BoxStream<'e, Result<O, Error>>
        where
            Self: 'e,
        {
            let this = self;
            $stream!(this, query, fetch_as)
        }

        fn fetch_all_scalar<'e, O: Send + Unpin + DynamicDecode + DynamicType>(
            self,
            query: impl SeaQuerySqlxBuild,
        ) -> BoxFuture<'e, Result<Vec<O>, Error>>
        where
            Self: 'e,
        {
            let this = self;
            $future!(this, query, fetch_all_scalar)
        }

        fn fetch_one_scalar<'e, O: Send + Unpin + DynamicDecode + DynamicType>(
            self,
            query: impl SeaQuerySqlxBuild,
        ) -> BoxFuture<'e, Result<O, Error>>
        where
            Self: 'e,
        {
            let this = self;
            $future!(this, query, fetch_one_scalar)
        }

        fn fetch_optional_scalar<'e, O: Send + Unpin + DynamicType + DynamicDecode>(
            self,
            query: impl SeaQuerySqlxBuild,
        ) -> BoxFuture<'e, Result<Option<O>, Error>>
        where
            Self: 'e,
        {
            let this = self;
            $future!(this, query, fetch_optional_scalar)
        }
    };
}

macro_rules! pool_delegate {
    ($self:ident, $query:ident, $method:ident) => {
        $self.pool.$method($query)
    };
}

impl<'a, D: DbDomain> SeaExecutor for DomainPool<'a, D> {
    type Reused<'r>
        = DomainPool<'r, D>
    where
        Self: 'r;

    fn reuse(&mut self) -> Self::Reused<'_> {
        *self
    }

    domain_sea_executor!(pool_delegate, pool_delegate);
}

/// A service's connection to domain `D`: ready (the primary) or latent (a
/// `read_connection` — the connection materializes on first use, so an
/// untouched domain costs no acquisition). `&mut DomainConnection<D>` is the
/// domain's executor: query it directly (`conn.live()...`).
pub struct DomainConnection<D: DbDomain> {
    state: DomainConnectionState,
    _d: PhantomData<D>,
}

enum DomainConnectionState {
    Ready(DynamicConnection<'static>),
    Latent(TdPool),
}

impl<D: DbDomain> DomainConnection<D> {
    pub fn ready(conn: DynamicConnection<'static>) -> Self {
        Self {
            state: DomainConnectionState::Ready(conn),
            _d: PhantomData,
        }
    }

    /// Bless `tx` as a ready slot for `D` — the boot/boundary-code entry;
    /// recover it with [`into_transaction`](Self::into_transaction) to commit.
    pub fn from_transaction(tx: DynamicTransaction<'static>) -> Self {
        Self::ready(tx.into_connection())
    }

    pub fn latent(pool: TdPool) -> Self {
        Self {
            state: DomainConnectionState::Latent(pool),
            _d: PhantomData,
        }
    }

    async fn materialize(&mut self) -> Result<&mut DynamicConnection<'static>, Error> {
        if let DomainConnectionState::Latent(pool) = &self.state {
            let conn = pool.acquire_for_read().await?;
            self.state = DomainConnectionState::Ready(conn);
        }
        match &mut self.state {
            DomainConnectionState::Ready(conn) => Ok(conn),
            DomainConnectionState::Latent(_) => unreachable!("just materialized"),
        }
    }

    /// The underlying connection, for connection-level operations (ping, GUCs,
    /// advisory locks) — not for queries, which run on the slot itself.
    pub async fn get(&mut self) -> Result<&mut DynamicConnection<'static>, TdError> {
        Ok(self
            .materialize()
            .await
            .map_err(DbError::CannotGetDomainConnection)?)
    }

    /// The ready connection, consumed. `None` for a latent slot.
    pub fn into_ready(self) -> Option<DynamicConnection<'static>> {
        match self.state {
            DomainConnectionState::Ready(conn) => Some(conn),
            DomainConnectionState::Latent(_) => None,
        }
    }

    /// The transaction behind a ready slot, consumed — how boot/boundary code
    /// recovers a [`from_transaction`](Self::from_transaction) blessing for
    /// commit. `None` for a latent slot or a non-transaction connection.
    pub fn into_transaction(self) -> Option<DynamicTransaction<'static>> {
        self.into_ready()?.into_transaction()
    }

    /// The ready connection, borrowed. `None` for a latent slot.
    pub fn ready_mut(&mut self) -> Option<&mut DynamicConnection<'static>> {
        match &mut self.state {
            DomainConnectionState::Ready(conn) => Some(conn),
            DomainConnectionState::Latent(_) => None,
        }
    }

    /// Whether `D`'s database is PostgreSQL — answered statelessly (a latent
    /// slot asks its pool), unlike the connection-level ops behind
    /// [`get`](Self::get).
    pub fn is_postgres(&self) -> bool {
        match self.kind() {
            #[cfg(postgres)]
            DatabaseBackend::Postgres => true,
            #[allow(unreachable_patterns)]
            _ => false,
        }
    }
}

impl<D: DbDomain> Domained for &'_ mut DomainConnection<D> {
    type Domain = D;
}

impl<D: DbDomain> DatabaseKind for DomainConnection<D> {
    fn kind(&self) -> DatabaseBackend {
        match &self.state {
            DomainConnectionState::Ready(conn) => conn.kind(),
            DomainConnectionState::Latent(pool) => pool.kind(),
        }
    }
}

// A ready slot (every primary connection) delegates straight through — no
// extra boxing. A latent slot pre-builds `(sql, values)` synchronously (the
// backend kind is known without a connection, and the future stays `Send`),
// then materializes inside the returned future; acquisition failure surfaces
// as the executor's own `sqlx::Error`.
macro_rules! lazy_future {
    ($self:ident, $query:ident, $method:ident) => {{
        match $self.state {
            DomainConnectionState::Ready(ref mut conn) => conn.$method($query),
            DomainConnectionState::Latent(_) => {
                let built = $query.build_sea_query(&*$self);
                Box::pin(async move {
                    match $self.materialize().await {
                        Ok(conn) => conn.$method(built).await,
                        Err(e) => Err(e),
                    }
                })
            }
        }
    }};
}

macro_rules! lazy_stream {
    ($self:ident, $query:ident, $method:ident) => {{
        match $self.state {
            DomainConnectionState::Ready(ref mut conn) => conn.$method($query),
            DomainConnectionState::Latent(_) => {
                let built = $query.build_sea_query(&*$self);
                Box::pin(
                    futures_util::stream::once(async move {
                        match $self.materialize().await {
                            Ok(conn) => conn.$method(built).left_stream(),
                            Err(e) => futures_util::stream::iter([Err(e)]).right_stream(),
                        }
                    })
                    .flatten(),
                )
            }
        }
    }};
}

impl<D: DbDomain> SeaExecutor for &mut DomainConnection<D> {
    type Reused<'a>
        = &'a mut DomainConnection<D>
    where
        Self: 'a;

    fn reuse(&mut self) -> Self::Reused<'_> {
        self
    }

    domain_sea_executor!(lazy_future, lazy_stream);
}

/// A service's pools as one parameter, keyed by domain (built at boot via
/// [`ServingPools::domain_pools`](crate::components::ServingPools::domain_pools)).
#[derive(Clone)]
#[cfg_attr(any(test, feature = "test-utils"), derive(Default))]
pub struct DomainPools(BTreeMap<&'static str, TdPool>);

impl DomainPools {
    pub(crate) fn from_map(map: BTreeMap<&'static str, TdPool>) -> Self {
        Self(map)
    }

    /// Test-only assembly, one domain at a time: bless `pool` as `D`'s — the
    /// caller asserts the provenance, as with [`DomainPool::over`].
    #[cfg(any(test, feature = "test-utils"))]
    pub fn with<D: DbDomain>(mut self, pool: TdPool) -> Self {
        self.0.insert(D::SCHEMA_KEY, pool);
        self
    }

    /// Errors only if the deployment never registered `D`'s component.
    pub fn get<D: DbDomain>(&self) -> Result<&TdPool, DbError> {
        self.0
            .get(D::SCHEMA_KEY)
            .ok_or_else(|| DbError::UnknownSchemaEntry(D::SCHEMA_KEY.to_owned()))
    }
}
