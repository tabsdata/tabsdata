//
// Copyright 2026 Tabs Data Inc.
//

//! Db-backed test setup. The macro `#[td_test::test(db, ...)]` expands to one
//! `DbTestSetup<P>` per enabled backend `P`, where each `P` provisions a
//! fresh, isolated `TdPool` for one test.

use crate::__private::{TestSetup, TestSetupExecution};
use std::future::Future;
use std::marker::PhantomData;
use td_database::pool::TdPool;
use td_database::sea_query::kind::DatabaseKind;
use td_schema::schema::backend::Backend;

pub mod postgres;
pub mod sqlite;

/// Default value for the `TD_TEST_BACKENDS` env var. Postgres is the default
/// Sqlite and mysql arms still exist; opt in with e.g. `TD_TEST_BACKENDS=sqlite`
/// or`TD_TEST_BACKENDS=postgres,sqlite,mysql`.
const DEFAULT_BACKENDS: &str = "postgres";
const BACKENDS_ENV: &str = "TD_TEST_BACKENDS";

/// Backend-specific provisioning of a fresh `TdPool` for one test. Each impl
/// owns whatever per-test isolation matches its backend (sqlite: temp file;
/// postgres: fresh embedded server parked inside the returned `TdPool`).
/// When the test fn finishes the pool drops, taking any owned per-test
/// resources with it. No singletons, no atexit, no leaks.
pub trait DbBackendProvider: Sized {
    /// Stable identifier matching the cfg/feature name (`sqlite`, `postgres`,
    /// `mysql`). Used by `TD_TEST_BACKENDS` filtering.
    const NAME: &'static str;
    /// Build the pool. The macro calls this once per test, per enabled
    /// backend.
    fn provision() -> impl Future<Output = TdPool> + Send;
}

/// Generic test setup parameterized by a backend provider. The macro emits one
/// instance per enabled backend.
pub struct DbTestSetup<P> {
    install_schema: bool,
    fixture_builders: Vec<fn(Backend) -> Vec<String>>,
    _provider: PhantomData<P>,
}

impl<P> DbTestSetup<P> {
    #[allow(dead_code)]
    pub fn new(install_schema: bool, fixture_builders: Vec<fn(Backend) -> Vec<String>>) -> Self {
        Self {
            install_schema,
            fixture_builders,
            _provider: PhantomData,
        }
    }
}

impl<P: DbBackendProvider + Send + Sync> TestSetup<TdPool> for DbTestSetup<P> {
    async fn setup(&self) -> TestSetupExecution<TdPool> {
        let enabled = std::env::var(BACKENDS_ENV).unwrap_or_else(|_| DEFAULT_BACKENDS.to_string());
        if !enabled.split(',').map(str::trim).any(|b| b == P::NAME) {
            return TestSetupExecution::Skip;
        }

        let db = P::provision().await;

        if self.install_schema {
            db.upgrade().await.unwrap();
        }

        if !self.fixture_builders.is_empty() {
            let backend = db.kind().into();
            let stmts: Vec<String> = self
                .fixture_builders
                .iter()
                .flat_map(|f| f(backend))
                .collect();
            let mut tx = db.pool.begin().await.unwrap();
            tx.as_mut_conn().execute_batch(&stmts).await.unwrap();
            tx.commit().await.unwrap();
        }

        TestSetupExecution::Run(db)
    }
}
