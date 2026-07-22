//
// Copyright 2026 Tabsdata Inc.
//

//! Db-backed test setup. The macro `#[td_test::test(db, ...)]` expands to one
//! `DbTestSetup<P>` per enabled backend `P`, where each `P` provisions a
//! fresh, isolated `TdPool` for one test.

use crate::__private::{TestSetup, TestSetupExecution, teardown};
use std::future::Future;
use std::marker::PhantomData;
use td_database::components::SchemaComponent;
use td_database::pool::TdPool;
use td_database::test_utils::MultiTestDb;

pub mod postgres;
pub mod sqlite;

/// Default value for the `TD_TEST_BACKENDS` env var. Postgres is the default
/// Sqlite and mysql arms still exist; opt in with e.g. `TD_TEST_BACKENDS=sqlite`
/// or`TD_TEST_BACKENDS=postgres,sqlite,mysql`.
const DEFAULT_BACKENDS: &str = "postgres";
const BACKENDS_ENV: &str = "TD_TEST_BACKENDS";

/// Backend-specific provisioning for one test: provision the requested
/// [`SchemaComponent`]s — each on its own schema — and return a serving pool per
/// component. Each impl owns whatever per-test isolation matches its backend
/// (sqlite: temp file; postgres: fresh embedded server parked inside the returned
/// [`MultiTestDb`]). When the test fn finishes the pools drop, taking any owned
/// per-test resources with them. No singletons, no atexit, no leaks.
pub trait DbBackendProvider: Sized {
    /// Stable identifier matching the cfg/feature name (`sqlite`, `postgres`,
    /// `mysql`). Used by `TD_TEST_BACKENDS` filtering.
    const NAME: &'static str;
    /// Build the test database, provisioning the given components. The macro
    /// calls this once per test, per enabled backend.
    fn provision(
        components: &'static [&'static dyn SchemaComponent],
    ) -> impl Future<Output = MultiTestDb> + Send;
}

/// Generic test setup parameterized by a backend provider. The macro emits one
/// instance per enabled backend, carrying the components the test asked for
/// (defaulting to the apiserver component).
pub struct DbTestSetup<P> {
    components: &'static [&'static dyn SchemaComponent],
    _provider: PhantomData<P>,
}

impl<P> DbTestSetup<P> {
    #[allow(dead_code)]
    pub fn new(components: &'static [&'static dyn SchemaComponent]) -> Self {
        Self {
            components,
            _provider: PhantomData,
        }
    }
}

impl<P: DbBackendProvider + Send + Sync> TestSetup<Vec<TdPool>> for DbTestSetup<P> {
    async fn setup(&self) -> TestSetupExecution<Vec<TdPool>> {
        let enabled = std::env::var(BACKENDS_ENV).unwrap_or_else(|_| DEFAULT_BACKENDS.to_string());
        if !enabled.split(',').map(str::trim).any(|b| b == P::NAME) {
            return TestSetupExecution::Skip;
        }

        // Provision the requested components, each on its own schema. Hand the test
        // their serving pools (pure app pools); the per-test database resources
        // (postgres: per-test schemas on the shared server; sqlite: a temp-file
        // connection) are released by the guard's async teardown after the test body.
        let MultiTestDb { serving, cleanup } = P::provision(self.components).await;

        TestSetupExecution::Run(serving, teardown(async move { cleanup.run().await }))
    }
}
