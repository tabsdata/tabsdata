//
// Copyright 2026 Tabsdata Inc.
//

//! Db-backed test setup. The macro `#[td_test::test(db, ...)]` expands to a
//! [`DbTestSetup`] that provisions a fresh, isolated `TdPool` per requested
//! component for one test.

use crate::__private::{TestSetup, TestSetupExecution, teardown};
use td_database::components::SchemaComponent;
use td_database::pool::TdPool;
use td_database::test_utils::MultiTestDb;

pub mod postgres;

/// Test setup carrying the components the test asked for (defaulting to the
/// apiserver component): provision each on its own schema and hand the test
/// their serving pools.
pub struct DbTestSetup {
    test_name: &'static str,
    components: &'static [&'static dyn SchemaComponent],
}

impl DbTestSetup {
    #[allow(dead_code)]
    pub fn new(
        test_name: &'static str,
        components: &'static [&'static dyn SchemaComponent],
    ) -> Self {
        Self {
            test_name,
            components,
        }
    }
}

impl TestSetup<Vec<TdPool>> for DbTestSetup {
    async fn setup(&self) -> TestSetupExecution<Vec<TdPool>> {
        // Provision the requested components, each on its own schema. Hand the test
        // their serving pools (pure app pools); the per-test schemas in the tdx
        // domain's database are released by the guard's async teardown after the
        // test body. `None` declines the test — the database it needs is out of
        // reach and the policy says skip rather than fail.
        let Some(MultiTestDb { serving, cleanup }) =
            postgres::provision(self.test_name, self.components).await
        else {
            return TestSetupExecution::Skip;
        };

        TestSetupExecution::Run(serving, teardown(async move { cleanup.run().await }))
    }
}
