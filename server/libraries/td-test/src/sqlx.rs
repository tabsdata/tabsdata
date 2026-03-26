//
// Copyright 2025 Tabs Data Inc.
//

use crate::{TestSetup, TestSetupExecution};
use td_database::pool::TdPool;
use td_database::sea_query::kind::DatabaseKind;
use td_schema::schema::backend::Backend;

pub struct SqlxTestSetup {
    install_schema: bool,
    fixture_builders: Vec<fn(Backend) -> Vec<String>>,
}

impl SqlxTestSetup {
    #[allow(dead_code)]
    pub fn new(install_schema: bool, fixture_builders: Vec<fn(Backend) -> Vec<String>>) -> Self {
        Self {
            install_schema,
            fixture_builders,
        }
    }
}

impl TestSetup<TdPool> for SqlxTestSetup {
    /// Similar to [`sqlx::testing::setup_test_db`], but generating TdPool.
    async fn setup(&self) -> TestSetupExecution<TdPool> {
        let config = td_database::test_utils::test_config();
        let db = TdPool::connect(&config).await.unwrap();

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
