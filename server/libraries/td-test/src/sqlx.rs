//
// Copyright 2025 Tabs Data Inc.
//

use crate::{TestSetup, TestSetupExecution};
use async_trait::async_trait;
use sqlx::Executor;
use td_database::sql::{DbPool, DbSchema};

pub struct SqlxTestSetup<'a> {
    schema: Option<&'static DbSchema>,
    fixtures: Vec<&'a str>,
}

impl<'a> SqlxTestSetup<'a> {
    #[allow(dead_code)]
    pub fn new(schema: Option<&'static DbSchema>, fixtures: Vec<&'a str>) -> Self {
        Self { schema, fixtures }
    }
}

#[async_trait]
impl TestSetup<DbPool> for SqlxTestSetup<'_> {
    /// Similar to [`sqlx::testing::setup_test_db`], but generating DbPool.
    async fn setup(&self) -> TestSetupExecution<DbPool> {
        let config = td_database::test_utils::test_config();
        let schema = self.schema.unwrap_or_else(|| {
            if self.fixtures.is_empty() {
                td_schema::schema()
            } else {
                &DbSchema::DEFAULT
            }
        });

        let db = DbPool::new(&config, schema).await.unwrap();

        for fixture in self.fixtures.iter() {
            db.execute(*fixture).await.unwrap();
        }

        TestSetupExecution::Run(db)
    }
}
