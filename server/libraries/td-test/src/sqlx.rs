//
// Copyright 2025 Tabs Data Inc.
//

use sqlx::Executor;
use td_database::sql::{DbPool, DbSchema};

/// Similar to [`sqlx::testing::setup_test_db`], but generating DbPool.
pub async fn setup_test_db(schema: Option<&'static DbSchema>, fixtures: Vec<&str>) -> DbPool {
    let config = td_database::test_utils::test_config();
    let schema = schema.unwrap_or_else(|| {
        if fixtures.is_empty() {
            td_schema::schema()
        } else {
            &DbSchema::DEFAULT
        }
    });

    let db = DbPool::new(&config, schema).await.unwrap();

    for fixture in fixtures {
        db.execute(fixture).await.unwrap();
    }

    db
}
