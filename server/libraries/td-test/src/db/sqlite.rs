//
// Copyright 2026 Tabs Data Inc.
//

//! SQLite provider. Thin delegate over [`td_database::test_utils::sqlite::pool`].
//! Sqlite stores its database in a temp file with no separate server process,
//! so there's nothing extra for the pool to own beyond the sqlx pool itself.

pub struct SqliteProvider;

#[cfg(sqlite)]
mod imp {
    use super::SqliteProvider;
    use crate::db::DbBackendProvider;
    use std::future::Future;
    use td_database::pool::TdPool;

    impl DbBackendProvider for SqliteProvider {
        const NAME: &'static str = "sqlite";
        fn provision() -> impl Future<Output = TdPool> + Send {
            td_database::test_utils::sqlite::pool()
        }
    }
}
