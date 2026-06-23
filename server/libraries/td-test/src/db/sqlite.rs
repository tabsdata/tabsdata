//
// Copyright 2026 Tabs Data Inc.
//

//! SQLite provider. Thin delegate over [`td_database::test_utils::sqlite::pool`].
//! Sqlite stores its database in a temp file with no separate server process,
//! so there's nothing extra to own beyond the sqlx pools themselves.

pub struct SqliteProvider;

#[cfg(sqlite)]
mod imp {
    use super::SqliteProvider;
    use crate::db::DbBackendProvider;
    use std::future::Future;
    use td_database::system::SchemaComponent;
    use td_database::test_utils::MultiTestDb;

    impl DbBackendProvider for SqliteProvider {
        const NAME: &'static str = "sqlite";
        fn provision(
            components: &'static [&'static dyn SchemaComponent],
        ) -> impl Future<Output = MultiTestDb> + Send {
            td_database::test_utils::sqlite::pools(components)
        }
    }
}
