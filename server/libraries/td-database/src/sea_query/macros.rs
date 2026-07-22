//
// Copyright 2026 Tabsdata Inc.
//

/// SeaQuery raw_query wrapper to inject database kind based on the pool.
/// pool param should impl AsRef<DynamicPool>
#[macro_export]
macro_rules! raw_query {
    ($pool:expr, $query:tt $(,)?) => {
        match $pool.as_ref() {
            #[cfg(sqlite)]
            $crate::dynamic::DynamicPool::Sqlite { .. } => {
                let query = sea_query::raw_query!(SqliteQueryBuilder, $query);
                $crate::sea_query::raw::RawSeaQuery(query)
            }
            #[cfg(postgres)]
            $crate::dynamic::DynamicPool::Postgres { .. } => {
                let query = sea_query::raw_query!(PostgresQueryBuilder, $query);
                $crate::sea_query::raw::RawSeaQuery(query)
            }
            #[cfg(mysql)]
            $crate::dynamic::DynamicPool::MySql { .. } => {
                let query = sea_query::raw_query!(MysqlQueryBuilder, $query);
                $crate::sea_query::raw::RawSeaQuery(query)
            }
        }
    };
}
