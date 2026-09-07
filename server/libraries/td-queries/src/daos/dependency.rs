//
// Copyright 2026 Tabsdata Inc.
//

//! Per-DAO queries for `DependencyDB`.

use crate::Serves;
use crate::primitives::scd2::select_visible_at;
use ta_objects::Dao;
use ta_objects::errors::SqlError;
use td_database::sea_query::builder::SeaQueryExt;
use td_objects::dxo::dependency::DependencyDB;
use td_objects::types::basic::{AtTime, FunctionId};

/// `dependencies`-table operations.
pub struct Dependencies<E> {
    pub(crate) executor: E,
}

impl<E> Dependencies<E>
where
    E: Serves<DependencyDB>,
{
    pub async fn active_for_function_at(
        self,
        function_id: &FunctionId,
        on: &AtTime,
    ) -> Result<Vec<DependencyDB>, SqlError> {
        select_visible_at::<DependencyDB, _>(function_id, on)
            .order_by_columns(DependencyDB::order_by())
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<DependencyDB>)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};
    use sea_query::PostgresQueryBuilder;
    use td_objects::types::basic::AtTime;

    #[test]
    fn test_active_for_function_at_sql() {
        let function_id = FunctionId::try_from("00000000000000000000000000").unwrap();
        let on = AtTime::from(
            "2026-01-01T00:00:10.000+00:00"
                .parse::<DateTime<Utc>>()
                .unwrap(),
        );
        let sql = select_visible_at::<DependencyDB, _>(&function_id, &on)
            .order_by_columns(DependencyDB::order_by())
            .to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "id", "collection_id", "dependency_id", "function_id", "table_collection_id", "table_function_id", "table_id", "table_versions", "dep_pos", "defined_by_id", "system", "project_id", "valid_from", "valid_to" FROM "dependencies" WHERE "function_id" = '00000000000000000000000000' AND ("valid_from" <= '2026-01-01 00:00:10.000000 +00:00' AND "valid_to" > '2026-01-01 00:00:10.000000 +00:00') ORDER BY "dep_pos" DESC"#
        );
    }
}
