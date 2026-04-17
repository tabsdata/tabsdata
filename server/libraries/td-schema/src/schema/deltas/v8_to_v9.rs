//
// Copyright 2026 Tabs Data Inc.
//

//! Migration from v8 to v9:
//!
//! 0. Add index on `function_requirements.execution_id`.
//! 1. Drop `function_runs__to_execute` view (replaced by direct query).
//! 2. Drop `function_runs__to_commit` view (replaced by direct query).

use sea_query::{Iden, Index};

use crate::schema::SchemaDelta;
use crate::schema::backend::Backend;
use tracing::info;

// Local Iden enums — frozen for this delta, independent of current/.

#[derive(Iden)]
enum FunctionRequirements {
    Table,
    ExecutionId,
}

/// Marker type for the v8-to-v9 migration.
pub struct Migration;

impl SchemaDelta for Migration {
    const FROM_VERSION: usize = 8;
    const TO_VERSION: usize = 9;
    const FINGERPRINTS: [&'static str; 3] = [
        "2a2f0a4c67769828433de3d6c9b65b4ee1f19404ad6aae9d1fac9b75130585bc", // Sqlite
        "2a2f0a4c67769828433de3d6c9b65b4ee1f19404ad6aae9d1fac9b75130585bc", // Postgres
        "39d0030c1cda634af0524b06d4613bb25d07a607156507d27c584f51c668c38d", // MySql
    ];

    fn up(b: Backend) -> Vec<String> {
        info!(
            from = Self::FROM_VERSION,
            to = Self::TO_VERSION,
            backend = ?b,
            "Applying migration"
        );

        let stmts = vec![
            // 0. Add index on function_requirements.execution_id for fast lookups
            //    in the function_requirements__with_status view.
            b.build_schema(
                Index::create()
                    .name("function_requirements___execution_id___idx")
                    .table(FunctionRequirements::Table)
                    .col(FunctionRequirements::ExecutionId)
                    .to_owned(),
            ),
        );

        // 1. Drop the function_runs__to_execute view.
        //    Replaced by FunctionRunQueries::fetch_function_runs_to_execute (td-queries).
        stmts.push(b.drop_view("function_runs__to_execute"));

        // 2. Drop the function_runs__to_commit view.
        //    Replaced by FunctionRunQueries::fetch_to_commit (td-queries).
        stmts.push(b.drop_view("function_runs__to_commit"));

        // 3. Mandatory version bump.
        stmts.push(b.update_db_version(Self::TO_VERSION));

        info!(
            from = Self::FROM_VERSION,
            to = Self::TO_VERSION,
            statements = stmts.len(),
            "Migration built"
        );
        stmts
    }
}

#[cfg(test)]
mod tests {
    use super::Migration;
    use crate::schema::tests::validate_delta;

    #[test]
    fn test_validate_delta_v8_to_v9() {
        validate_delta::<Migration>();
    }
}
