//
// Copyright 2026 Tabs Data Inc.
//

//! Migration from v8 to v9:
//!
//! 0. Add index on `function_requirements.execution_id`.
//! 1. Drop `function_runs__to_execute` view (replaced by direct query).
//! 2. Drop `function_runs__to_commit` view (replaced by direct query).
//! 3. Drop `function_requirements__with_names` view (depends on `__with_status`).
//! 4. Drop `function_requirements__with_status` view (no longer needed).
//! 5. Recreate `function_requirements__with_names` joining directly from base table.

use sea_query::{Alias, Asterisk, Expr, ExprTrait, Iden, Index, JoinType, Query};

use crate::schema::SchemaDelta;
use crate::schema::backend::{Backend, ViewDef};
use tracing::info;

// Local Iden enums — frozen for this delta, independent of current/.

#[derive(Iden)]
enum FunctionRequirements {
    Table,
    ExecutionId,
    CollectionId,
    RequirementTableVersionId,
    RequirementFunctionVersionId,
}

#[derive(Iden)]
enum Collections {
    Table,
    Id,
    Name,
}

#[derive(Iden)]
enum Tables {
    Table,
    Id,
}

#[derive(Iden)]
enum Functions {
    Table,
    Id,
    Name,
}

/// Marker type for the v8-to-v9 migration.
pub struct Migration;

impl SchemaDelta for Migration {
    const FROM_VERSION: usize = 8;
    const TO_VERSION: usize = 9;
    const FINGERPRINTS: [&'static str; 3] = [
        "c42a11c0b694c6dd318e16029569c76229a62c28e4e7a1f0dc12c66cdb1bd152", // Sqlite
        "c42a11c0b694c6dd318e16029569c76229a62c28e4e7a1f0dc12c66cdb1bd152", // Postgres
        "9e626fcddd49898b208711165a89dd77bf8522f9b01701a1dd4a21cc8d6d75b6", // MySql
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
            // 1. Drop the function_runs__to_execute view.
            //    Replaced by FunctionRunQueries::fetch_function_runs_to_execute (td-queries).
            b.drop_view("function_runs__to_execute"),
            // 2. Drop the function_runs__to_commit view.
            //    Replaced by FunctionRunQueries::fetch_to_commit (td-queries).
            b.drop_view("function_runs__to_commit"),
            // 3. Drop function_requirements__with_names (depends on __with_status).
            b.drop_view("function_requirements__with_names"),
            // 4. Drop function_requirements__with_status (no longer needed).
            b.drop_view("function_requirements__with_status"),
            // 5. Recreate function_requirements__with_names joining directly from
            //    the base function_requirements table.
            b.create_view(&ViewDef::new(
                "function_requirements__with_names",
                Query::select()
                    .expr(Expr::col((FunctionRequirements::Table, Asterisk)))
                    .expr_as(
                        Expr::col((Collections::Table, Collections::Name)),
                        Alias::new("collection"),
                    )
                    .expr_as(
                        Expr::col((Functions::Table, Functions::Name)),
                        Alias::new("function"),
                    )
                    .expr_as(
                        Expr::col((Tables::Table, Alias::new("name"))),
                        Alias::new("requirement_table"),
                    )
                    .from(FunctionRequirements::Table)
                    .join(
                        JoinType::LeftJoin,
                        Collections::Table,
                        Expr::col((
                            FunctionRequirements::Table,
                            FunctionRequirements::CollectionId,
                        ))
                        .equals((Collections::Table, Collections::Id)),
                    )
                    .join(
                        JoinType::LeftJoin,
                        Tables::Table,
                        Expr::col((
                            FunctionRequirements::Table,
                            FunctionRequirements::RequirementTableVersionId,
                        ))
                        .equals((Tables::Table, Tables::Id)),
                    )
                    .join(
                        JoinType::LeftJoin,
                        Functions::Table,
                        Expr::col((
                            FunctionRequirements::Table,
                            FunctionRequirements::RequirementFunctionVersionId,
                        ))
                        .equals((Functions::Table, Functions::Id)),
                    )
                    .to_owned(),
            )),
            // 6. Mandatory version bump.
            b.update_db_version(Self::TO_VERSION),
        ];

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
