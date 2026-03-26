//
// Copyright 2026 Tabs Data Inc.
//

//! Migration from v7 to v8:
//!
//! 0. Add `schema_changed` boolean column to `table_data_versions`.
//! 1. Populate `schema_changed` for existing rows.
//! 2. Add `function_version_id` column to `function_requirements`.
//! 3. Populate `function_version_id` from `function_runs`.
//! 4. Add index on `function_requirements.function_version_id`.

use sea_query::{
    Alias, ColumnDef, Expr, ExprTrait, Func, Iden, Index, Order, Query, SimpleExpr, Table,
};

use crate::schema::SchemaDelta;
use crate::schema::backend::Backend;
use tracing::info;

// Local Iden enums — frozen for this delta, independent of current/.

#[derive(Iden)]
enum TableDataVersions {
    Table,
    TableId,
    HasData,
    SchemaHash,
    SchemaChanged,
    TriggeredOn,
}

#[derive(Iden)]
enum FunctionRequirements {
    Table,
    FunctionRunId,
    FunctionVersionId,
}

#[derive(Iden)]
enum FunctionRuns {
    Table,
    Id,
    FunctionVersionId,
}

/// Marker type for the v7-to-v8 migration.
pub struct Migration;

impl SchemaDelta for Migration {
    const FROM_VERSION: usize = 7;
    const TO_VERSION: usize = 8;
    const FINGERPRINTS: [&'static str; 3] = [
        "dd37c4b24297fbe3837e485f8e93d0906c499b20839a1a6eb1d79148e676944a", // Sqlite
        "b003d7ba4a69070a752ec6afb71c670a04d165a35dab4600348302f01947199b", // Postgres
        "1800e5cdc82652bc6ebaa80610f5eba4f94560c51bb0dec9cc628fb1bea277de", // MySql
    ];

    fn up(b: Backend) -> Vec<String> {
        info!(
            from = Self::FROM_VERSION,
            to = Self::TO_VERSION,
            backend = ?b,
            "Applying migration"
        );

        let mut stmts = Vec::new();

        // 0. Add schema_changed column to table_data_versions
        stmts.push(
            b.build_schema(
                Table::alter()
                    .table(TableDataVersions::Table)
                    .add_column(
                        ColumnDef::new(TableDataVersions::SchemaChanged)
                            .boolean()
                            .null(),
                    )
                    .to_owned(),
            ),
        );

        // 1. Populate schema_changed = false where has_data = false
        stmts.push(
            b.build_query(
                Query::update()
                    .table(TableDataVersions::Table)
                    .value(TableDataVersions::SchemaChanged, false)
                    .and_where(Expr::col(TableDataVersions::HasData).eq(false))
                    .to_owned(),
            ),
        );

        // 2. Populate schema_changed for rows where has_data = true.
        //    Compare schema_hash with the previous version's schema_hash for the
        //    same table. If different (or no previous version), schema_changed = true.
        {
            let prev = Alias::new("prev");
            let prev_hash = Query::select()
                .column(TableDataVersions::SchemaHash)
                .from_as(TableDataVersions::Table, prev.clone())
                .and_where(
                    Expr::col((prev.clone(), TableDataVersions::TableId))
                        .equals((TableDataVersions::Table, TableDataVersions::TableId)),
                )
                .and_where(Expr::col((prev.clone(), TableDataVersions::HasData)).eq(true))
                .and_where(
                    Expr::col((prev.clone(), TableDataVersions::TriggeredOn)).lt(Expr::col((
                        TableDataVersions::Table,
                        TableDataVersions::TriggeredOn,
                    ))),
                )
                .order_by((prev, TableDataVersions::TriggeredOn), Order::Desc)
                .limit(1)
                .to_owned();

            let coalesce_args: Vec<SimpleExpr> = vec![prev_hash.into(), Expr::val("")];

            stmts.push(
                b.build_query(
                    Query::update()
                        .table(TableDataVersions::Table)
                        .value(
                            TableDataVersions::SchemaChanged,
                            Expr::case(
                                Expr::col(TableDataVersions::SchemaHash)
                                    .ne(Func::coalesce(coalesce_args)),
                                true,
                            )
                            .finally(false),
                        )
                        .and_where(Expr::col(TableDataVersions::HasData).eq(true))
                        .to_owned(),
                ),
            );
        }

        // 3. Add function_version_id column to function_requirements (NOT NULL, default empty).
        //    The empty default is a placeholder — step 4 populates every row. Every
        //    function_requirement has a function_run_id, and every function_run has a
        //    function_version_id, so no row will remain with the empty default.
        stmts.push(
            b.build_schema(
                Table::alter()
                    .table(FunctionRequirements::Table)
                    .add_column(
                        ColumnDef::new(FunctionRequirements::FunctionVersionId)
                            .text()
                            .not_null()
                            .default(""),
                    )
                    .to_owned(),
            ),
        );

        // 4. Populate function_version_id from function_runs via function_run_id.
        stmts.push(
            b.build_query(
                Query::update()
                    .table(FunctionRequirements::Table)
                    .value(
                        FunctionRequirements::FunctionVersionId,
                        Query::select()
                            .column(FunctionRuns::FunctionVersionId)
                            .from(FunctionRuns::Table)
                            .and_where(Expr::col((FunctionRuns::Table, FunctionRuns::Id)).equals((
                                FunctionRequirements::Table,
                                FunctionRequirements::FunctionRunId,
                            )))
                            .to_owned(),
                    )
                    .to_owned(),
            ),
        );

        // 5. Add index on function_requirements.function_version_id.
        stmts.push(
            b.build_schema(
                Index::create()
                    .name("function_requirements___function_version_id___idx")
                    .table(FunctionRequirements::Table)
                    .col(FunctionRequirements::FunctionVersionId)
                    .to_owned(),
            ),
        );

        // 6. Mandatory version bump.
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
    fn test_validate_delta_v7_to_v8() {
        validate_delta::<Migration>();
    }
}
