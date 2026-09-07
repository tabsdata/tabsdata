//
// Copyright 2026 Tabsdata Inc.
//

//! v2 → v3: `physical_function_runs.base_path` freezes the collection's
//! effective prefix on the run. Existing rows are backfilled with the prefix
//! they resolve to today, which is what the read path recomputed before.

use sea_query::{ColumnDef, Expr, Iden, Query, Table};

use crate::schema::render::{render_query, render_schema, update_db_version};
use crate::schema::version::SchemaDelta;

// Deltas are frozen: they carry their own idens instead of borrowing
// `current`'s, so later schema renames can't rewrite migration history.
#[derive(Iden, Clone, Copy)]
enum PhysicalFunctionRuns {
    Table,
    BasePath,
}

pub struct Migration;

impl SchemaDelta for Migration {
    const FROM_VERSION: usize = 2;
    const TO_VERSION: usize = 3;

    fn up() -> Vec<String> {
        // Nullable, then backfilled, then constrained: a NOT NULL DEFAULT
        // would linger as a default a fresh install does not have.
        let add_column = Table::alter()
            .table(PhysicalFunctionRuns::Table)
            .add_column(ColumnDef::new(PhysicalFunctionRuns::BasePath).text().null())
            .to_owned();

        // The same resolution the mint stamps: the function-version override,
        // else the collection's effective prefix. Base tables, not
        // `collections_resolved`: derived objects are dropped for the ladder.
        let backfill = Query::update()
            .table(PhysicalFunctionRuns::Table)
            .value(
                PhysicalFunctionRuns::BasePath,
                Expr::cust(
                    r#"COALESCE(
                        (SELECT "functions"."base_path" FROM "functions"
                          WHERE "functions"."id" = "physical_function_runs"."function_version_id"),
                        (SELECT "collections"."base_path" FROM "collections"
                          WHERE "collections"."id" = "physical_function_runs"."collection_id"),
                        (SELECT "projects"."base_path" FROM "projects"
                          WHERE "projects"."id" = "physical_function_runs"."project_id"),
                        ''
                    )"#,
                ),
            )
            .to_owned();

        let set_not_null = Table::alter()
            .table(PhysicalFunctionRuns::Table)
            .modify_column(ColumnDef::new(PhysicalFunctionRuns::BasePath).text().not_null())
            .to_owned();

        vec![
            render_schema(add_column),
            render_query(backfill),
            render_schema(set_not_null),
            update_db_version(Self::TO_VERSION),
        ]
    }
}
