//
// Copyright 2026 Tabs Data Inc.
//

//! Migration from v7 to v8:
//!
//! 0. Add `schema_changed` boolean column to `table_data_versions`.
//! 1. Populate `schema_changed` for existing rows.

use sea_query::{Alias, ColumnDef, Expr, ExprTrait, Func, Iden, Order, Query, SimpleExpr, Table};

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

/// Marker type for the v7-to-v8 migration.
pub struct Migration;

impl SchemaDelta for Migration {
    const FROM_VERSION: usize = 7;
    const TO_VERSION: usize = 8;
    const FINGERPRINTS: [&'static str; 3] = [
        "18a342abf21d83f1f0f83876feab7c0403518217f1761f5bd77ef6b32bfd1407", // Sqlite
        "de9af0bfc2ccf1ccb57ad365f5a8c8730d062db9d6eedb62e7823fd6756fa332", // Postgres
        "5ede68f31e0c015aad2e7b88f28d0b4ed73a343d16b4ee0fa46ded15f57de9cd", // MySql
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
                    Expr::col((prev.clone(), TableDataVersions::TriggeredOn))
                        .lt(Expr::col((
                            TableDataVersions::Table,
                            TableDataVersions::TriggeredOn,
                        ))),
                )
                .order_by((prev, TableDataVersions::TriggeredOn), Order::Desc)
                .limit(1)
                .to_owned();

            let coalesce_args: Vec<SimpleExpr> = vec![
                prev_hash.into(),
                Expr::val("").into(),
            ];

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
    fn test_validate_delta_v7_to_v8() {
        validate_delta::<Migration>();
    }
}
