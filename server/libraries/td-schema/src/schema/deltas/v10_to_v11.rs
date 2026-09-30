//
// Copyright 2026 Tabsdata Inc.
//

//! v10 → v11: `functions.function_type` records what kind of function a row
//! is, alongside the `decorator` that records how it was declared. Existing
//! rows predate any platform-registered function, so they are all `Regular`.
//!
//! Also adds the nullable parent link `functions.parent_function_id`.

use sea_query::{ColumnDef, Iden, Query, Table};

use crate::schema::render::{render_query, render_schema, update_db_version};
use crate::schema::sql::id_column;
use crate::schema::version::SchemaDelta;

// Deltas are frozen: they carry their own idens instead of borrowing
// `current`'s, so later schema renames can't rewrite migration history.
#[derive(Iden, Clone, Copy)]
enum Functions {
    Table,
    FunctionType,
    ParentFunctionId,
}

/// Frozen: every function registered before v11 was user-authored, so the
/// rung cannot follow a later rename of the enum it backfills.
const FUNCTION_CLASS_REGULAR: &str = "R";

pub struct Migration;

impl SchemaDelta for Migration {
    const FROM_VERSION: usize = 10;
    const TO_VERSION: usize = 11;

    fn retired() -> Vec<String> {
        vec![]
    }

    fn up() -> Vec<String> {
        // Nullable, then backfilled, then constrained: a NOT NULL DEFAULT
        // would linger as a default a fresh install does not have.
        let add_column =
            Table::alter().table(Functions::Table).add_column(ColumnDef::new(Functions::FunctionType).text().null()).to_owned();

        let backfill = Query::update().table(Functions::Table).value(Functions::FunctionType, FUNCTION_CLASS_REGULAR).to_owned();

        let set_not_null =
            Table::alter().table(Functions::Table).modify_column(ColumnDef::new(Functions::FunctionType).text().not_null()).to_owned();

        let add_parent_function_id =
            Table::alter().table(Functions::Table).add_column(id_column(Functions::ParentFunctionId).null()).to_owned();

        vec![
            render_schema(add_column),
            render_query(backfill),
            render_schema(set_not_null),
            render_schema(add_parent_function_id),
            update_db_version(Self::TO_VERSION),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::Migration;
    use crate::schema::tests::validate_delta;

    #[test]
    fn test_validate_delta_v10_to_v11() {
        validate_delta::<Migration>();
    }
}
