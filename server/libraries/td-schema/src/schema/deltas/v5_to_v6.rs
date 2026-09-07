//
// Copyright 2026 Tabsdata Inc.
//

//! The SCD2 interval start becomes the `valid_from` column on every interval
//! table: `open(.., at)` binds it from the lock instant, and the payload
//! column that used to double as it (`defined_on` on the definition tables,
//! `triggered_on` on the attempt tables) is renamed rather than duplicated,
//! so a row keeps one instant.

use crate::schema::render::update_db_version;
use crate::schema::version::SchemaDelta;

const RENAMED: &[(&str, &str)] = &[
    ("functions", "defined_on"),
    ("function_patches", "defined_on"),
    ("tables", "defined_on"),
    ("dependencies", "defined_on"),
    ("triggers", "defined_on"),
    ("physical_function_runs", "triggered_on"),
    ("physical_table_data_versions", "triggered_on"),
];

pub struct Migration;

impl SchemaDelta for Migration {
    const FROM_VERSION: usize = 5;
    const TO_VERSION: usize = 6;

    /// The indexes over the renamed columns keep their v5 names past the
    /// rename, so the ladder cannot drop them by their v6 names.
    fn retired() -> Vec<String> {
        [
            "dependencies___dependency_id__defined_on__id___idx",
            "function_patches___function_id__defined_on___idx",
            "function_patches___parent_fv_id__defined_on__id___idx",
            "functions___function_id__defined_on__id___idx",
            "pfr___logical_function_run_id__triggered_on__id___idx",
            "pfr___status__project_id__triggered_on___idx",
            "ptdv___logical_table_data_version_id__triggered_on__id___idx",
            "tables___table_id__defined_on__id___idx",
            "triggers___trigger_id__defined_on__id___idx",
        ]
        .iter()
        .map(|name| format!(r#"DROP INDEX IF EXISTS "{name}""#))
        .collect()
    }

    fn up() -> Vec<String> {
        let mut out: Vec<String> = RENAMED
            .iter()
            .flat_map(|(table, column)| {
                [
                    format!(r#"ALTER TABLE "{table}" RENAME COLUMN "{column}" TO "valid_from""#),
                    format!(r#"ALTER TABLE "{table}" RENAME CONSTRAINT "{table}_{column}_not_null" TO "{table}_valid_from_not_null""#),
                ]
            })
            .collect();
        out.push(update_db_version(Self::TO_VERSION));
        out
    }
}
