//
// Copyright 2026 Tabsdata Inc.
//

//! v7 → v8: every logical id column becomes `bigint`. A logical id's at-rest
//! form is the `i64` whose digits are `<YYYYMMDD><COUNTER>`, so numeric order
//! is chronological — which is the only thing the padded text form it replaces
//! was for.
//!
//! `text` and `bigint` cannot be the two sides of a foreign key, so every key
//! on an affected table comes off before the `ALTER` and goes back after it,
//! restored from the definition PostgreSQL itself prints. A column holding the
//! retired legacy id form (a base32hex UUIDv7, which has no `i64`) stops the
//! ladder rather than being truncated into a wrong id.

use crate::schema::render::update_db_version;
use crate::schema::version::SchemaDelta;

/// Frozen at v8: never re-point this at `current/`.
const LOGICAL_COLUMNS: &[(&str, &[&str])] = &[
    ("executions", &["id", "function_version_id"]),
    ("function_patches", &["id", "parent_function_version_id"]),
    ("functions", &["id"]),
    (
        "logical_function_run_inputs",
        &[
            "execution_id",
            "transaction_id",
            "logical_function_run_id",
            "requirement_logical_table_data_version_id",
        ],
    ),
    ("logical_function_runs", &["id", "execution_id", "transaction_id", "function_version_id"]),
    ("logical_table_data_versions", &["id", "execution_id", "transaction_id", "producer_logical_function_run_id"]),
    (
        "physical_function_runs",
        &[
            "id",
            "function_version_id",
            "function_version_patch_id",
            "execution_id",
            "transaction_id",
            "logical_function_run_id",
        ],
    ),
    (
        "physical_table_data_versions",
        &[
            "id",
            "function_version_id",
            "function_version_patch_id",
            "execution_id",
            "transaction_id",
            "physical_function_run_id",
            "logical_table_data_version_id",
            "with_data_table_data_version_id",
        ],
    ),
    ("table_partitions", &["function_version_id", "table_data_version_id"]),
    ("tables", &["function_version_id"]),
    ("transactions", &["id", "execution_id"]),
    (
        "workers",
        &[
            "id",
            "execution_id",
            "transaction_id",
            "function_version_id",
            "physical_function_run_id",
        ],
    ),
];

/// The one logical column v7 defaults. The empty string it defaulted to is no
/// `bigint`, so both the default and the rows standing on it land here.
const EPOCH_DEFAULT: (&str, &str) = ("logical_function_runs", "function_version_id");
const EPOCH_LOGICAL_ID: i64 = 197001010000000001;

const FK_BACKUP: &str = "td_v8_foreign_keys";

pub struct Migration;

impl SchemaDelta for Migration {
    const FROM_VERSION: usize = 7;
    const TO_VERSION: usize = 8;

    fn retired() -> Vec<String> {
        vec![]
    }

    fn up() -> Vec<String> {
        let tables = LOGICAL_COLUMNS.iter().map(|(table, _)| format!("'{table}'")).collect::<Vec<_>>().join(", ");
        let mut out = vec![
            format!(
                "CREATE TEMP TABLE {FK_BACKUP} AS \
                 SELECT cls.relname AS table_name, con.conname AS constraint_name, \
                 pg_get_constraintdef(con.oid) AS definition \
                 FROM pg_constraint con \
                 JOIN pg_class cls ON cls.oid = con.conrelid \
                 JOIN pg_class ref ON ref.oid = con.confrelid \
                 JOIN pg_namespace ns ON ns.oid = con.connamespace AND ns.nspname = current_schema() \
                 WHERE con.contype = 'f' AND (cls.relname IN ({tables}) OR ref.relname IN ({tables}))"
            ),
            format!(
                "DO $$ DECLARE fk record; BEGIN \
                 FOR fk IN SELECT * FROM {FK_BACKUP} LOOP \
                 EXECUTE format('ALTER TABLE %I DROP CONSTRAINT %I', fk.table_name, fk.constraint_name); \
                 END LOOP; END $$"
            ),
            format!(r#"ALTER TABLE "{}" ALTER COLUMN "{}" DROP DEFAULT"#, EPOCH_DEFAULT.0, EPOCH_DEFAULT.1),
        ];
        for (table, columns) in LOGICAL_COLUMNS {
            let alters: Vec<String> =
                columns.iter().map(|column| format!(r#"ALTER COLUMN "{column}" TYPE bigint USING {}"#, at_rest(table, column))).collect();
            out.push(format!(r#"ALTER TABLE "{table}" {}"#, alters.join(", ")));
        }
        out.push(format!(r#"ALTER TABLE "{}" ALTER COLUMN "{}" SET DEFAULT {EPOCH_LOGICAL_ID}"#, EPOCH_DEFAULT.0, EPOCH_DEFAULT.1));
        out.push(format!(
            "DO $$ DECLARE fk record; BEGIN \
             FOR fk IN SELECT * FROM {FK_BACKUP} LOOP \
             EXECUTE format('ALTER TABLE %I ADD CONSTRAINT %I %s', fk.table_name, fk.constraint_name, fk.definition); \
             END LOOP; END $$"
        ));
        out.push(format!("DROP TABLE {FK_BACKUP}"));
        out.push(update_db_version(Self::TO_VERSION));
        out
    }
}

/// `<KEY><YYYYMMDD>-<COUNTER>` read as the `i64` `<YYYYMMDD><COUNTER>`.
fn at_rest(table: &str, column: &str) -> String {
    let digits = format!(r#"NULLIF(replace(regexp_replace("{column}", '^[^0-9]+', ''), '-', ''), '')::bigint"#);
    if (table, column) == EPOCH_DEFAULT {
        format!("COALESCE({digits}, {EPOCH_LOGICAL_ID})")
    } else {
        digits
    }
}
