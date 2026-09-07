//
// Copyright 2026 Tabsdata Inc.
//

//! v3 → v4: versioned tables become SCD2. Each version row gets a `valid_to`
//! (`'infinity'` for the open one), backfilled in the `(version_by, id)`
//! order the close-out trigger uses, and the `_current` snapshot tables go.
//!
//! The pointer event logs are dropped rather than converted: the attempt
//! tables become SCD2 tables partitioned by their logical row and ordered by
//! `triggered_on`, so the newest attempt is the current one. The logs'
//! `Pending`/`Voided` events, `set_by_id` and `reason` had no readers.
//!
//! RLS splits too: the cross-project branch leaves the `project_isolation`
//! predicate for its own role-targeted `across_projects` policy.

use crate::schema::archive::v3::project_scoped_tables;
use crate::schema::render::update_db_version;
use crate::schema::version::SchemaDelta;

// Deltas are frozen: they carry their own table/column names instead of
// borrowing `current`'s, so later schema renames can't rewrite migration
// history.
struct Scd2Table {
    table: &'static str,
    partition: &'static str,
    version_by: &'static str,
}

const SCD2_TABLES: &[Scd2Table] = &[
    Scd2Table {
        table: "functions",
        partition: "function_id",
        version_by: "defined_on",
    },
    Scd2Table {
        table: "function_patches",
        partition: "parent_function_version_id",
        version_by: "defined_on",
    },
    Scd2Table {
        table: "tables",
        partition: "table_id",
        version_by: "defined_on",
    },
    Scd2Table {
        table: "dependencies",
        partition: "dependency_id",
        version_by: "defined_on",
    },
    Scd2Table {
        table: "triggers",
        partition: "trigger_id",
        version_by: "defined_on",
    },
];

// Deletion is the closed interval with no successor, so the tombstone
// versions go; the backfill already ended their predecessors at the
// tombstone's instant. Referrers first.
const TOMBSTONED_TABLES: &[&str] = &["dependencies", "triggers", "tables", "functions"];

const CURRENT_TABLES: &[&str] = &[
    "functions_current",
    "function_patches_current",
    "tables_current",
    "dependencies_current",
    "triggers_current",
    "logical_function_run_pointers_current",
    "logical_table_data_version_pointers_current",
];

struct Attempts {
    log: &'static str,
    logical_col: &'static str,
    logical_table: &'static str,
    concrete_col: &'static str,
    table: &'static str,
}

const ATTEMPTS: &[Attempts] = &[
    Attempts {
        log: "logical_function_run_pointers",
        logical_col: "logical_function_run_id",
        logical_table: "logical_function_runs",
        concrete_col: "physical_function_run_id",
        table: "physical_function_runs",
    },
    Attempts {
        log: "logical_table_data_version_pointers",
        logical_col: "logical_table_data_version_id",
        logical_table: "logical_table_data_versions",
        concrete_col: "physical_table_data_version_id",
        table: "physical_table_data_versions",
    },
];

const RETIRED_VIEWS: &[&str] = &[
    "dependencies_current__with_names",
    "functions_current__with_names",
    "tables_current__with_names",
    "triggers_current__with_names",
];

const RETIRED_INDEXES: &[&str] = &[
    "bundles___collection_id___idx",
    "collections___group_id___idx",
    "dependencies___table_function_id___idx",
    "dependencies_current___function_id___idx",
    "dependencies_current___table_function_id___idx",
    "executions___collection_id___idx",
    "executions___function_version_id___idx",
    "executions___triggered_by_id___idx",
    "executions___triggered_on___idx",
    "function_patches___parent_function_version_id__defined_on__id___idx",
    "function_runs___collection_id___idx",
    "function_runs___ended_on___idx",
    "function_runs___execution_id__status___idx",
    "function_runs___function_version_id___idx",
    "function_runs___status___idx",
    "function_runs___transaction_id__status___idx",
    "functions___collection_id___idx",
    "functions_current___collection_id___idx",
    "functions_current___status___idx",
    "lfr_pointers_current___physical_function_run_id___idx",
    "logical_function_run_inputs___requirement_logical_table_data_version_id___idx",
    "logical_function_run_pointers___function_run_id___idx",
    "logical_function_run_pointers___logical_function_run_id__set_on__id___idx",
    "logical_function_runs___group_id__collection_id___idx",
    "logical_function_runs___status___idx",
    "logical_table_data_version_pointers___logical_table_data_version_id__set_on__id___idx",
    "logical_table_data_version_pointers___table_data_version_id___idx",
    "logical_table_data_versions___execution_id___idx",
    "logical_table_data_versions___group_id___idx",
    "logical_table_data_versions___producer_logical_function_run_id___idx",
    "logical_table_data_versions___transaction_id___idx",
    "permissions___entity_id___idx",
    "physical_table_data_versions___collection_id___idx",
    "physical_table_data_versions___execution_id___idx",
    "physical_table_data_versions___physical_function_run_id___idx",
    "physical_table_data_versions___table_id___has_data___idx",
    "physical_table_data_versions___table_id__triggered_on__id___idx",
    "tables___status___idx",
    "tables_current___collection_id___idx",
    "tables_current___function_id___idx",
    "tables_current___status___idx",
    "transactions___collection_id___idx",
    "transactions___group_id__collection_id___idx",
    "triggers___collection_id___idx",
    "triggers___trigger_by_function_id___idx",
    "triggers_current___collection_id___idx",
    "triggers_current___function_id___idx",
    "triggers_current___trigger_by_function_id___idx",
    "users___enabled___idx",
    "users___system___idx",
];

const TENANT_PREDICATE: &str = "project_id = current_setting('app.project_id', true)";
const ACROSS_PROJECTS_ROLE: &str = "td_across_projects";

pub struct Migration;

impl SchemaDelta for Migration {
    const FROM_VERSION: usize = 3;
    const TO_VERSION: usize = 4;

    fn retired() -> Vec<String> {
        let mut out = Vec::new();
        for view in RETIRED_VIEWS {
            out.push(format!("DROP VIEW IF EXISTS {view}"));
        }
        for t in SCD2_TABLES {
            let table = t.table;
            out.push(format!("DROP TRIGGER IF EXISTS {table}__maintain_current ON {table}"));
            out.push(format!("DROP FUNCTION IF EXISTS {table}__maintain_current_fn()"));
        }
        for a in ATTEMPTS {
            let log = a.log;
            out.push(format!("DROP TRIGGER IF EXISTS {log}__maintain_current ON {log}"));
            out.push(format!("DROP TRIGGER IF EXISTS {log}__immutable_no_update ON {log}"));
            out.push(format!("DROP TRIGGER IF EXISTS {log}__immutable_no_delete ON {log}"));
            out.push(format!("DROP FUNCTION IF EXISTS {log}__maintain_current_fn()"));
            out.push(format!("DROP FUNCTION IF EXISTS {log}__immutable_fn()"));
        }
        for index in RETIRED_INDEXES {
            out.push(format!(r#"DROP INDEX IF EXISTS "{index}""#));
        }
        out
    }

    fn up() -> Vec<String> {
        let mut out = Vec::new();

        for a in ATTEMPTS {
            let (log, logical_col, concrete_col) = (a.log, a.logical_col, a.concrete_col);
            let table = a.table;
            // Fresh-install column order: the with-names views expand `*`.
            out.push(format!(r#"ALTER TABLE "{table}" ADD COLUMN "{logical_col}" text NULL"#));
            out.push(format!(r#"ALTER TABLE "{table}" ADD COLUMN "valid_to" timestamptz NULL"#));
            out.push(format!(
                r#"UPDATE "{table}" SET "{logical_col}" = e."{logical_col}" FROM "{log}" AS e WHERE e."{concrete_col}" = "{table}"."id""#,
            ));
        }
        // The structural fallback covers attempts the log never pointed at
        // (a failed attempt whose only event is its retry's).
        out.push(
            r#"UPDATE "physical_function_runs" SET "logical_function_run_id" = l."id" FROM "logical_function_runs" AS l WHERE "physical_function_runs"."logical_function_run_id" IS NULL AND l."execution_id" = "physical_function_runs"."execution_id" AND l."function_id" = "physical_function_runs"."function_id""#
                .to_string(),
        );
        out.push(
            r#"UPDATE "physical_table_data_versions" SET "logical_table_data_version_id" = l."id" FROM "logical_table_data_versions" AS l, "physical_function_runs" AS p WHERE "physical_table_data_versions"."logical_table_data_version_id" IS NULL AND p."id" = "physical_table_data_versions"."physical_function_run_id" AND l."producer_logical_function_run_id" = p."logical_function_run_id" AND l."table_id" = "physical_table_data_versions"."table_id""#
                .to_string(),
        );
        // Unnamed, so PostgreSQL derives `<table>_<column>_fkey` exactly as
        // the fresh install's inline declaration does.
        for a in ATTEMPTS {
            let (table, logical_col, logical_table) = (a.table, a.logical_col, a.logical_table);
            out.push(format!(r#"ALTER TABLE "{table}" ALTER COLUMN "{logical_col}" SET NOT NULL"#));
            out.push(format!(
                r#"ALTER TABLE "{table}" ADD FOREIGN KEY ("{logical_col}") REFERENCES "{logical_table}" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT"#
            ));
        }

        // Nullable, then backfilled, then constrained: a NOT NULL DEFAULT
        // would linger as a default a fresh install does not have.
        for t in SCD2_TABLES {
            let table = t.table;
            out.push(format!(r#"ALTER TABLE "{table}" ADD COLUMN "valid_to" timestamptz NULL"#));
        }
        let attempts: Vec<Scd2Table> = ATTEMPTS
            .iter()
            .map(|a| Scd2Table {
                table: a.table,
                partition: a.logical_col,
                version_by: "triggered_on",
            })
            .collect();
        for t in SCD2_TABLES.iter().chain(attempts.iter()) {
            let (table, partition, version_by) = (t.table, t.partition, t.version_by);
            out.push(format!(
                r#"UPDATE "{table}" SET "valid_to" = COALESCE(n."next_valid_from", 'infinity') FROM (SELECT "id", LEAD("{version_by}") OVER (PARTITION BY "{partition}" ORDER BY "{version_by}", "id") AS "next_valid_from" FROM "{table}") AS n WHERE "{table}"."id" = n."id""#,
            ));
            out.push(format!(r#"ALTER TABLE "{table}" ALTER COLUMN "valid_to" SET NOT NULL"#));
        }

        // v3 parented the table versions a delete wrote (frozen user tables,
        // re-registered system tables) to the tombstone version. They keep
        // their history under the last real version, so the tombstone can go.
        out.push(
            r#"UPDATE "tables" SET "function_version_id" = (SELECT f."id" FROM "functions" AS f WHERE f."function_id" = "tables"."function_id" AND f."status" <> 'D' ORDER BY f."defined_on" DESC, f."id" DESC LIMIT 1) WHERE "function_version_id" IN (SELECT "id" FROM "functions" WHERE "status" = 'D')"#
                .to_string(),
        );
        for table in TOMBSTONED_TABLES {
            out.push(format!(r#"DELETE FROM "{table}" WHERE "status" = 'D'"#));
        }
        out.push(r#"ALTER TABLE "dependencies" DROP COLUMN "status""#.to_string());

        // A v3 function delete re-registered the function's system tables and
        // system dependencies under its tombstone version, so they outlive the
        // function as open rows. v4 closes them with the function.
        for (table, extra) in [
            ("tables", r#" AND "tables"."status" = 'A' AND "tables"."name" LIKE 'td.fn\_state\_\_%'"#),
            ("dependencies", ""),
            ("triggers", ""),
        ] {
            out.push(format!(
                r#"UPDATE "{table}" SET "valid_to" = f."closed_on" FROM (SELECT "function_id", MAX("valid_to") AS "closed_on" FROM "functions" GROUP BY "function_id" HAVING MAX("valid_to") < 'infinity') AS f WHERE "{table}"."function_id" = f."function_id" AND "{table}"."valid_to" = 'infinity'{extra}"#,
            ));
        }

        for table in CURRENT_TABLES {
            out.push(format!(r#"DROP TABLE "{table}""#));
        }

        for a in ATTEMPTS {
            let log = a.log;
            out.push(format!(r#"DROP TABLE "{log}""#));
        }

        // v3 policed the `_current` snapshots and pointer logs too; they are
        // gone by here, policies included.
        let dropped = |table: &str| CURRENT_TABLES.contains(&table) || ATTEMPTS.iter().any(|a| a.log == table);
        for table in project_scoped_tables().iter().map(|t| t.to_string()) {
            if dropped(&table) {
                continue;
            }
            out.push(format!("DROP POLICY project_isolation ON {table}"));
            out.push(format!("CREATE POLICY project_isolation ON {table} USING ({TENANT_PREDICATE}) WITH CHECK ({TENANT_PREDICATE})"));
            out.push(format!("CREATE POLICY across_projects ON {table} TO {ACROSS_PROJECTS_ROLE} USING (true) WITH CHECK (true)"));
        }

        out.push(update_db_version(Self::TO_VERSION));
        out
    }
}
