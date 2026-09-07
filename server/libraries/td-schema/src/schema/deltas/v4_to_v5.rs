//
// Copyright 2026 Tabsdata Inc.
//

//! v4 → v5: every id column becomes `text COLLATE "C"`. Ids are fixed-width,
//! zero-padded and ASCII, so byte order is their chronological order and the
//! only thing the database default collation added was a locale comparison
//! on every index probe and sort over them.
//!
//! PostgreSQL refuses to alter a column an RLS policy reads, so the two
//! policies of each project-scoped table come off before the `ALTER` and go
//! back after it. The alter rebuilds the column's indexes and rewrites no rows.

use crate::schema::archive::v4::{ENTITIES, project_scoped_tables};
use crate::schema::render::update_db_version;
use crate::schema::sql::{created_table, is_id_column};
use crate::schema::version::SchemaDelta;

/// Every id column of every v4 table, read off the frozen archive definitions.
fn id_columns() -> Vec<(String, Vec<String>)> {
    ENTITIES
        .iter()
        .flat_map(|e| e.create_tables())
        .map(|stmt| {
            let columns = stmt.get_columns().iter().map(|c| c.get_column_name()).filter(|c| is_id_column(c)).collect();
            (created_table(&stmt), columns)
        })
        .filter(|(_, columns): &(String, Vec<String>)| !columns.is_empty())
        .collect()
}

const ID_COLLATION: &str = "COLLATE \"C\"";
const TENANT_PREDICATE: &str = "project_id = current_setting('app.project_id', true)";
const ACROSS_PROJECTS_ROLE: &str = "td_across_projects";

pub struct Migration;

impl SchemaDelta for Migration {
    const FROM_VERSION: usize = 4;
    const TO_VERSION: usize = 5;

    fn retired() -> Vec<String> {
        vec![]
    }

    fn up() -> Vec<String> {
        let mut out = Vec::new();
        let scoped: Vec<String> = project_scoped_tables().iter().map(|t| t.to_string()).collect();
        for table in &scoped {
            out.push(format!("DROP POLICY project_isolation ON {table}"));
            out.push(format!("DROP POLICY across_projects ON {table}"));
        }
        for (table, columns) in id_columns() {
            let alters: Vec<String> = columns.iter().map(|c| format!(r#"ALTER COLUMN "{c}" TYPE text {ID_COLLATION}"#)).collect();
            out.push(format!(r#"ALTER TABLE "{table}" {}"#, alters.join(", ")));
        }
        for table in &scoped {
            out.push(format!("CREATE POLICY project_isolation ON {table} USING ({TENANT_PREDICATE}) WITH CHECK ({TENANT_PREDICATE})"));
            out.push(format!("CREATE POLICY across_projects ON {table} TO {ACROSS_PROJECTS_ROLE} USING (true) WITH CHECK (true)"));
        }
        out.push(update_db_version(Self::TO_VERSION));
        out
    }
}
