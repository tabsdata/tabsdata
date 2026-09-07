//
// Copyright 2026 Tabsdata Inc.
//

//! PostgreSQL row-level security DDL for project (tenant) isolation.
//!
//! A project is the tenant boundary. Every project-scoped table carries a
//! `project_id` column and an RLS policy that compares it against the
//! `app.project_id` session variable the connection layer sets per request.
//! Cross-project work is a second policy, granted to
//! [`ACROSS_PROJECTS_ROLE`] alone.

use crate::{ACROSS_PROJECTS_ROLE, PROJECT_ID_GUC};

/// Read with the missing-ok (`true`) form, so an unset variable yields NULL
/// and the connection sees no rows rather than raising.
///
/// Must stay a bare equality: an `OR` is index-usable only where every branch
/// constrains an indexed column, so admitting cross-project work here would
/// cost every tenant read a per-row filter over the whole instance.
fn tenant_predicate() -> String {
    format!("project_id = current_setting('{PROJECT_ID_GUC}', true)")
}

/// RLS statements for one project-scoped table. Caller emits these in the
/// schema build's final phase — after seeds, so `FORCE ROW LEVEL SECURITY` plus
/// `WITH CHECK` never reject the un-scoped bootstrap inserts.
pub(crate) fn policy_statements(table: &str) -> Vec<String> {
    let pred = tenant_predicate();
    vec![
        // An unset `app.project_id` yields NULL, which fails NOT NULL —
        // fail-closed. Cross-project writes set `project_id` explicitly.
        format!(
            "ALTER TABLE {table} ALTER COLUMN project_id SET DEFAULT \
             NULLIF(current_setting('{PROJECT_ID_GUC}', true), '')"
        ),
        format!("ALTER TABLE {table} ENABLE ROW LEVEL SECURITY"),
        format!("ALTER TABLE {table} FORCE ROW LEVEL SECURITY"),
        format!("CREATE POLICY project_isolation ON {table} USING ({pred}) WITH CHECK ({pred})"),
        format!(
            "CREATE POLICY across_projects ON {table} TO {ACROSS_PROJECTS_ROLE} \
             USING (true) WITH CHECK (true)"
        ),
    ]
}
