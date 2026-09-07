//
// Copyright 2026 Tabsdata Inc.
//

//! PostgreSQL row-level security DDL for project (tenant) isolation.
//!
//! A project is the tenant boundary. Every project-scoped table carries a
//! `project_id` column and an RLS policy that compares it against the
//! `app.project_id` session variable the connection layer sets per request.

use crate::{BYPASS_RLS_GUC, PROJECT_ID_GUC};

/// Predicate shared by a policy's `USING` (reads/updates/deletes) and
/// `WITH CHECK` (inserts/updates) clauses.
///
/// A row is visible/writable when its `project_id` matches the request's
/// `app.project_id`, or when `app.bypass_rls` is `on` (system / scheduler /
/// admin work that must span projects). Both settings are read with the
/// missing-ok (`true`) form of `current_setting`, so an unset variable yields
/// NULL — the predicate is false and the connection sees no rows (fail-closed)
/// rather than raising an error. The connection layer is responsible for always
/// setting one of the two; a fail-closed default keeps a forgotten GUC from
/// leaking another project's rows.
fn predicate() -> String {
    format!(
        "current_setting('{BYPASS_RLS_GUC}', true) = 'on' \
         OR project_id = current_setting('{PROJECT_ID_GUC}', true)"
    )
}

/// RLS statements for one project-scoped table. Caller emits these in the
/// schema build's final phase — after seeds, so `FORCE ROW LEVEL SECURITY` plus
/// `WITH CHECK` never reject the un-scoped bootstrap inserts.
pub(crate) fn policy_statements(table: &str) -> Vec<String> {
    let pred = predicate();
    vec![
        // New rows inherit the request's project from the GUC. There is no
        // fallback project: PostgreSQL boots with none, so an unset
        // `app.project_id` yields NULL, which fails the NOT NULL constraint —
        // fail-closed. The per-request provider always sets the GUC for tenant
        // work; system/bypass writes set `project_id` explicitly. (Test builds
        // default the serving pool to `app.bypass_rls = on` via the `td-test`
        // feature, so unscoped reads/writes span projects; a field-less insert
        // there must still be scoped so the GUC resolves.) The base column
        // carries no default, so this is the column's sole default.
        format!(
            "ALTER TABLE {table} ALTER COLUMN project_id SET DEFAULT \
             NULLIF(current_setting('{PROJECT_ID_GUC}', true), '')"
        ),
        format!("ALTER TABLE {table} ENABLE ROW LEVEL SECURITY"),
        format!("ALTER TABLE {table} FORCE ROW LEVEL SECURITY"),
        format!("CREATE POLICY project_isolation ON {table} USING ({pred}) WITH CHECK ({pred})"),
    ]
}
