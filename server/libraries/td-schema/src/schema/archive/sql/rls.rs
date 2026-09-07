//
// Copyright 2026 Tabsdata Inc.
//

//! The pre-v4 row-level security DDL, frozen: one policy whose predicate
//! carried cross-project access as an `OR` on a session variable. Archives
//! install the shape their own version had.
//!
//! The bypass variable is shared with the upgrade seam rather than spelled
//! again here: the seam has to set exactly what these policies read.

use crate::LEGACY_BYPASS_RLS_GUC as BYPASS_RLS_GUC;

const PROJECT_ID_GUC: &str = "app.project_id";

fn predicate() -> String {
    format!(
        "current_setting('{BYPASS_RLS_GUC}', true) = 'on' \
         OR project_id = current_setting('{PROJECT_ID_GUC}', true)"
    )
}

pub(crate) fn policy_statements(table: &str) -> Vec<String> {
    let pred = predicate();
    vec![
        format!(
            "ALTER TABLE {table} ALTER COLUMN project_id SET DEFAULT \
             NULLIF(current_setting('{PROJECT_ID_GUC}', true), '')"
        ),
        format!("ALTER TABLE {table} ENABLE ROW LEVEL SECURITY"),
        format!("ALTER TABLE {table} FORCE ROW LEVEL SECURITY"),
        format!("CREATE POLICY project_isolation ON {table} USING ({pred}) WITH CHECK ({pred})"),
    ]
}
