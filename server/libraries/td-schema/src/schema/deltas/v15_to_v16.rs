//
// Copyright 2026 Tabsdata Inc.
//

//! v15 → v16: `project_topologies`, the commit-ordered versions of a
//! project's topology, and the version each execution planned on. A
//! project's creation and every distinct instant its topology carries become
//! a version, and an execution takes the latest version at or before its
//! trigger, or the first one.

use sea_query::{ColumnDef, Iden, Table};

use crate::schema::render::{render_schema, update_db_version};
use crate::schema::sql::id_column;
use crate::schema::sql::mint::{DROP_MINT_ID, MINT_ID};
use crate::schema::version::SchemaDelta;

const TENANT_PREDICATE: &str = "project_id = current_setting('app.project_id', true)";
const ACROSS_PROJECTS_ROLE: &str = "td_across_projects";

/// Frozen at the shape it had at v16.
#[derive(Iden, Clone, Copy)]
enum ProjectTopologies {
    Table,
    Id,
    ProjectId,
    ValidFrom,
    ValidTo,
}

#[derive(Iden, Clone, Copy)]
enum Executions {
    Table,
    TopologyVersionId,
}

const BACKFILL_VERSIONS: &str = "\
INSERT INTO project_topologies (id, project_id, valid_from, valid_to)
SELECT td_mint_id(at), project_id, at,
       COALESCE(lead(at) OVER (PARTITION BY project_id ORDER BY at), 'infinity')
FROM (
    SELECT DISTINCT project_id, at FROM (
        SELECT id AS project_id, created_on AS at FROM projects
        UNION ALL SELECT project_id, valid_from FROM functions
        UNION ALL SELECT project_id, valid_to FROM functions WHERE valid_to <> 'infinity'
        UNION ALL SELECT project_id, valid_from FROM tables
        UNION ALL SELECT project_id, valid_to FROM tables WHERE valid_to <> 'infinity'
        UNION ALL SELECT project_id, valid_from FROM dependencies
        UNION ALL SELECT project_id, valid_to FROM dependencies WHERE valid_to <> 'infinity'
        UNION ALL SELECT project_id, valid_from FROM triggers
        UNION ALL SELECT project_id, valid_to FROM triggers WHERE valid_to <> 'infinity'
    ) AS instants
) AS distinct_instants";

const BACKFILL_EXECUTIONS: &str = "\
UPDATE executions e SET topology_version_id = COALESCE(
    (SELECT t.id FROM project_topologies t
      WHERE t.project_id = e.project_id AND t.valid_from <= e.triggered_on
      ORDER BY t.valid_from DESC LIMIT 1),
    (SELECT t.id FROM project_topologies t
      WHERE t.project_id = e.project_id ORDER BY t.valid_from LIMIT 1)
)";

fn policies(table: &str) -> Vec<String> {
    vec![
        format!(
            "ALTER TABLE {table} ALTER COLUMN project_id SET DEFAULT \
             NULLIF(current_setting('app.project_id', true), '')"
        ),
        format!("ALTER TABLE {table} ENABLE ROW LEVEL SECURITY"),
        format!("ALTER TABLE {table} FORCE ROW LEVEL SECURITY"),
        format!("CREATE POLICY project_isolation ON {table} USING ({TENANT_PREDICATE}) WITH CHECK ({TENANT_PREDICATE})"),
        format!("CREATE POLICY across_projects ON {table} TO {ACROSS_PROJECTS_ROLE} USING (true) WITH CHECK (true)"),
    ]
}

pub struct Migration;

impl SchemaDelta for Migration {
    const FROM_VERSION: usize = 15;
    const TO_VERSION: usize = 16;

    fn retired() -> Vec<String> {
        vec![
            r#"DROP INDEX IF EXISTS "dependencies___project_id___current_idx""#.to_string(),
            r#"DROP INDEX IF EXISTS "dependencies___table_id___idx""#.to_string(),
            r#"DROP INDEX IF EXISTS "functions___project_id__status___current_idx""#.to_string(),
            r#"DROP INDEX IF EXISTS "tables___project_id__status___current_idx""#.to_string(),
            r#"DROP INDEX IF EXISTS "triggers___project_id__status___current_idx""#.to_string(),
            r#"DROP INDEX IF EXISTS "triggers___trigger_by_table_id___idx""#.to_string(),
        ]
    }

    fn up() -> Vec<String> {
        let topologies = Table::create()
            .table(ProjectTopologies::Table)
            .col(id_column(ProjectTopologies::Id).not_null().primary_key())
            .col(id_column(ProjectTopologies::ProjectId).not_null())
            .col(ColumnDef::new(ProjectTopologies::ValidFrom).timestamp_with_time_zone().not_null())
            .col(ColumnDef::new(ProjectTopologies::ValidTo).timestamp_with_time_zone().not_null())
            .to_owned();
        let execution_version =
            Table::alter().table(Executions::Table).add_column(id_column(Executions::TopologyVersionId).null()).to_owned();
        let mut out = vec![
            render_schema(topologies),
            MINT_ID.to_string(),
            BACKFILL_VERSIONS.to_string(),
            DROP_MINT_ID.to_string(),
        ];
        out.extend(policies("project_topologies"));
        out.extend([
            render_schema(execution_version),
            BACKFILL_EXECUTIONS.to_string(),
            "ALTER TABLE executions ALTER COLUMN topology_version_id SET NOT NULL".to_string(),
            "ALTER TABLE executions ADD FOREIGN KEY (topology_version_id) REFERENCES project_topologies (id) \
             ON DELETE RESTRICT ON UPDATE RESTRICT"
                .to_string(),
            update_db_version(Self::TO_VERSION),
        ]);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::Migration;
    use crate::schema::tests::validate_delta;

    #[test]
    fn test_validate_delta_v15_to_v16() {
        validate_delta::<Migration>();
    }
}
