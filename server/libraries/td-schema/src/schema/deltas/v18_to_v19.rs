//
// Copyright 2026 Tabsdata Inc.
//

//! v18 → v19: `tags`, the versioned key/value annotations of projects,
//! groups, collections, functions and tables. A function's or table's parent
//! moves from its own column to a `_parent` topology tag over the same
//! intervals.

use sea_query::{ColumnDef, Iden, Table};

use crate::schema::render::{render_schema, update_db_version};
use crate::schema::sql::id_column;
use crate::schema::sql::mint::{DROP_MINT_ID, MINT_ID};
use crate::schema::version::SchemaDelta;

const TENANT_PREDICATE: &str = "project_id = current_setting('app.project_id', true)";
const ACROSS_PROJECTS_ROLE: &str = "td_across_projects";

/// Frozen at the shape it had at v19.
#[derive(Iden, Clone, Copy)]
enum Tags {
    Table,
    Id,
    TagId,
    ProjectId,
    EntityType,
    EntityId,
    Kind,
    Key,
    Value,
    ValidFrom,
    DefinedById,
    ValidTo,
}

/// One tag per run of contiguous row versions naming the same parent, as the
/// live writer opens a new version only when the parent changes and a new tag
/// once the parent was dropped.
fn backfill_parents(table: &str, entity_type: &str, entity_id: &str, parent: &str) -> String {
    format!(
        "INSERT INTO tags (id, tag_id, project_id, entity_type, entity_id, kind, key, value, \
                           valid_from, defined_by_id, valid_to) \
         SELECT td_mint_id(min(i.valid_from)), td_mint_id(min(i.valid_from)), i.project_id, '{entity_type}', \
                i.{entity_id}, 'T', '_parent', i.{parent}, min(i.valid_from), \
                (array_agg(i.defined_by_id ORDER BY i.valid_from, i.id))[1], max(i.valid_to) \
         FROM (SELECT r.*, sum(r.starts) OVER (PARTITION BY r.{entity_id} \
                                               ORDER BY r.valid_from, r.id) AS run \
               FROM (SELECT v.id, v.project_id, v.{entity_id}, v.{parent}, v.valid_from, \
                            v.valid_to, v.defined_by_id, \
                            CASE WHEN lag(v.{parent}) OVER w = v.{parent} \
                                  AND lag(v.valid_to) OVER w = v.valid_from \
                                 THEN 0 ELSE 1 END AS starts \
                     FROM {table} v WHERE v.{parent} IS NOT NULL \
                     WINDOW w AS (PARTITION BY v.{entity_id} ORDER BY v.valid_from, v.id)) r) i \
         GROUP BY i.{entity_id}, i.run, i.project_id, i.{parent}"
    )
}

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
    const FROM_VERSION: usize = 18;
    const TO_VERSION: usize = 19;

    fn retired() -> Vec<String> {
        vec!["DROP INDEX IF EXISTS \"functions___parent_function_id___current_idx\"".to_string()]
    }

    fn up() -> Vec<String> {
        let tags = Table::create()
            .table(Tags::Table)
            .col(id_column(Tags::Id).not_null().primary_key())
            .col(id_column(Tags::TagId).not_null())
            .col(id_column(Tags::ProjectId).not_null())
            .col(ColumnDef::new(Tags::EntityType).text().not_null())
            .col(id_column(Tags::EntityId).not_null())
            .col(ColumnDef::new(Tags::Kind).text().not_null())
            .col(ColumnDef::new(Tags::Key).text().not_null())
            .col(ColumnDef::new(Tags::Value).text().not_null())
            .col(ColumnDef::new(Tags::ValidFrom).timestamp_with_time_zone().not_null())
            .col(id_column(Tags::DefinedById).not_null())
            .col(ColumnDef::new(Tags::ValidTo).timestamp_with_time_zone().not_null())
            .to_owned();
        let mut out = vec![render_schema(tags)];
        out.extend(policies("tags"));
        out.extend([
            MINT_ID.to_string(),
            backfill_parents("functions", "F", "function_id", "parent_function_id"),
            backfill_parents("tables", "T", "table_id", "parent_table_id"),
            DROP_MINT_ID.to_string(),
            "ALTER TABLE functions DROP COLUMN parent_function_id".to_string(),
            "ALTER TABLE tables DROP COLUMN parent_table_id".to_string(),
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
    fn test_validate_delta_v18_to_v19() {
        validate_delta::<Migration>();
    }
}
