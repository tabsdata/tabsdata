//
// Copyright 2026 Tabsdata Inc.
//

//! v18 → v19: `tags`, the versioned key/value annotations of projects,
//! groups, collections, functions and tables.

use sea_query::{ColumnDef, Iden, Table};

use crate::schema::render::{render_schema, update_db_version};
use crate::schema::sql::id_column;
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
        vec![]
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
        out.push(update_db_version(Self::TO_VERSION));
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
