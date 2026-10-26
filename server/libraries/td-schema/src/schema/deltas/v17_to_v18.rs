//
// Copyright 2026 Tabsdata Inc.
//

//! v17 → v18: projects, collections and groups become SCD2 timelines in
//! place. Each table's `id` becomes the entity column (`project_id`,
//! `collection_id`, `group_id`) and a fresh `id` keys its versions; a delete
//! closes the timeline instead of renaming the row to a tombstone. With the
//! entity id no longer unique, the foreign keys into these tables go.
//!
//! The backfill is the best history the old rows hold. An entity modified
//! after its creation gets two versions, `[created, modified)` and
//! `[modified, ∞)`, both carrying the current values (earlier values were
//! never kept): the first version starts at the creation, the open one at the
//! last modification, and an as-of read in between still finds the entity. A
//! deleted collection or project closes at its tombstone stamp, and a deleted
//! project's groups close with it. No topology version is added for these
//! instants: executions pin the existing ones.

use crate::schema::render::update_db_version;
use crate::schema::sql::mint::{DROP_MINT_ID, MINT_ID};
use crate::schema::version::SchemaDelta;

const DROP_FOREIGN_KEYS: &str = "\
DO $$
DECLARE fk record;
BEGIN
    FOR fk IN SELECT conrelid::regclass AS referrer, conname FROM pg_constraint
              WHERE contype = 'f'
                AND confrelid IN ('collections'::regclass, 'groups'::regclass, 'projects'::regclass)
    LOOP
        EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', fk.referrer, fk.conname);
    END LOOP;
END $$";

/// The end of each project's timeline: its delete stamp, else open.
const PROJECT_END: &str = "\
CASE WHEN p.name_when_deleted IS NOT NULL THEN p.modified_on ELSE 'infinity'::timestamptz END";

/// When a deleted collection's timeline closes: its tombstone stamp.
const COLLECTION_END: &str = "c.modified_on";

/// An entity's table re-keyed by version: `id` becomes the entity column and
/// the version columns arrive nullable, for the backfill to fill.
fn rekey(table: &str, entity: &str) -> Vec<String> {
    vec![
        format!(r#"ALTER TABLE "{table}" DROP CONSTRAINT "{table}_pkey""#),
        format!(r#"ALTER TABLE "{table}" RENAME COLUMN "id" TO "{entity}""#),
        format!(r#"ALTER TABLE "{table}" RENAME CONSTRAINT "{table}_id_not_null" TO "{table}_{entity}_not_null""#),
        format!(
            r#"ALTER TABLE "{table}" ADD COLUMN "id" text COLLATE "C" NULL, ADD COLUMN "valid_from" timestamp with time zone NULL, ADD COLUMN "valid_to" timestamp with time zone NULL, ADD COLUMN "defined_by_id" text COLLATE "C" NULL"#
        ),
    ]
}

/// The version columns made mandatory, the primary key restored on `id`, and
/// the creation, modification and soft-delete columns the versions replace
/// dropped.
fn settle(table: &str, dropped: &[&str]) -> Vec<String> {
    let mut out = vec![
        format!(
            r#"ALTER TABLE "{table}" ALTER COLUMN "id" SET NOT NULL, ALTER COLUMN "valid_from" SET NOT NULL, ALTER COLUMN "valid_to" SET NOT NULL, ALTER COLUMN "defined_by_id" SET NOT NULL"#
        ),
        format!(r#"ALTER TABLE "{table}" ADD PRIMARY KEY ("id")"#),
    ];
    out.extend(dropped.iter().map(|column| format!(r#"ALTER TABLE "{table}" DROP COLUMN "{column}""#)));
    out
}

fn collections() -> Vec<String> {
    let mut out = rekey("collections", "collection_id");
    out.push(
        "INSERT INTO collections (project_id, collection_id, name, description, base_path, created_on, \
           created_by_id, group_id, collection_type, connection_id, modified_on, modified_by_id, \
           id, valid_from, valid_to, defined_by_id) \
         SELECT project_id, collection_id, name, description, base_path, created_on, created_by_id, \
           group_id, collection_type, connection_id, modified_on, modified_by_id, \
           td_mint_id(modified_on), modified_on, 'infinity', modified_by_id \
         FROM collections WHERE name_when_deleted IS NULL AND modified_on > created_on"
            .to_string(),
    );
    out.push(format!(
        "UPDATE collections c SET id = td_mint_id(c.created_on), valid_from = c.created_on, \
           defined_by_id = c.created_by_id, name = COALESCE(c.name_when_deleted, c.name), \
           valid_to = CASE WHEN c.name_when_deleted IS NOT NULL THEN {COLLECTION_END} \
                           WHEN c.modified_on > c.created_on THEN c.modified_on \
                           ELSE 'infinity'::timestamptz END \
         WHERE c.id IS NULL"
    ));
    out.extend(settle("collections", &["created_on", "created_by_id", "modified_on", "modified_by_id", "name_when_deleted"]));
    out
}

fn groups() -> Vec<String> {
    let mut out = rekey("groups", "group_id");
    out.push(format!(
        "INSERT INTO groups (project_id, group_id, name, description, created_on, created_by_id, system, \
           kind, modified_on, modified_by_id, id, valid_from, valid_to, defined_by_id) \
         SELECT g.project_id, g.group_id, g.name, g.description, g.created_on, g.created_by_id, g.system, \
           g.kind, g.modified_on, g.modified_by_id, td_mint_id(g.modified_on), g.modified_on, \
           {PROJECT_END}, g.modified_by_id \
         FROM groups g JOIN projects p ON p.id = g.project_id \
         WHERE g.modified_on > g.created_on AND g.modified_on < {PROJECT_END}"
    ));
    out.push(format!(
        "UPDATE groups g SET id = td_mint_id(g.created_on), valid_from = g.created_on, \
           defined_by_id = g.created_by_id, \
           valid_to = CASE WHEN g.modified_on > g.created_on AND g.modified_on < {PROJECT_END} \
                           THEN g.modified_on ELSE {PROJECT_END} END \
         FROM projects p WHERE p.id = g.project_id AND g.id IS NULL"
    ));
    out.extend(settle("groups", &["created_on", "created_by_id", "modified_on", "modified_by_id"]));
    out
}

fn projects() -> Vec<String> {
    let mut out = rekey("projects", "project_id");
    out.push(
        "INSERT INTO projects (project_id, name, description, base_path, created_on, created_by_id, \
           modified_on, modified_by_id, id, valid_from, valid_to, defined_by_id) \
         SELECT project_id, name, description, base_path, created_on, created_by_id, modified_on, \
           modified_by_id, td_mint_id(modified_on), modified_on, 'infinity', modified_by_id \
         FROM projects WHERE name_when_deleted IS NULL AND modified_on > created_on"
            .to_string(),
    );
    out.push(
        "UPDATE projects SET id = td_mint_id(created_on), valid_from = created_on, \
           defined_by_id = created_by_id, name = COALESCE(name_when_deleted, name), \
           valid_to = CASE WHEN name_when_deleted IS NOT NULL THEN modified_on \
                           WHEN modified_on > created_on THEN modified_on \
                           ELSE 'infinity'::timestamptz END \
         WHERE id IS NULL"
            .to_string(),
    );
    out.extend(settle("projects", &["created_on", "created_by_id", "modified_on", "modified_by_id", "name_when_deleted"]));
    out
}

pub struct Migration;

impl SchemaDelta for Migration {
    const FROM_VERSION: usize = 17;
    const TO_VERSION: usize = 18;

    fn retired() -> Vec<String> {
        vec![
            "DROP VIEW IF EXISTS projects_active".to_string(),
            "DROP VIEW IF EXISTS projects_resolved".to_string(),
            "DROP VIEW IF EXISTS collections_active".to_string(),
            "DROP VIEW IF EXISTS collections_resolved".to_string(),
            r#"DROP INDEX IF EXISTS "projects___name___live_uidx""#.to_string(),
            r#"DROP INDEX IF EXISTS "projects___resolved_name___idx""#.to_string(),
            r#"DROP INDEX IF EXISTS "collections___project_id__resolved_name___idx""#.to_string(),
            r#"DROP INDEX IF EXISTS "collections___project_id__name___live_uidx""#.to_string(),
            r#"DROP INDEX IF EXISTS "groups___project_id__name___uidx""#.to_string(),
        ]
    }

    /// Collections and groups first: their backfill reads a project's
    /// deletion from the columns the projects step drops.
    fn up() -> Vec<String> {
        let mut out = vec![DROP_FOREIGN_KEYS.to_string(), MINT_ID.to_string()];
        out.extend(collections());
        out.extend(groups());
        out.extend(projects());
        out.push(DROP_MINT_ID.to_string());
        out.push(update_db_version(Self::TO_VERSION));
        out
    }
}

#[cfg(test)]
mod tests {
    use super::Migration;
    use crate::schema::tests::validate_delta;

    #[test]
    fn test_validate_delta_v17_to_v18() {
        validate_delta::<Migration>();
    }
}
