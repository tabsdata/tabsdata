//
// Copyright 2026 Tabsdata Inc.
//

//! v12 → v13: Live names are unique through partial indexes, so a rename never
//! takes a key lock; the upgrade stops, listing them, when two live functions
//! or tables of a collection share a name.

use sea_query::{ColumnDef, Expr, ExprTrait, ForeignKey, ForeignKeyAction, Iden, IntoIden, Query, Table};

use crate::schema::render::{render_query, render_schema, update_db_version};
use crate::schema::sql::id_column;
use crate::schema::sql::{Children, Parent, Physical, RollupArm, RollupDef, Runs};
use crate::schema::version::SchemaDelta;

pub struct Migration;

/// Stops the upgrade, listing them, while two live functions or tables of a
/// collection share a name.
const DUPLICATE_LIVE_NAMES: &str = "\
DO $$
DECLARE
  duplicates text;
BEGIN
  SELECT string_agg(format('%s %s in collection %s', kind, name, collection_id), '; ' ORDER BY kind, collection_id, name)
    INTO duplicates
    FROM (
      SELECT 'function' AS kind, collection_id, name FROM functions
       WHERE valid_to = 'infinity' GROUP BY collection_id, name HAVING count(*) > 1
      UNION ALL
      SELECT 'table' AS kind, collection_id, name FROM tables
       WHERE valid_to = 'infinity' GROUP BY collection_id, name HAVING count(*) > 1
    ) d;
  IF duplicates IS NOT NULL THEN
    RAISE EXCEPTION 'Live names must be unique within a collection; rename or delete one of each, then upgrade again: %', duplicates;
  END IF;
END $$";

impl SchemaDelta for Migration {
    const FROM_VERSION: usize = 12;
    const TO_VERSION: usize = 13;

    fn retired() -> Vec<String> {
        vec![
            r#"DROP INDEX IF EXISTS "collections___project_id__name___idx""#.to_string(),
            r#"DROP INDEX IF EXISTS "functions___collection_id__name___current_idx""#.to_string(),
            r#"DROP INDEX IF EXISTS "groups___project_id__name___idx""#.to_string(),
            r#"DROP INDEX IF EXISTS "tables___collection_id__name___current_idx""#.to_string(),
        ]
    }

    fn up() -> Vec<String> {
        vec![
            DUPLICATE_LIVE_NAMES.to_string(),
            "ALTER TABLE projects DROP CONSTRAINT IF EXISTS projects_name_key".to_string(),
            "ALTER TABLE roles DROP CONSTRAINT IF EXISTS roles_name_key".to_string(),
            update_db_version(Self::TO_VERSION),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::Migration;
    use crate::schema::tests::validate_delta;

    #[test]
    fn test_validate_delta_v12_to_v13() {
        validate_delta::<Migration>();
    }
}
