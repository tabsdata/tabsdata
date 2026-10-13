//
// Copyright 2026 Tabsdata Inc.
//

//! v12 → v13: Runs, attempts and workers admit only their declared status
//! transitions, and a run's new attempt opens only once its current one has
//! ended. The runs' sticky-terminals guard is subsumed by its transitions
//! trigger. Transactions roll up atomically and executions from their
//! transactions, so both stored statuses are recomputed, transactions first.
//! Live names are unique through partial indexes, so a rename never takes a key
//! lock; the upgrade stops, listing them, when two live functions or tables of
//! a collection share a name. A membership references its user, so a grant
//! racing the user's delete cannot leave one behind; memberships of users
//! already gone are deleted.

use sea_query::{ColumnDef, Expr, ExprTrait, ForeignKey, ForeignKeyAction, Iden, IntoIden, Query, Table};

use crate::schema::render::{render_query, render_schema, update_db_version};
use crate::schema::sql::id_column;
use crate::schema::sql::{Children, Instants, Parent, Physical, RollupArm, RollupDef, Runs};
use crate::schema::version::SchemaDelta;

const DERIVATION_WORK_MEM: &str = "64MB";

#[derive(Iden, Clone, Copy)]
enum Users {
    Table,
    Id,
}

#[derive(Iden, Clone, Copy)]
enum UsersRoles {
    Table,
    UserId,
}

#[derive(Iden, Clone, Copy)]
enum Executions {
    Table,
    Id,
    Status,
    StartedOn,
    EndedOn,
}

#[derive(Iden, Clone, Copy)]
enum Transactions {
    Table,
    Id,
    ExecutionId,
    Status,
    StartedOn,
    EndedOn,
}

#[derive(Iden, Clone, Copy)]
enum LogicalFunctionRuns {
    Table,
    Id,
    ExecutionId,
    TransactionId,
    Status,
}

#[derive(Iden, Clone, Copy)]
enum PhysicalFunctionRuns {
    Table,
    Id,
    ExecutionId,
    TransactionId,
    StartedOn,
    EndedOn,
}

const TERMINALS: &[&str] = &["C", "X", "Y"];

const TRANSACTION_LADDER: &[RollupArm] = &[
    RollupArm::All { all: &["S"], status: "S" },
    RollupArm::Any {
        any: &["S", "RR", "RS", "R", "E"],
        status: "R",
    },
    RollupArm::Any {
        any: &["F", "H"],
        status: "L",
    },
    RollupArm::Any { any: &["Y"], status: "Y" },
    RollupArm::Any { any: &["D"], status: "R" },
    RollupArm::All { all: &["C"], status: "C" },
];

const EXECUTION_LADDER: &[RollupArm] = &[
    RollupArm::All { all: &["S"], status: "S" },
    RollupArm::Any {
        any: &["S", "R"],
        status: "R",
    },
    RollupArm::Any { any: &["L"], status: "L" },
];

fn transaction_rollup() -> RollupDef {
    RollupDef {
        parent: Parent {
            table: Transactions::Table.into_iden(),
            id: Transactions::Id.into_iden(),
            status: Transactions::Status.into_iden(),
            started_on: Transactions::StartedOn.into_iden(),
            ended_on: Transactions::EndedOn.into_iden(),
        },
        children: Children {
            table: LogicalFunctionRuns::Table.into_iden(),
            parent: LogicalFunctionRuns::TransactionId.into_iden(),
            status: LogicalFunctionRuns::Status.into_iden(),
        },
        runs: Runs {
            table: LogicalFunctionRuns::Table.into_iden(),
            id: LogicalFunctionRuns::Id.into_iden(),
            parent: LogicalFunctionRuns::TransactionId.into_iden(),
        },
        physical: Physical {
            table: PhysicalFunctionRuns::Table.into_iden(),
            id: PhysicalFunctionRuns::Id.into_iden(),
            parent: PhysicalFunctionRuns::TransactionId.into_iden(),
            started_on: PhysicalFunctionRuns::StartedOn.into_iden(),
            ended_on: PhysicalFunctionRuns::EndedOn.into_iden(),
        },
        instants: Instants::Attempts,
        ladder: TRANSACTION_LADDER,
        fallback: "X",
        finished: TERMINALS,
    }
}

fn execution_rollup() -> RollupDef {
    RollupDef {
        parent: Parent {
            table: Executions::Table.into_iden(),
            id: Executions::Id.into_iden(),
            status: Executions::Status.into_iden(),
            started_on: Executions::StartedOn.into_iden(),
            ended_on: Executions::EndedOn.into_iden(),
        },
        children: Children {
            table: Transactions::Table.into_iden(),
            parent: Transactions::ExecutionId.into_iden(),
            status: Transactions::Status.into_iden(),
        },
        runs: Runs {
            table: LogicalFunctionRuns::Table.into_iden(),
            id: LogicalFunctionRuns::Id.into_iden(),
            parent: LogicalFunctionRuns::ExecutionId.into_iden(),
        },
        physical: Physical {
            table: PhysicalFunctionRuns::Table.into_iden(),
            id: PhysicalFunctionRuns::Id.into_iden(),
            parent: PhysicalFunctionRuns::ExecutionId.into_iden(),
            started_on: PhysicalFunctionRuns::StartedOn.into_iden(),
            ended_on: PhysicalFunctionRuns::EndedOn.into_iden(),
        },
        instants: Instants::Attempts,
        ladder: EXECUTION_LADDER,
        fallback: "F",
        finished: TERMINALS,
    }
}

/// The v13 roll-up of the parents, transactions first. Exposed so the DAO
/// gate can hold the live door to the rows the rung derives.
#[cfg(feature = "td-test")]
pub fn status_rollup_backfill() -> Vec<String> {
    vec![
        render_query(transaction_rollup().backfill()),
        render_query(execution_rollup().backfill()),
    ]
}

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
            "DROP TRIGGER IF EXISTS logical_function_runs__sticky_terminals ON logical_function_runs".to_string(),
            "DROP FUNCTION IF EXISTS logical_function_runs__sticky_terminals_fn()".to_string(),
            r#"DROP INDEX IF EXISTS "collections___project_id__name___idx""#.to_string(),
            r#"DROP INDEX IF EXISTS "functions___collection_id__name___current_idx""#.to_string(),
            r#"DROP INDEX IF EXISTS "groups___project_id__name___idx""#.to_string(),
            r#"DROP INDEX IF EXISTS "metrics_facts___table_version_id___written___idx""#.to_string(),
            r#"DROP INDEX IF EXISTS "sessions___status___idx""#.to_string(),
            r#"DROP INDEX IF EXISTS "tables___collection_id__name___current_idx""#.to_string(),
        ]
    }

    fn up() -> Vec<String> {
        let orphan_memberships = Query::delete()
            .from_table(UsersRoles::Table)
            .and_where(Expr::col(UsersRoles::UserId).not_in_subquery(Query::select().column(Users::Id).from(Users::Table).to_owned()))
            .to_owned();
        let membership_user = Table::alter()
            .table(UsersRoles::Table)
            .add_foreign_key(
                ForeignKey::create()
                    .from(UsersRoles::Table, UsersRoles::UserId)
                    .to(Users::Table, Users::Id)
                    .on_delete(ForeignKeyAction::Restrict)
                    .on_update(ForeignKeyAction::Restrict)
                    .get_foreign_key(),
            )
            .to_owned();
        vec![
            DUPLICATE_LIVE_NAMES.to_string(),
            "ALTER TABLE projects DROP CONSTRAINT IF EXISTS projects_name_key".to_string(),
            "ALTER TABLE roles DROP CONSTRAINT IF EXISTS roles_name_key".to_string(),
            render_query(orphan_memberships),
            render_schema(membership_user),
            format!("SET LOCAL work_mem = '{DERIVATION_WORK_MEM}'"),
            render_query(transaction_rollup().backfill()),
            render_query(execution_rollup().backfill()),
            "RESET work_mem".to_string(),
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
