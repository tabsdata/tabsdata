//
// Copyright 2026 Tabsdata Inc.
//

//! v22 → v23: references between project-scoped tables carry the project,
//! against a new `(project_id, id)` key on each parent, and the tenant policy
//! reads the project through a sub-select.

use sea_query::{Alias, ForeignKey, ForeignKeyAction, Table};

use crate::schema::archive::v22::project_scoped_tables;
use crate::schema::render::{render_schema, update_db_version};
use crate::schema::version::SchemaDelta;

const TENANT_PREDICATE: &str = "project_id = (SELECT current_setting('app.project_id', true))";

/// Frozen at v23: each parent's `(project_id, key)` constraint.
const PARENT_KEYS: &[(&str, &str, &str)] = &[
    ("connections", "id", "connections___project_id__id___key"),
    ("connection_catalogs", "connection_id", "connection_catalogs___project_id__connection_id___key"),
    ("functions", "id", "functions___project_id__id___key"),
    ("function_patches", "id", "function_patches___project_id__id___key"),
    ("project_topologies", "id", "project_topologies___project_id__id___key"),
    ("executions", "id", "executions___project_id__id___key"),
    ("transactions", "id", "transactions___project_id__id___key"),
    ("logical_function_runs", "id", "logical_function_runs___project_id__id___key"),
    ("logical_table_data_versions", "id", "logical_table_data_versions___project_id__id___key"),
    ("physical_function_runs", "id", "pfr___project_id__id___key"),
    ("physical_table_data_versions", "id", "ptdv___project_id__id___key"),
];

/// Frozen at v23: a project-scoped reference, and the single-column key it
/// replaces (named by PostgreSQL at v22).
struct Fk {
    table: &'static str,
    retired: Option<&'static str>,
    name: &'static str,
    column: &'static str,
    parent: &'static str,
    key: &'static str,
    on_delete: ForeignKeyAction,
}

const FKS: &[Fk] = &[
    Fk {
        table: "collections",
        retired: Some("collections_connection_id_fkey"),
        name: "collections___connection_id___fkey",
        column: "connection_id",
        parent: "connections",
        key: "id",
        on_delete: ForeignKeyAction::Restrict,
    },
    Fk {
        table: "connection_catalogs",
        retired: Some("connection_catalogs_connection_id_fkey"),
        name: "connection_catalogs___connection_id___fkey",
        column: "connection_id",
        parent: "connections",
        key: "id",
        on_delete: ForeignKeyAction::Cascade,
    },
    Fk {
        table: "connection_entities",
        retired: Some("connection_entities_connection_id_fkey"),
        name: "connection_entities___connection_id___fkey",
        column: "connection_id",
        parent: "connection_catalogs",
        key: "connection_id",
        on_delete: ForeignKeyAction::Cascade,
    },
    Fk {
        table: "connection_probes",
        retired: Some("connection_probes_connection_id_fkey"),
        name: "connection_probes___connection_id___fkey",
        column: "connection_id",
        parent: "connections",
        key: "id",
        on_delete: ForeignKeyAction::Cascade,
    },
    Fk {
        table: "executions",
        retired: Some("executions_function_version_id_fkey"),
        name: "executions___function_version_id___fkey",
        column: "function_version_id",
        parent: "functions",
        key: "id",
        on_delete: ForeignKeyAction::Restrict,
    },
    Fk {
        table: "executions",
        retired: Some("executions_topology_version_id_fkey"),
        name: "executions___topology_version_id___fkey",
        column: "topology_version_id",
        parent: "project_topologies",
        key: "id",
        on_delete: ForeignKeyAction::Restrict,
    },
    Fk {
        table: "function_patches",
        retired: Some("function_patches_parent_function_version_id_fkey"),
        name: "function_patches___parent_function_version_id___fkey",
        column: "parent_function_version_id",
        parent: "functions",
        key: "id",
        on_delete: ForeignKeyAction::Restrict,
    },
    Fk {
        table: "logical_function_run_inputs",
        retired: Some("logical_function_run_inputs_execution_id_fkey"),
        name: "lfri___execution_id___fkey",
        column: "execution_id",
        parent: "executions",
        key: "id",
        on_delete: ForeignKeyAction::Restrict,
    },
    Fk {
        table: "logical_function_run_inputs",
        retired: Some("logical_function_run_inputs_logical_function_run_id_fkey"),
        name: "lfri___logical_function_run_id___fkey",
        column: "logical_function_run_id",
        parent: "logical_function_runs",
        key: "id",
        on_delete: ForeignKeyAction::Restrict,
    },
    Fk {
        table: "logical_function_run_inputs",
        retired: Some("logical_function_run_inputs_requirement_logical_table_data_fkey"),
        name: "lfri___requirement_logical_table_data_version_id___fkey",
        column: "requirement_logical_table_data_version_id",
        parent: "logical_table_data_versions",
        key: "id",
        on_delete: ForeignKeyAction::Restrict,
    },
    Fk {
        table: "logical_function_run_inputs",
        retired: Some("logical_function_run_inputs_transaction_id_fkey"),
        name: "lfri___transaction_id___fkey",
        column: "transaction_id",
        parent: "transactions",
        key: "id",
        on_delete: ForeignKeyAction::Restrict,
    },
    Fk {
        table: "logical_function_runs",
        retired: Some("logical_function_runs_execution_id_fkey"),
        name: "logical_function_runs___execution_id___fkey",
        column: "execution_id",
        parent: "executions",
        key: "id",
        on_delete: ForeignKeyAction::Restrict,
    },
    Fk {
        table: "logical_function_runs",
        retired: Some("logical_function_runs_transaction_id_fkey"),
        name: "logical_function_runs___transaction_id___fkey",
        column: "transaction_id",
        parent: "transactions",
        key: "id",
        on_delete: ForeignKeyAction::Restrict,
    },
    Fk {
        table: "logical_table_data_versions",
        retired: Some("logical_table_data_versions_execution_id_fkey"),
        name: "ltdv___execution_id___fkey",
        column: "execution_id",
        parent: "executions",
        key: "id",
        on_delete: ForeignKeyAction::Restrict,
    },
    Fk {
        table: "logical_table_data_versions",
        retired: Some("logical_table_data_versions_producer_logical_function_run__fkey"),
        name: "ltdv___producer_logical_function_run_id___fkey",
        column: "producer_logical_function_run_id",
        parent: "logical_function_runs",
        key: "id",
        on_delete: ForeignKeyAction::Restrict,
    },
    Fk {
        table: "logical_table_data_versions",
        retired: Some("logical_table_data_versions_transaction_id_fkey"),
        name: "ltdv___transaction_id___fkey",
        column: "transaction_id",
        parent: "transactions",
        key: "id",
        on_delete: ForeignKeyAction::Restrict,
    },
    Fk {
        table: "physical_function_runs",
        retired: Some("physical_function_runs_execution_id_fkey"),
        name: "pfr___execution_id___fkey",
        column: "execution_id",
        parent: "executions",
        key: "id",
        on_delete: ForeignKeyAction::Restrict,
    },
    Fk {
        table: "physical_function_runs",
        retired: Some("physical_function_runs_function_version_id_fkey"),
        name: "pfr___function_version_id___fkey",
        column: "function_version_id",
        parent: "functions",
        key: "id",
        on_delete: ForeignKeyAction::Restrict,
    },
    Fk {
        table: "physical_function_runs",
        retired: Some("physical_function_runs_function_version_patch_id_fkey"),
        name: "pfr___function_version_patch_id___fkey",
        column: "function_version_patch_id",
        parent: "function_patches",
        key: "id",
        on_delete: ForeignKeyAction::Restrict,
    },
    Fk {
        table: "physical_function_runs",
        retired: Some("physical_function_runs_logical_function_run_id_fkey"),
        name: "pfr___logical_function_run_id___fkey",
        column: "logical_function_run_id",
        parent: "logical_function_runs",
        key: "id",
        on_delete: ForeignKeyAction::Restrict,
    },
    Fk {
        table: "physical_table_data_versions",
        retired: Some("physical_table_data_versions_execution_id_fkey"),
        name: "ptdv___execution_id___fkey",
        column: "execution_id",
        parent: "executions",
        key: "id",
        on_delete: ForeignKeyAction::Restrict,
    },
    Fk {
        table: "physical_table_data_versions",
        retired: Some("physical_table_data_versions_function_version_id_fkey"),
        name: "ptdv___function_version_id___fkey",
        column: "function_version_id",
        parent: "functions",
        key: "id",
        on_delete: ForeignKeyAction::Restrict,
    },
    Fk {
        table: "physical_table_data_versions",
        retired: Some("physical_table_data_versions_function_version_patch_id_fkey"),
        name: "ptdv___function_version_patch_id___fkey",
        column: "function_version_patch_id",
        parent: "function_patches",
        key: "id",
        on_delete: ForeignKeyAction::Restrict,
    },
    Fk {
        table: "physical_table_data_versions",
        retired: Some("physical_table_data_versions_logical_table_data_version_id_fkey"),
        name: "ptdv___logical_table_data_version_id___fkey",
        column: "logical_table_data_version_id",
        parent: "logical_table_data_versions",
        key: "id",
        on_delete: ForeignKeyAction::Restrict,
    },
    Fk {
        table: "physical_table_data_versions",
        retired: Some("physical_table_data_versions_physical_function_run_id_fkey"),
        name: "ptdv___physical_function_run_id___fkey",
        column: "physical_function_run_id",
        parent: "physical_function_runs",
        key: "id",
        on_delete: ForeignKeyAction::Restrict,
    },
    Fk {
        table: "table_partitions",
        retired: Some("table_partitions_function_version_id_fkey"),
        name: "table_partitions___function_version_id___fkey",
        column: "function_version_id",
        parent: "functions",
        key: "id",
        on_delete: ForeignKeyAction::Restrict,
    },
    Fk {
        table: "table_partitions",
        retired: Some("table_partitions_table_data_version_id_fkey"),
        name: "table_partitions___table_data_version_id___fkey",
        column: "table_data_version_id",
        parent: "physical_table_data_versions",
        key: "id",
        on_delete: ForeignKeyAction::Restrict,
    },
    Fk {
        table: "tables",
        retired: Some("tables_function_version_id_fkey"),
        name: "tables___function_version_id___fkey",
        column: "function_version_id",
        parent: "functions",
        key: "id",
        on_delete: ForeignKeyAction::Restrict,
    },
    Fk {
        table: "transactions",
        retired: Some("transactions_execution_id_fkey"),
        name: "transactions___execution_id___fkey",
        column: "execution_id",
        parent: "executions",
        key: "id",
        on_delete: ForeignKeyAction::Restrict,
    },
    Fk {
        table: "workers",
        retired: Some("workers_execution_id_fkey"),
        name: "workers___execution_id___fkey",
        column: "execution_id",
        parent: "executions",
        key: "id",
        on_delete: ForeignKeyAction::Restrict,
    },
    Fk {
        table: "workers",
        retired: Some("workers_function_version_id_fkey"),
        name: "workers___function_version_id___fkey",
        column: "function_version_id",
        parent: "functions",
        key: "id",
        on_delete: ForeignKeyAction::Restrict,
    },
    Fk {
        table: "workers",
        retired: Some("workers_physical_function_run_id_fkey"),
        name: "workers___physical_function_run_id___fkey",
        column: "physical_function_run_id",
        parent: "physical_function_runs",
        key: "id",
        on_delete: ForeignKeyAction::Restrict,
    },
    Fk {
        table: "workers",
        retired: Some("workers_transaction_id_fkey"),
        name: "workers___transaction_id___fkey",
        column: "transaction_id",
        parent: "transactions",
        key: "id",
        on_delete: ForeignKeyAction::Restrict,
    },
    Fk {
        table: "physical_function_runs",
        retired: None,
        name: "pfr___transaction_id___fkey",
        column: "transaction_id",
        parent: "transactions",
        key: "id",
        on_delete: ForeignKeyAction::Restrict,
    },
];

pub struct Migration;

impl SchemaDelta for Migration {
    const FROM_VERSION: usize = 22;
    const TO_VERSION: usize = 23;

    fn retired() -> Vec<String> {
        [
            "executions___project_id__id___idx",
            "functions___project_id__id___idx",
            "logical_function_runs___project_id__id___idx",
            "logical_table_data_versions___project_id__id___idx",
            "pfr___project_id__id___idx",
            "ptdv___project_id__id___idx",
            "transactions___project_id__id___idx",
        ]
        .iter()
        .map(|name| format!(r#"DROP INDEX IF EXISTS "{name}""#))
        .collect()
    }

    fn up() -> Vec<String> {
        let mut out: Vec<String> = PARENT_KEYS
            .iter()
            .map(|(table, key, name)| format!(r#"ALTER TABLE "{table}" ADD CONSTRAINT "{name}" UNIQUE ("project_id", "{key}")"#))
            .collect();
        for fk in FKS {
            if let Some(retired) = fk.retired {
                out.push(render_schema(Table::alter().table(Alias::new(fk.table)).drop_constraint(Alias::new(retired)).to_owned()));
            }
            out.push(render_schema(
                Table::alter()
                    .table(Alias::new(fk.table))
                    .add_foreign_key(
                        ForeignKey::create()
                            .name(fk.name)
                            .from(Alias::new(fk.table), (Alias::new("project_id"), Alias::new(fk.column)))
                            .to(Alias::new(fk.parent), (Alias::new("project_id"), Alias::new(fk.key)))
                            .on_delete(fk.on_delete)
                            .on_update(ForeignKeyAction::Restrict)
                            .get_foreign_key(),
                    )
                    .to_owned(),
            ));
        }
        for table in project_scoped_tables() {
            out.push(format!("ALTER POLICY project_isolation ON {table} USING ({TENANT_PREDICATE}) WITH CHECK ({TENANT_PREDICATE})"));
        }
        out.push(update_db_version(Self::TO_VERSION));
        out
    }
}

#[cfg(test)]
mod tests {
    use super::Migration;
    use crate::schema::tests::validate_delta;

    #[test]
    fn test_validate_delta_v22_to_v23() {
        validate_delta::<Migration>();
    }
}
