//
// Copyright 2026 Tabsdata Inc.
//

//! Every index of the current schema names what reads it. The registry is
//! checked both ways against td-schema's inventory, so an index whose query
//! went away, or a query whose index was never declared, fails here.

use std::collections::BTreeSet;

use ta_objects::ListDto;
use td_objects::dxo::crudl::ListParams;
use td_objects::dxo::execution::Execution;
use td_objects::dxo::logical::{FunctionRun, TableDataVersion};
use td_objects::dxo::transaction::Transaction;
use td_objects::dxo::worker::Worker;
use td_objects::list::params::ListQueryParams;
use td_objects::types::basic::{CollectionId, ExecutionId, ProjectId, TransactionId};
use td_schema::schema::current::indexes;

use crate::audit::scan_surface;

enum ServedBy {
    /// `Type::method` names on this crate's query surface.
    Queries(&'static [&'static str]),
    /// A `list` on the DTO under these filters and ordering; parsing the
    /// params proves the combination is still one a client can send.
    List(fn() -> Result<(), String>),
    Constraint,
    /// A consumer outside this crate's query surface: an FK probe, a
    /// keyset tie-break under RLS, a service lookup by a keyed DAO read.
    Path(&'static str),
}

struct IndexCase {
    index: &'static str,
    served_by: ServedBy,
}

/// A bare column filters by a fresh id of its type; `status:eq:X` and the
/// like pass as written.
fn list<T: ListDto>(filter: &[&str], order_by: Option<&str>) -> Result<(), String> {
    let id = |column: &str| match column {
        "project_id" => ProjectId::default().to_string(),
        "collection_id" => CollectionId::default().to_string(),
        "execution_id" => ExecutionId::new().to_string(),
        "transaction_id" => TransactionId::new().to_string(),
        other => panic!("no id type registered for filter column `{other}`"),
    };
    let params = ListParams {
        filter: filter
            .iter()
            .map(|f| {
                if f.contains(':') {
                    f.to_string()
                } else {
                    format!("{f}:eq:{}", id(f))
                }
            })
            .collect(),
        order_by: order_by.map(str::to_string),
        ..ListParams::default()
    };
    ListQueryParams::<T>::try_from(&params)
        .map(drop)
        .map_err(|e| format!("{e:?}"))
}

use ServedBy::{Constraint, List, Path, Queries};

const KEYSET: &str = "keyset pagination: ORDER BY id under the tenant predicate";
const GROUP_FK: &str = "group-delete RESTRICT FK probe";
const TIMELINE: &[&str] = &["Timeline::list_until", "Scd2::fetch_one"];

const INDEX_CASES: &[IndexCase] = &[
    IndexCase {
        index: "collections___group_id__project_id___idx",
        served_by: Path(GROUP_FK),
    },
    IndexCase {
        index: "collections___project_id__id___idx",
        served_by: Path(KEYSET),
    },
    IndexCase {
        index: "collections___project_id__name___live_uidx",
        served_by: Constraint,
    },
    IndexCase {
        index: "collections___project_id__resolved_name___idx",
        served_by: Path("collections_resolved lookup by name"),
    },
    IndexCase {
        index: "dependencies___dependency_id___current_uidx",
        served_by: Constraint,
    },
    IndexCase {
        index: "dependencies___dependency_id__valid_from__id___idx",
        served_by: Queries(TIMELINE),
    },
    IndexCase {
        index: "dependencies___function_id___idx",
        served_by: Queries(&["Dependencies::active_for_function_at"]),
    },
    IndexCase {
        index: "dependencies___function_id___current_idx",
        served_by: Path("open dependencies of a function: register, graph build"),
    },
    IndexCase {
        index: "dependencies___project_id___current_idx",
        served_by: Path("graph recursion edge set: every open row of the tenant"),
    },
    IndexCase {
        index: "dependencies___project_id__id___idx",
        served_by: Path(KEYSET),
    },
    IndexCase {
        index: "dependencies___table_id___idx",
        served_by: Path("register and collection delete resolve dependencies on a set of tables"),
    },
    IndexCase {
        index: "dependencies___table_id___current_idx",
        served_by: Path(
            "collection delete and the table-rooted graph build read open dependencies of a set of tables",
        ),
    },
    IndexCase {
        index: "executions___collection_id__triggered_on__id___idx",
        served_by: List(|| {
            list::<Execution>(&["project_id", "collection_id"], Some("triggered_on-"))
        }),
    },
    IndexCase {
        index: "executions___function_id__external_uid___idx",
        served_by: Constraint,
    },
    IndexCase {
        index: "executions___fv_id__trigger_type__triggered_on___idx",
        served_by: Queries(&["Executions::list_by_function_version"]),
    },
    IndexCase {
        index: "executions___project_id__id___idx",
        served_by: Path(KEYSET),
    },
    IndexCase {
        index: "executions___project_id__status__triggered_on__id___idx",
        served_by: List(|| {
            list::<Execution>(&["project_id", "status:eq:S"], Some("triggered_on-"))
        }),
    },
    IndexCase {
        index: "executions___project_id__triggered_on__id___idx",
        served_by: List(|| list::<Execution>(&["project_id"], Some("triggered_on-"))),
    },
    IndexCase {
        index: "executions___triggered_on__id___idx",
        served_by: List(|| list::<Execution>(&[], Some("triggered_on-"))),
    },
    IndexCase {
        index: "function_patches___function_id__valid_from___idx",
        served_by: Path("patches of a function in version order"),
    },
    IndexCase {
        index: "function_patches___parent_function_version_id___current_uidx",
        served_by: Constraint,
    },
    IndexCase {
        index: "function_patches___parent_fv_id__valid_from__id___idx",
        served_by: Queries(TIMELINE),
    },
    IndexCase {
        index: "functions___collection_id__name___idx",
        served_by: Path("function by name at an instant"),
    },
    IndexCase {
        index: "functions___collection_id__name___current_uidx",
        served_by: Constraint,
    },
    IndexCase {
        index: "functions___function_id___current_uidx",
        served_by: Constraint,
    },
    IndexCase {
        index: "functions___function_id__valid_from__id___idx",
        served_by: Queries(TIMELINE),
    },
    IndexCase {
        index: "functions___parent_function_id___current_idx",
        served_by: Path("child functions of a parent"),
    },
    IndexCase {
        index: "functions___project_id__id___idx",
        served_by: Path(KEYSET),
    },
    IndexCase {
        index: "functions___project_id__status___current_idx",
        served_by: Path("graph recursion reference set: open rows of the tenant by status"),
    },
    IndexCase {
        index: "groups___name___idx",
        served_by: Path("group by name"),
    },
    IndexCase {
        index: "groups___project_id__name___uidx",
        served_by: Constraint,
    },
    IndexCase {
        index: "logical_function_run_inputs___execution_id___idx",
        served_by: Path("inputs of an execution: plan build, cancel"),
    },
    IndexCase {
        index: "logical_function_run_inputs___logical_function_run_id___idx",
        served_by: Queries(&["LogicalRuns::for_dispatch", "LogicalRuns::to_execute"]),
    },
    IndexCase {
        index: "lfri___requirement_logical_table_data_version_id___idx",
        served_by: Queries(&[
            "LogicalRuns::blocked_ids",
            "LogicalRuns::downstream_one_hop",
        ]),
    },
    IndexCase {
        index: "logical_function_runs___collection_id__triggered_on__id___idx",
        served_by: List(|| {
            list::<FunctionRun>(&["project_id", "collection_id"], Some("triggered_on-"))
        }),
    },
    IndexCase {
        index: "logical_function_runs___execution_id__function_id___idx",
        served_by: Constraint,
    },
    IndexCase {
        index: "logical_function_runs___execution_id__status___idx",
        served_by: Path("execution status roll-up and per-status counts"),
    },
    IndexCase {
        index: "logical_function_runs___execution_id__triggered_on__id___idx",
        served_by: List(|| {
            list::<FunctionRun>(&["project_id", "execution_id"], Some("triggered_on-"))
        }),
    },
    IndexCase {
        index: "logical_function_runs___function_id__triggered_on__id___idx",
        served_by: Queries(&["LogicalRuns::previous_for_function_at"]),
    },
    IndexCase {
        index: "logical_function_runs___function_version_id___idx",
        served_by: Path("runs of a function version: update and delete guards"),
    },
    IndexCase {
        index: "logical_function_runs___group_id__project_id___idx",
        served_by: Path(GROUP_FK),
    },
    IndexCase {
        index: "logical_function_runs___project_id__id___idx",
        served_by: Path(KEYSET),
    },
    IndexCase {
        index: "logical_function_runs___project_id__triggered_on__id___idx",
        served_by: List(|| list::<FunctionRun>(&["project_id"], Some("triggered_on-"))),
    },
    IndexCase {
        index: "logical_function_runs___status__project_id__triggered_on___idx",
        served_by: Queries(&[
            "LogicalRuns::to_execute",
            "LogicalRuns::awaiting_commit",
            "LogicalRuns::done_beside_canceled",
        ]),
    },
    IndexCase {
        index: "logical_function_runs___transaction_id__status___idx",
        served_by: Queries(&[
            "LogicalRuns::to_commit",
            "LogicalRuns::done_beside_canceled",
            "LogicalRuns::awaiting_commit",
        ]),
    },
    IndexCase {
        index: "logical_function_runs___transaction_id__triggered_on__id___idx",
        served_by: List(|| {
            list::<FunctionRun>(&["project_id", "transaction_id"], Some("triggered_on-"))
        }),
    },
    IndexCase {
        index: "logical_function_runs___triggered_on__id___idx",
        served_by: List(|| list::<FunctionRun>(&[], Some("triggered_on-"))),
    },
    IndexCase {
        index: "ltdv___collection_id__triggered_on__id___idx",
        served_by: List(|| list::<TableDataVersion>(&["collection_id"], Some("created_at-"))),
    },
    IndexCase {
        index: "logical_table_data_versions___execution_id__table_id___idx",
        served_by: Path(
            "one row per (execution, table): plan resolver; executions RESTRICT FK probe",
        ),
    },
    IndexCase {
        index: "ltdv___execution_id__triggered_on__id___idx",
        served_by: List(|| list::<TableDataVersion>(&["execution_id"], Some("created_at-"))),
    },
    IndexCase {
        index: "logical_table_data_versions___group_id__project_id___idx",
        served_by: Path(GROUP_FK),
    },
    IndexCase {
        index: "ltdv___producer_logical_function_run_id___idx",
        served_by: Path("versions produced by a run: commit, cancel"),
    },
    IndexCase {
        index: "logical_table_data_versions___project_id__id___idx",
        served_by: Path(KEYSET),
    },
    IndexCase {
        index: "logical_table_data_versions___table_id__triggered_on__id___idx",
        served_by: Queries(&[
            "Timeline::list_until",
            "TableDataVersions::committed_data_at",
            "TableDataVersions::committed_data_before",
        ]),
    },
    IndexCase {
        index: "metrics_facts___attempt_id___idx",
        served_by: Queries(&["attempts_ended"]),
    },
    IndexCase {
        index: "metrics_facts___function_run_id___idx",
        served_by: Queries(&["runs_status_changed", "runs_reattempted"]),
    },
    IndexCase {
        index: "metrics_facts___project_id__ts___idx",
        served_by: Queries(&["Metrics::per_table"]),
    },
    IndexCase {
        index: "metrics_facts___table_version_id__side___idx",
        served_by: Queries(&["versions_consumed", "attempts_ended"]),
    },
    IndexCase {
        index: "metrics_facts___ts___idx",
        served_by: Queries(&["Metrics::per_table"]),
    },
    IndexCase {
        index: "pfr___function_version_id___idx",
        served_by: Path("attempts of a function version"),
    },
    IndexCase {
        index: "pfr___function_version_patch_id___idx",
        served_by: Path("attempts of a patch"),
    },
    IndexCase {
        index: "pfr___logical_function_run_id___current_uidx",
        served_by: Constraint,
    },
    IndexCase {
        index: "pfr___logical_function_run_id__valid_from__id___idx",
        served_by: Queries(TIMELINE),
    },
    IndexCase {
        index: "pfr___project_id__execution_id__started_on__ended_on___idx",
        served_by: Path("execution roll-up: MIN(started_on) / MAX(ended_on) of the attempts"),
    },
    IndexCase {
        index: "pfr___project_id__id___idx",
        served_by: Path(KEYSET),
    },
    IndexCase {
        index: "pfr___project_id__transaction_id__started_on__ended_on___idx",
        served_by: Path("transaction roll-up: MIN(started_on) / MAX(ended_on) of the attempts"),
    },
    IndexCase {
        index: "pfr___status__project_id__dispatched_on___idx",
        served_by: Queries(&["PhysicalRuns::dispatched_before"]),
    },
    IndexCase {
        index: "pfr___status__project_id__valid_from___idx",
        served_by: Queries(&["PhysicalRuns::to_dispatch"]),
    },
    IndexCase {
        index: "ptdv___logical_table_data_version_id___current_uidx",
        served_by: Constraint,
    },
    IndexCase {
        index: "ptdv___logical_table_data_version_id__valid_from__id___idx",
        served_by: Queries(TIMELINE),
    },
    IndexCase {
        index: "ptdv___physical_function_run_id__table_id___uidx",
        served_by: Constraint,
    },
    IndexCase {
        index: "ptdv___project_id__id___idx",
        served_by: Path(KEYSET),
    },
    IndexCase {
        index: "permissions___role_id__permission_type__entity___idx",
        served_by: Constraint,
    },
    IndexCase {
        index: "projects___name___live_uidx",
        served_by: Constraint,
    },
    IndexCase {
        index: "projects___resolved_name___idx",
        served_by: Path("projects_resolved lookup by name"),
    },
    IndexCase {
        index: "roles___name___uidx",
        served_by: Constraint,
    },
    IndexCase {
        index: "roles___project_id___idx",
        served_by: Path("roles of a project"),
    },
    IndexCase {
        index: "roles___user_id__role_id___idx",
        served_by: Constraint,
    },
    IndexCase {
        index: "service_tokens___user_id__slot___idx",
        served_by: Constraint,
    },
    IndexCase {
        index: "sessions___expires_on___idx",
        served_by: Queries(&["Sessions::purge"]),
    },
    IndexCase {
        index: "sessions___user_id___idx",
        served_by: Path("sessions of a user: logout, password change"),
    },
    IndexCase {
        index: "tables___collection_id__name___idx",
        served_by: Path("table by name at an instant"),
    },
    IndexCase {
        index: "tables___collection_id__name___current_uidx",
        served_by: Constraint,
    },
    IndexCase {
        index: "tables___function_id___current_idx",
        served_by: Path("open tables of a function"),
    },
    IndexCase {
        index: "tables___function_id__status___idx",
        served_by: Path("enable re-registers every table of a function version"),
    },
    IndexCase {
        index: "tables___function_version_id___idx",
        served_by: Path("tables of a function version"),
    },
    IndexCase {
        index: "tables___project_id__id___idx",
        served_by: Path(KEYSET),
    },
    IndexCase {
        index: "tables___project_id__status___current_idx",
        served_by: Path("graph recursion reference set: open rows of the tenant by status"),
    },
    IndexCase {
        index: "tables___table_id___current_uidx",
        served_by: Constraint,
    },
    IndexCase {
        index: "tables___table_id__valid_from__id___idx",
        served_by: Queries(TIMELINE),
    },
    IndexCase {
        index: "transactions___execution_id__triggered_on__id___idx",
        served_by: List(|| {
            list::<Transaction>(&["project_id", "execution_id"], Some("triggered_on-"))
        }),
    },
    IndexCase {
        index: "transactions___group_id__project_id___idx",
        served_by: Path(GROUP_FK),
    },
    IndexCase {
        index: "transactions___project_id__id___idx",
        served_by: Path(KEYSET),
    },
    IndexCase {
        index: "transactions___project_id__started_on__id___idx",
        served_by: List(|| list::<Transaction>(&["project_id"], Some("started_on-"))),
    },
    IndexCase {
        index: "transactions___project_id__status__triggered_on__id___idx",
        served_by: List(|| {
            list::<Transaction>(&["project_id", "status:eq:S"], Some("triggered_on-"))
        }),
    },
    IndexCase {
        index: "transactions___project_id__triggered_on__id___idx",
        served_by: List(|| list::<Transaction>(&["project_id"], Some("triggered_on-"))),
    },
    IndexCase {
        index: "transactions___triggered_on__id___idx",
        served_by: List(|| list::<Transaction>(&[], Some("triggered_on-"))),
    },
    IndexCase {
        index: "triggers___function_id___idx",
        served_by: Path("triggers of a function at an instant"),
    },
    IndexCase {
        index: "triggers___function_id___current_idx",
        served_by: Path("open triggers of a function"),
    },
    IndexCase {
        index: "triggers___project_id__id___idx",
        served_by: Path(KEYSET),
    },
    IndexCase {
        index: "triggers___project_id__status___current_idx",
        served_by: Path("graph recursion reference set: open rows of the tenant by status"),
    },
    IndexCase {
        index: "triggers___trigger_by_table_id___idx",
        served_by: Path("triggers fired by a table at an instant"),
    },
    IndexCase {
        index: "triggers___trigger_by_table_id___current_idx",
        served_by: Path("open triggers fired by a table"),
    },
    IndexCase {
        index: "triggers___trigger_id___current_uidx",
        served_by: Constraint,
    },
    IndexCase {
        index: "triggers___trigger_id__valid_from__id___idx",
        served_by: Queries(TIMELINE),
    },
    IndexCase {
        index: "users_roles___role_id__id___idx",
        served_by: Path("users of a role, keyset"),
    },
    IndexCase {
        index: "workers___collection_id___idx",
        served_by: Path("workers of a collection: collection delete"),
    },
    IndexCase {
        index: "workers___function_version_id__worker_type___idx",
        served_by: Path("trigger workers of a function version"),
    },
    IndexCase {
        index: "workers___physical_function_run_id___idx",
        served_by: Queries(&["PhysicalRuns::to_dispatch"]),
    },
    IndexCase {
        index: "workers___project_id__id___idx",
        served_by: Path(KEYSET),
    },
    IndexCase {
        index: "workers___project_id__started_on__id___idx",
        served_by: List(|| list::<Worker>(&["project_id"], Some("started_on-"))),
    },
    IndexCase {
        index: "workers___project_id__status__started_on__id___idx",
        served_by: List(|| list::<Worker>(&["project_id", "status:eq:R"], Some("started_on-"))),
    },
    IndexCase {
        index: "workers___started_on__id___idx",
        served_by: List(|| list::<Worker>(&[], Some("started_on-"))),
    },
    IndexCase {
        index: "workers___enabled__worker_type__status__project_id__id___idx",
        served_by: Path("scheduler: enabled trigger workers by type and status, across projects"),
    },
];

#[test]
fn every_index_names_its_consumer() {
    let inventory: BTreeSet<String> = indexes().into_iter().map(|i| i.name).collect();
    let registered: BTreeSet<String> = INDEX_CASES.iter().map(|c| c.index.to_string()).collect();
    let unregistered: Vec<_> = inventory.difference(&registered).collect();
    let stale: Vec<_> = registered.difference(&inventory).collect();
    assert!(
        unregistered.is_empty(),
        "indexes with no registered consumer: {unregistered:?}"
    );
    assert!(
        stale.is_empty(),
        "registered indexes the schema no longer declares: {stale:?}"
    );
    assert_eq!(
        registered.len(),
        INDEX_CASES.len(),
        "duplicate registry entries"
    );
}

#[test]
fn constraints_are_the_unique_indexes() {
    for info in indexes() {
        let case = INDEX_CASES
            .iter()
            .find(|c| c.index == info.name)
            .expect("registered");
        let declared_constraint = matches!(case.served_by, Constraint);
        assert_eq!(
            info.unique, declared_constraint,
            "{}: unique = {}, registered as constraint = {}",
            info.name, info.unique, declared_constraint
        );
    }
}

/// Prints the consumer kinds under `--nocapture`; the paths are the
/// consumers this crate cannot check, so their reasons are the review list.
#[test]
fn consumers_tally() {
    let (mut queries, mut lists, mut constraints, mut paths) = (0, 0, 0, Vec::new());
    for case in INDEX_CASES {
        match case.served_by {
            Queries(_) => queries += 1,
            List(_) => lists += 1,
            Constraint => constraints += 1,
            Path(reason) => paths.push((case.index, reason)),
        }
    }
    eprintln!(
        "index consumers: {queries} queries, {lists} lists, {constraints} constraints, {} paths",
        paths.len()
    );
    for (index, reason) in &paths {
        assert!(
            !reason.is_empty(),
            "{index}: a path consumer needs its reason"
        );
        eprintln!("  {index}: {reason}");
    }
}

#[test]
fn every_query_consumer_is_on_the_surface() {
    let surface = scan_surface();
    for case in INDEX_CASES {
        if let Queries(names) = case.served_by {
            for name in names {
                assert!(
                    surface.contains(*name),
                    "{}: `{name}` is not a query of this crate",
                    case.index
                );
            }
        }
    }
}

#[test]
fn every_list_consumer_is_a_combination_a_client_can_send() {
    for case in INDEX_CASES {
        if let List(check) = case.served_by {
            check().unwrap_or_else(|e| panic!("{}: {e}", case.index));
        }
    }
}
