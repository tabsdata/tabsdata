//
// Copyright 2025 Tabsdata Inc.
//

use itertools::Itertools;
use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use ta_services::ctx_service::{Message, ReqCtx};
use td_bridge::{BridgeProviderHandle, Orchestrator};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::DisplayVec;
use td_error::{ApiError, TdError, api_error, td_error};
use td_execution::VersionResolver;
use td_objects::dxo::collection::ActiveCollectionDB;
use td_objects::dxo::connection::ConnectionDB;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::dependency::DependencyDB;
use td_objects::dxo::function::FunctionDB;
use td_objects::dxo::table::{TableDB, TableDBWithNames};
use td_objects::dxo::trigger::{TriggerDB, TriggerDBWithNames};
use td_objects::dxo::worker::{WorkerDB, WorkerUpdateDB};
use td_objects::system::{
    SYSTEM_INPUT_TABLE_DEPENDENCY_PREFIXES, SYSTEM_OUTPUT_TABLE_NAMES_PREFIXES, TD_FN_STATE_PREFIX,
};
use td_objects::types::basic::{
    AtTime, CollectionId, CollectionName, ConnectionKind, Decorator, DependencyId, DependencyPos,
    DependencyStatus, DependencyVersionId, FunctionId, FunctionVersionId, Partitioned, Private,
    ProjectId, ResetState, ReuseFrozen, System, TableFunctionParamPos, TableId, TableName,
    TableNameDto, TableStatus, TableType, TableVersionId, TriggerId, TriggerStatus,
    TriggerVersionId, TriggeredOn, WorkerConfig, WorkerId, WorkerMessageStatus, WorkerStatus,
    WorkerType, WorkerTypeDto,
};
use td_objects::types::composed::{
    TableDependency, TableDependencyDto, TableTrigger, TableTriggerDto,
};
use td_objects::{and, or};
use td_queries::{ChunkedQueries, HistoricEntry, LiveEntry};
use tracing::{debug, error, warn};

#[td_error]
pub enum RegisterFunctionError {
    #[error("Tables [{0}] already exists in collection '{1}'")]
    TableAlreadyExists(DisplayVec<TableName>, CollectionName) = 0,
    #[error("Dependency table '{0}' does not exist")]
    DependencyTableDoesNotExist(TableDependency) = 1,
    #[error("Table '{0}' cannot trigger its own function")]
    SelfTrigger(TableTrigger) = 2,
    #[error("Trigger table '{0}' does not exist")]
    TriggerTableDoesNotExist(TableTrigger) = 3,
    #[error("Table '{0}' exists in collection '{1}' but is in frozen state: {2}")]
    FrozenTableAlreadyExists(TableName, CollectionName, String) = 4,
    #[error("Cannot use private table '{0}/{1}' from another collection")]
    PrivateTableDependency(CollectionName, TableName) = 5,
    #[error("Duplicate output tables defined: [{0}]")]
    DuplicateTableDefinition(DisplayVec<TableName>) = 6,
    #[error("A {0:?} function must declare the connection kind it uses")]
    ConnectionKindRequired(Decorator) = 7,
    #[error("A transformer function cannot declare a connection kind (got '{0}')")]
    ConnectionKindNotAllowed(ConnectionKind) = 8,
    #[error("Collection '{0}' has no connection; a publisher/subscriber requires one")]
    CollectionHasNoConnection(CollectionName) = 9,
    #[error("Connection kind '{0}' does not match collection '{2}' connection kind '{1}'")]
    ConnectionKindMismatch(ConnectionKind, ConnectionKind, CollectionName) = 10,
}

/// A publisher/subscriber must declare the connection kind it uses, its
/// collection must carry a connection, and the two kinds must match. A
/// transformer must declare none.
///
/// TODO FOR 2.0: this contract is enforced only under the Kubernetes
/// orchestrator (the 2.x deployment). The 1.x local Supervisor still runs
/// the legacy `tabsdata` client, which declares no connection kind and uses
/// connection-less collections, so the contract cannot hold there -- the
/// guard below skips the check. Remove the guard once the legacy client is
/// retired, so the check applies under every orchestrator.
pub async fn ensure_connection_kind_matches(
    conn: &mut DomainConnection<Apiserver>,
    bridge: &BridgeProviderHandle,
    function: &FunctionDB,
    collection: &ActiveCollectionDB,
) -> Result<(), TdError> {
    // TODO FOR 2.0: drop this guard (see fn doc) -- enforce on every orchestrator.
    if !matches!(bridge.orchestrator(), Orchestrator::Kubernetes) {
        return Ok(());
    }
    match &function.decorator {
        Decorator::Publisher | Decorator::Subscriber => {
            let function_connection = function.connection.as_ref().ok_or_else(|| {
                RegisterFunctionError::ConnectionKindRequired(function.decorator.clone())
            })?;
            let connection_id = collection.connection_id.ok_or_else(|| {
                RegisterFunctionError::CollectionHasNoConnection(collection.name.clone())
            })?;
            let connection: ConnectionDB = conn.live().fetch_one(connection_id).await?;
            if *function_connection != connection.kind {
                Err(RegisterFunctionError::ConnectionKindMismatch(
                    function_connection.clone(),
                    connection.kind.clone(),
                    collection.name.clone(),
                ))?
            }
        }
        Decorator::Transformer => {
            if let Some(connection) = function.connection.as_ref() {
                Err(RegisterFunctionError::ConnectionKindNotAllowed(
                    connection.clone(),
                ))?
            }
        }
    }
    Ok(())
}

pub async fn validate_tables_do_not_exist(
    conn: &mut DomainConnection<Apiserver>,
    collection_id: &CollectionId,
    collection_name: &CollectionName,
    existing_versions: &[TableDB],
    new_tables: &Option<Vec<TableNameDto>>,
    at_time: &AtTime,
) -> Result<(), TdError> {
    if let Some(new_tables) = new_tables.as_ref() {
        let duplicates = new_tables
            .iter()
            .duplicates()
            .map(TableName::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        if !duplicates.is_empty() {
            Err(RegisterFunctionError::DuplicateTableDefinition(
                duplicates.into(),
            ))?
        }

        // We use existing tables to discard lookups in updates (as when registering a function,
        // there is no existing tables).
        let existing_tables_map = existing_versions
            .iter()
            .map(|t| ((&t.collection_id, &t.name), t))
            .collect::<HashMap<_, _>>();
        let new_table_names: Vec<TableName> = new_tables
            .iter()
            .map(TableName::try_from)
            .filter_ok(|t| !existing_tables_map.contains_key(&(collection_id, t)))
            .collect::<Result<Vec<_>, _>>()?;

        let new_table_lookups: Vec<_> = new_table_names
            .iter()
            .map(|t| and!(collection_id, t))
            .collect();
        let tables_found = conn
            .historic(at_time)
            .state(TableDBWithNames::available())
            .fetch_all_chunked::<_>(&new_table_lookups)
            .await?;

        if !tables_found.is_empty() {
            let duplicate_tables: Vec<_> = tables_found.into_iter().map(|t| t.base.name).collect();
            Err(RegisterFunctionError::TableAlreadyExists(
                duplicate_tables.into(),
                collection_name.clone(),
            ))?
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn build_table_versions(
    msgs: &mut ReqCtx,
    collection_id: &CollectionId,
    collection_name: &CollectionName,
    function_id: &FunctionId,
    existing_versions: &[TableDB],
    new_tables: &Option<Vec<TableNameDto>>,
    new_tables_types: &Option<HashMap<TableNameDto, TableType>>,
    function: &FunctionDB,
    request_context: &RequestContext,
    reuse_frozen: &ReuseFrozen,
    reset_state: &ResetState,
) -> Result<Vec<TableDB>, TdError> {
    let mut new_table_versions = HashMap::new();

    let existing_versions: HashMap<_, _> = existing_versions
        .iter()
        .map(|t| ((&t.collection_id, &t.name), t))
        .collect();

    let new_tables_types = match new_tables_types {
        Some(new_tables_types) => new_tables_types,
        None => &HashMap::new(),
    };

    // Add iterators of new tables (system tables with negative pos)
    let user_tables = new_tables
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|n| Ok((TableName::try_from(n)?, new_tables_types.get(n))))
        .collect::<Result<Vec<_>, TdError>>()?;
    let user_tables = user_tables
        .iter()
        .enumerate()
        .map(|(pos, table_name)| (pos as i32, table_name));

    let new_system_tables = SYSTEM_OUTPUT_TABLE_NAMES_PREFIXES
        .iter()
        .map(|prefix| {
            // system tables are the same for the same function
            // TODO we need to make sure these names are not going to conflict with user tables
            let table_name = format!("{prefix}__{function_id}");
            Ok((TableName::try_from(table_name)?, None))
        })
        .collect::<Result<Vec<_>, TdError>>()?;
    let system_tables = new_system_tables
        .iter()
        .enumerate()
        .map(|(pos, table_name)| (-(pos as i32 + 1), table_name));

    let new_tables = user_tables.chain(system_tables).collect::<Vec<_>>();

    for (pos, (table_name, table_type)) in new_tables {
        // Reuse table id if the table is the same
        let existing_version = existing_versions.get(&(collection_id, table_name));
        // `reset_state` forces a fresh id for the function state table so it
        // starts empty (publishers re-read initial config). User tables keep
        // their reuse behavior.
        let reset_fn_state = **reset_state && table_name.starts_with(TD_FN_STATE_PREFIX);
        let table_id = if reset_fn_state {
            &TableId::default()
        } else if let Some(existing_version) = existing_version {
            match existing_version.status {
                TableStatus::Active => &existing_version.table_id,
                TableStatus::Frozen => {
                    // We can only unfreeze a frozen table if reuse_frozen is enabled
                    if **reuse_frozen {
                        msgs.warning(Message::of(
                            RegisterFunctionError::FrozenTableAlreadyExists(
                                table_name.clone(),
                                collection_name.clone(),
                                "unfreezing with new table definition".to_string(),
                            ),
                        ));

                        &existing_version.table_id
                    } else {
                        Err(RegisterFunctionError::FrozenTableAlreadyExists(
                            table_name.clone(),
                            collection_name.clone(),
                            "could not reuse frozen table".to_string(),
                        ))?
                    }
                }
                TableStatus::Deleted => {
                    // Status deleted, table is detached from the created table
                    &TableId::default()
                }
            }
        } else {
            &TableId::default()
        };

        let table_version = TableDB {
            id: TableVersionId::default(),
            collection_id: function.collection_id,
            table_id: *table_id,
            name: table_name.clone(),
            function_id: function.function_id,
            function_version_id: function.id.clone(),
            function_param_pos: Some(TableFunctionParamPos::try_from(pos)?),
            private: Private::from(table_name.is_private()),
            partitioned: Partitioned::FALSE,
            defined_on: request_context.time,
            defined_by_id: request_context.user_id,
            status: TableStatus::Active,
            table_type: table_type.cloned().unwrap_or_default(),
        };

        new_table_versions.insert((collection_id, table_name), table_version);
    }

    for (key, existing_version) in existing_versions {
        if !new_table_versions.contains_key(&key) {
            let table_version = TableDB {
                id: TableVersionId::default(),
                collection_id: function.collection_id,
                table_id: existing_version.table_id,
                name: existing_version.name.clone(),
                function_id: function.function_id,
                function_version_id: function.id.clone(),
                function_param_pos: existing_version.function_param_pos,
                private: Private::FALSE,
                partitioned: Partitioned::FALSE,
                defined_on: request_context.time,
                defined_by_id: request_context.user_id,
                status: TableStatus::Frozen,
                table_type: TableType::default(),
            };

            new_table_versions.insert((collection_id, &existing_version.name), table_version);
        }
    }

    let new_table_versions = new_table_versions.into_values().collect();
    Ok(new_table_versions)
}

pub async fn build_tables_trigger_versions(
    conn: &mut DomainConnection<Apiserver>,
    at_time: &AtTime,
    new_table_versions: &[TableDB],
) -> Result<Vec<TriggerDB>, TdError> {
    let table_ids = new_table_versions
        .iter()
        .map(|t| t.table_id)
        .collect::<Vec<_>>();
    let existing_triggers = conn
        .historic(at_time)
        .fetch_all_chunked::<TriggerDB, _>(&table_ids)
        .await?;

    // Freeze triggers if the table is frozen, or reactivate them if the table is active again.
    // This has to be done downstream, because tables trigger functions.
    // This is needed because tables can change the function they are generated by. So
    // triggers need to change that too.
    let table_versions_map: HashMap<_, _> = new_table_versions
        .iter()
        .map(|t| (&t.table_id, t))
        .collect();
    let trigger_versions = existing_triggers
        .into_iter()
        .filter_map(|trigger| {
            let table = match table_versions_map.get(&trigger.trigger_by_table_id) {
                Some(table) => table,
                None => return None, // No matching table version found
            };
            match (&table.status, &trigger.status) {
                (TableStatus::Frozen, TriggerStatus::Active) => Some(TriggerDB {
                    id: TriggerVersionId::default(),
                    status: TriggerStatus::Frozen,
                    defined_on: *at_time,
                    ..trigger.clone()
                }),
                (TableStatus::Active, TriggerStatus::Frozen) => Some(TriggerDB {
                    id: TriggerVersionId::default(),
                    trigger_by_function_id: table.function_id,
                    status: TriggerStatus::Active,
                    defined_on: *at_time,
                    ..trigger.clone()
                }),
                _ => None,
            }
        })
        .collect::<Vec<_>>();

    Ok(trigger_versions)
}

#[allow(clippy::too_many_arguments)]
pub async fn build_dependency_versions(
    conn: &mut DomainConnection<Apiserver>,
    existing_versions: &Vec<DependencyDB>,
    new_dependencies: &Option<Vec<TableDependencyDto>>,
    collection_in_context: &CollectionName,
    function_id: &FunctionId,
    function: &FunctionDB,
    request_context: &RequestContext,
    at_time: &AtTime,
) -> Result<Vec<DependencyDB>, TdError> {
    let mut new_dependency_versions = HashMap::new();

    let existing_versions: HashMap<_, _> = existing_versions
        .deref()
        .iter()
        .map(|d| {
            (
                (
                    d.table_collection_id,
                    d.table_id,
                    d.table_versions.deref(),
                    *d.dep_pos,
                ),
                d,
            )
        })
        .collect();

    // Add iterators of new dependencies (dependency tables with negative pos)
    let user_dependencies = new_dependencies
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(TableDependency::try_from)
        .collect::<Result<Vec<_>, _>>()?;
    let user_dependencies = user_dependencies
        .iter()
        .enumerate()
        .map(|(pos, table_name)| (pos as i32, table_name));

    let system_dependencies = SYSTEM_INPUT_TABLE_DEPENDENCY_PREFIXES
        .iter()
        .map(|prefix| {
            // system tables are the same for the same function
            // TODO we need to make sure these names are not going to conflict with user tables
            let table_name = format!("{prefix}__{function_id}");
            TableDependency::try_from(table_name)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let system_dependencies = system_dependencies
        .iter()
        .enumerate()
        .map(|(pos, table_name)| (-(pos as i32 + 1), table_name));

    let new_dependencies = user_dependencies
        .chain(system_dependencies)
        .collect::<Vec<_>>();

    // Here, even function created tables are already populated in the tables list.
    let dep_lookups: Vec<_> = new_dependencies
        .iter()
        .map(|(_, d)| {
            let col = d.collection.as_ref().unwrap_or(&*collection_in_context);
            and!(col, &d.table)
        })
        .collect();
    let tables_found = conn
        .historic(at_time)
        .state(TableDBWithNames::available())
        .fetch_all_chunked::<_>(&dep_lookups)
        .await?;

    let tables_found: HashMap<_, _> = tables_found
        .iter()
        .map(|t| ((&t.collection, &t.base.name), t))
        .collect();

    for (pos, dependency_table) in new_dependencies {
        let table_db = match tables_found.get(&(
            dependency_table
                .collection
                .as_ref()
                .unwrap_or(collection_in_context),
            &dependency_table.table,
        )) {
            Some(table_db) => Ok(table_db),
            None => Err(RegisterFunctionError::DependencyTableDoesNotExist(
                dependency_table.clone(),
            )),
        }?;

        if *table_db.base.private && table_db.collection != *collection_in_context {
            Err(RegisterFunctionError::PrivateTableDependency(
                table_db.collection.clone(),
                table_db.base.name.clone(),
            ))?
        }

        // Validate dependencies if we can (fixed versions should exist at registration time,
        // ranges should be older to newer, etc.). This avoids potential issues on trigger.
        // A standalone function should be at least triggerable at the time it is registered.
        let _ = VersionResolver::builder()
            .function_id(function_id)
            .table_id(&table_db.base.table_id)
            .versions(&dependency_table.versions)
            .triggered_on(&TriggeredOn::try_from(at_time)?)
            .error_on_desc_range(true)
            .build()?
            .resolve(&mut *conn)
            .await?;

        // Reuse dependency id if the dependency is the same
        let existing_version = existing_versions.get(&(
            table_db.base.collection_id,
            table_db.base.table_id,
            &dependency_table.versions,
            pos,
        ));
        let dependency_id = if let Some(existing_version) = existing_version {
            &existing_version.dependency_id
        } else {
            &DependencyId::default()
        };

        let dependency_version = DependencyDB {
            id: DependencyVersionId::default(),
            collection_id: function.collection_id,
            dependency_id: *dependency_id,
            function_id: function.function_id,
            table_collection_id: table_db.base.collection_id,
            table_function_id: table_db.base.function_id,
            table_id: table_db.base.table_id,
            table_versions: dependency_table.versions.clone().into(),
            dep_pos: DependencyPos::try_from(pos)?,
            status: DependencyStatus::Active,
            defined_on: request_context.time,
            defined_by_id: request_context.user_id,
            system: System::from(pos < 0),
        };

        new_dependency_versions.insert(
            (
                table_db.base.collection_id,
                table_db.base.table_id,
                &dependency_table.versions,
                pos,
            ),
            dependency_version,
        );
    }

    for (key, existing_version) in existing_versions {
        if !new_dependency_versions.contains_key(&key) {
            let dependency_version = DependencyDB {
                id: DependencyVersionId::default(),
                collection_id: function.collection_id,
                dependency_id: existing_version.dependency_id,
                function_id: function.function_id,
                table_collection_id: existing_version.table_collection_id,
                table_function_id: existing_version.function_id,
                table_id: existing_version.table_id,
                table_versions: existing_version.table_versions.clone(),
                dep_pos: existing_version.dep_pos,
                status: DependencyStatus::Deleted,
                defined_on: request_context.time,
                defined_by_id: request_context.user_id,
                system: existing_version.system,
            };

            new_dependency_versions.insert(
                (
                    existing_version.table_collection_id,
                    existing_version.table_id,
                    existing_version.table_versions.deref(),
                    *existing_version.dep_pos,
                ),
                dependency_version,
            );
        }
    }

    let new_dependency_versions = new_dependency_versions.into_values().collect();
    Ok(new_dependency_versions)
}

#[allow(clippy::too_many_arguments)]
pub async fn build_trigger_versions(
    conn: &mut DomainConnection<Apiserver>,
    existing_versions: &[TriggerDBWithNames],
    new_tables: &Option<Vec<TableNameDto>>,
    new_triggers: &Option<Vec<TableTriggerDto>>,
    new_dependencies: &Option<Vec<TableDependencyDto>>,
    collection_in_context: &CollectionName,
    function: &FunctionDB,
    request_context: &RequestContext,
    at_time: &AtTime,
) -> Result<Vec<TriggerDB>, TdError> {
    let mut new_trigger_versions = HashMap::new();

    let existing_versions: HashMap<_, _> = existing_versions
        .iter()
        .map(|t| {
            (
                (
                    &t.base.trigger_by_collection_id,
                    &t.base.trigger_by_table_id,
                ),
                t,
            )
        })
        .collect();

    let new_triggers = if let Some(new_triggers) = new_triggers.as_deref() {
        new_triggers
            .iter()
            .map(TableTrigger::try_from)
            .collect::<Result<Vec<_>, _>>()?
    } else {
        // function does not specify triggers (different from empty triggers), then all dependencies are triggers,
        // except those that are produced by the function itself and dedup them
        let new_tables = new_tables.as_deref().unwrap_or_default();
        new_dependencies
            .as_deref()
            .unwrap_or_default()
            .iter()
            .filter(|t| {
                !((t.collection.is_none()
                    || t.collection.as_deref() == Some(&*collection_in_context))
                    && new_tables.contains(&t.table))
            })
            .map(TableTrigger::try_from)
            .collect::<Result<HashSet<_>, _>>()?
            .into_iter()
            .collect()
    };

    // Here, even function created tables are already populated in the tables list.
    let trigger_lookups: Vec<_> = new_triggers
        .iter()
        .map(|d| {
            let col = d.collection.as_ref().unwrap_or(&*collection_in_context);
            and!(col, &d.table)
        })
        .collect();
    let tables_found = conn
        .historic(at_time)
        .state(TableDBWithNames::available())
        .fetch_all_chunked::<_>(&trigger_lookups)
        .await?;

    let tables_found: HashMap<_, _> = tables_found
        .iter()
        .map(|t: &TableDBWithNames| ((&t.collection, &t.base.name), t))
        .collect();

    for trigger_table in new_triggers {
        let table_db = match tables_found.get(&(
            trigger_table
                .collection
                .as_ref()
                .unwrap_or(collection_in_context),
            &trigger_table.table,
        )) {
            None => Err(RegisterFunctionError::TriggerTableDoesNotExist(
                trigger_table.clone(),
            )),
            Some(table_db)
                if table_db.collection == *collection_in_context
                    && new_tables
                        .as_deref()
                        .is_some_and(|t| t.iter().any(|t| **t == *table_db.base.name)) =>
            {
                Err(RegisterFunctionError::SelfTrigger(trigger_table.clone()))
            }
            Some(table_db) => Ok(table_db),
        }?;

        // Reuse trigger id if the trigger is the same
        let existing_version =
            existing_versions.get(&(&table_db.base.collection_id, &table_db.base.table_id));
        let trigger_id = if let Some(existing_version) = existing_version {
            existing_version.base.trigger_id
        } else {
            TriggerId::default()
        };

        let trigger_version = TriggerDB {
            id: TriggerVersionId::default(),
            collection_id: function.collection_id,
            trigger_id,
            function_id: function.function_id,
            trigger_by_collection_id: table_db.base.collection_id,
            trigger_by_function_id: table_db.base.function_id,
            trigger_by_table_id: table_db.base.table_id,
            status: TriggerStatus::Active,
            defined_on: request_context.time,
            defined_by_id: request_context.user_id,
            system: table_db.system,
        };

        new_trigger_versions.insert(
            (&table_db.base.collection_id, &table_db.base.table_id),
            trigger_version,
        );
    }

    for (key, existing_version) in existing_versions {
        if !new_trigger_versions.contains_key(&key) {
            let trigger_version = TriggerDB {
                id: TriggerVersionId::default(),
                collection_id: function.collection_id,
                trigger_id: existing_version.base.trigger_id,
                function_id: function.function_id,
                trigger_by_collection_id: existing_version.base.trigger_by_collection_id,
                trigger_by_function_id: existing_version.base.trigger_by_function_id,
                trigger_by_table_id: existing_version.base.trigger_by_table_id,
                status: TriggerStatus::Deleted,
                defined_on: request_context.time,
                defined_by_id: request_context.user_id,
                system: existing_version.base.system,
            };

            new_trigger_versions.insert(
                (
                    &existing_version.base.trigger_by_collection_id,
                    &existing_version.base.trigger_by_table_id,
                ),
                trigger_version,
            );
        }
    }

    let new_trigger_versions = new_trigger_versions.into_values().collect();
    Ok(new_trigger_versions)
}

pub async fn stop_current_workers(
    conn: &mut DomainConnection<Apiserver>,
    function_version_id: &Option<FunctionVersionId>,
) -> Result<(), TdError> {
    if let Some(function_version_id) = function_version_id {
        // we cannot stop function workers
        let to_stop = and!(
            function_version_id,
            or!(WorkerType::CronTrigger, WorkerType::StageTrigger),
        );
        let workers = conn.live().fetch_all::<WorkerDB>(to_stop).await?;

        // only if the status is allowed to be updated
        let mut to_stop = vec![];
        for worker in workers {
            match worker.status {
                WorkerStatus::Scheduled | WorkerStatus::Requested | WorkerStatus::Running => {
                    debug!(
                        "Stopping worker {} with status {}",
                        worker.id, worker.status
                    );
                    // Keyed on the status read: unlocked, and the tick writes
                    // these same rows.
                    to_stop.push(and!(worker.id, worker.status));
                }
                WorkerStatus::Done
                | WorkerStatus::StopScheduled
                | WorkerStatus::StopRequested
                | WorkerStatus::Stopped
                | WorkerStatus::Error
                | WorkerStatus::Failed
                | WorkerStatus::Canceled => {
                    debug!(
                        "Ignoring stop of worker {}, as it is already in status {}",
                        worker.id, worker.status
                    )
                }
            }
        }

        let update = WorkerUpdateDB::stop_scheduled();
        let res = conn
            .update_chunked::<_, WorkerDB, _>(&update, &to_stop)
            .await?;
        if res.rows_affected() != to_stop.len() as u64 {
            warn!(
                function_version_id = %function_version_id,
                expected = to_stop.len(),
                updated = res.rows_affected(),
                "register: worker(s) changed status before the stop landed"
            );
        }
        debug!(
            "Set stopped status in {} workers for function version {}.",
            res.rows_affected(),
            function_version_id
        );
    }

    Ok(())
}

pub async fn build_workers(
    conn: &mut DomainConnection<Apiserver>,
    function: &FunctionDB,
    project_id: &ProjectId,
    function_version_id: &Option<FunctionVersionId>,
    new: &Option<HashMap<WorkerTypeDto, WorkerConfig>>,
) -> Result<Vec<WorkerDB>, TdError> {
    let new_worker = |worker_type: WorkerType, worker_config: Option<WorkerConfig>| WorkerDB {
        id: WorkerId::new(),
        project_id: *project_id,
        collection_id: function.collection_id,
        execution_id: None,
        transaction_id: None,
        physical_function_run_id: None,
        function_version_id: function.id.clone(),
        message_status: WorkerMessageStatus::None,
        started_on: None,
        ended_on: None,
        status: WorkerStatus::Scheduled,
        worker_type,
        worker_enabled: function.enabled,
        worker_config,
    };
    let mut new_workers = vec![];
    if let Some(new) = new {
        match new.len() {
            0 => {
                debug!("No workers specified.");
            }
            1 => {
                for (worker_type, worker_config) in new {
                    let worker_db =
                        new_worker(worker_type.clone().into(), Some(worker_config.clone()));
                    new_workers.push(worker_db);
                    debug!(worker_type = %worker_type, "function.register: new worker created");
                }
            }
            _ => {
                // for now, we only support 1 worker per function
                error!("Multiple workers specified. This is not supported.");
                Err(api_error!(
                    ApiError::InputError,
                    "Multiple workers are not supported."
                ))?
            }
        }
    } else if let Some(function_version_id) = function_version_id {
        // if there is no info about new workers, we recreate what was present for the new version

        let to_recreate = and!(
            function_version_id,
            or!(WorkerType::CronTrigger, WorkerType::StageTrigger),
        );
        let current_workers = conn.live().fetch_all::<WorkerDB>(to_recreate).await?;
        for worker in current_workers {
            let worker_db = new_worker(worker.worker_type, worker.worker_config.clone());
            new_workers.push(worker_db);
        }
    }

    debug!("Created {} new workers.", new_workers.len());
    Ok(new_workers)
}
