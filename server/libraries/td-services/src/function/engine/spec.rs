//
// Copyright 2026 Tabsdata Inc.
//

use crate::function::engine::FunctionState;
use crate::function::register::RegisterFunctionError;
use itertools::Itertools;
use std::collections::{HashMap, HashSet};
use td_database::access::SeesProjectRows;
use td_database::domain::{Apiserver, DomainConnection};
use td_error::{TdError, td_error};
use td_execution::VersionResolver;
use td_objects::and;
use td_objects::dxo::collection::ActiveCollectionDB;
use td_objects::dxo::function::{FunctionRegister, FunctionUpdate};
use td_objects::dxo::table::TableDBWithNames;
use td_objects::system::{
    SYSTEM_INPUT_TABLE_DEPENDENCY_PREFIXES, SYSTEM_OUTPUT_TABLE_NAMES_PREFIXES, TD_FN_STATE_PREFIX,
};
use td_objects::types::basic::{
    AtTime, CollectionId, CollectionName, FunctionId, ResetState, ReuseFrozen, System, TableId,
    TableName, TableNameDto, TableStatus, TableType, TriggeredOn,
};
use td_objects::types::composed::{
    TableDependency, TableDependencyDto, TableTrigger, TableTriggerDto,
};
use td_objects::types::table_ref::Versions;
use td_queries::Queries;

/// What a request asks a function's relations to be. `None` for tables,
/// dependencies or triggers means "none", except triggers, which then derive
/// from the dependencies on tables the function does not produce itself.
pub struct FunctionSpec<'a> {
    pub tables: Option<&'a [TableNameDto]>,
    pub table_types: Option<&'a HashMap<TableNameDto, TableType>>,
    pub dependencies: Option<&'a [TableDependencyDto]>,
    pub triggers: Option<&'a [TableTriggerDto]>,
    pub reuse_frozen: ReuseFrozen,
    pub reset_state: ResetState,
}

impl<'a> From<&'a FunctionRegister> for FunctionSpec<'a> {
    fn from(register: &'a FunctionRegister) -> Self {
        Self {
            tables: register.tables.as_deref(),
            table_types: register.table_types.as_ref(),
            dependencies: register.dependencies.as_deref(),
            triggers: register.triggers.as_deref(),
            reuse_frozen: register.reuse_frozen_tables,
            reset_state: ResetState::default(),
        }
    }
}

impl<'a> From<&'a FunctionUpdate> for FunctionSpec<'a> {
    fn from(update: &'a FunctionUpdate) -> Self {
        Self {
            tables: update.tables.as_deref(),
            table_types: update.table_types.as_ref(),
            dependencies: update.dependencies.as_deref(),
            triggers: update.triggers.as_deref(),
            reuse_frozen: update.reuse_frozen_tables,
            reset_state: update.reset_state,
        }
    }
}

/// A table the function will produce, with the id it keeps or gets.
pub struct ResolvedTable {
    pub pos: i32,
    pub name: TableName,
    pub table_type: TableType,
    pub table_id: TableId,
}

/// A table another relation points at, as the request named it.
#[derive(Clone)]
pub struct ReferencedRow {
    pub collection_id: CollectionId,
    pub function_id: FunctionId,
    pub table_id: TableId,
    pub system: System,
    pub status: TableStatus,
}

pub struct ResolvedDependency {
    pub pos: i32,
    pub versions: Versions,
    pub table: ReferencedRow,
}

pub struct ResolvedTrigger {
    pub table: ReferencedRow,
}

/// The spec with every name turned into a row and every rule checked.
pub struct ResolvedSpec {
    pub tables: Vec<ResolvedTable>,
    pub dependencies: Vec<ResolvedDependency>,
    pub triggers: Vec<ResolvedTrigger>,
}

/// Resolve `spec` against the collection and the function's current state:
/// duplicate and private-table rules, table ownership, the reuse of existing
/// and frozen table ids, the tables dependencies and triggers reference (one
/// batched lookup), self-triggers, and the version selectors.
pub async fn resolve<S: SeesProjectRows>(
    conn: &mut DomainConnection<Apiserver, S>,
    collection: &ActiveCollectionDB,
    function_id: &FunctionId,
    spec: &FunctionSpec<'_>,
    state: &FunctionState,
    at: &AtTime,
) -> Result<ResolvedSpec, TdError> {
    let user_tables = spec.tables.unwrap_or_default();
    let duplicates: Vec<TableName> = user_tables
        .iter()
        .duplicates()
        .map(TableName::try_from)
        .try_collect()?;
    if !duplicates.is_empty() {
        Err(RegisterFunctionError::DuplicateTableDefinition(
            duplicates.into(),
        ))?
    }
    check_private_tables(&collection.name, spec.dependencies)?;
    check_private_tables(&collection.name, spec.triggers)?;

    let tables = resolve_tables(conn, collection, function_id, spec, state, at).await?;

    let dependencies: Vec<TableDependency> = spec
        .dependencies
        .unwrap_or_default()
        .iter()
        .map(TableDependency::try_from)
        .try_collect()?;
    let triggers = trigger_list(spec, &collection.name)?;

    // The function's own tables, system ones included, are addressable before
    // they are written: a dependency on the state table is resolved here.
    let mut referenced =
        referenced_rows(conn, &collection.name, &dependencies, &triggers, at).await?;
    for table in &tables {
        let row = ReferencedRow {
            collection_id: collection.id,
            function_id: *function_id,
            table_id: table.table_id,
            system: System::from(table.pos < 0),
            status: TableStatus::Active,
        };
        referenced.insert((collection.name.clone(), table.name.clone()), row);
    }

    let system_dependencies: Vec<TableDependency> = SYSTEM_INPUT_TABLE_DEPENDENCY_PREFIXES
        .iter()
        .map(|prefix| TableDependency::try_from(format!("{prefix}__{function_id}")))
        .try_collect()?;
    let mut resolved_dependencies = Vec::new();
    for (pos, dependency) in positioned(&dependencies, &system_dependencies) {
        let key = (
            dependency
                .collection
                .clone()
                .unwrap_or_else(|| collection.name.clone()),
            dependency.table.clone(),
        );
        let row = referenced.get(&key).ok_or_else(|| {
            RegisterFunctionError::DependencyTableDoesNotExist(dependency.clone())
        })?;
        VersionResolver::builder()
            .function_id(function_id)
            .table_id(&row.table_id)
            .versions(&dependency.versions)
            .triggered_on(&TriggeredOn::try_from(at)?)
            .error_on_desc_range(true)
            .build()?
            .resolve(&mut *conn)
            .await?;
        resolved_dependencies.push(ResolvedDependency {
            pos,
            versions: dependency.versions.clone(),
            table: row.clone(),
        });
    }

    let own_tables: HashSet<&TableNameDto> = user_tables.iter().collect();
    let mut resolved_triggers = Vec::new();
    for trigger in &triggers {
        let key = (
            trigger
                .collection
                .clone()
                .unwrap_or_else(|| collection.name.clone()),
            trigger.table.clone(),
        );
        let row = referenced
            .get(&key)
            .ok_or_else(|| RegisterFunctionError::TriggerTableDoesNotExist(trigger.clone()))?;
        let own_collection = trigger
            .collection
            .as_ref()
            .is_none_or(|c| *c == collection.name);
        if own_collection && own_tables.iter().any(|t| ***t == *trigger.table) {
            Err(RegisterFunctionError::SelfTrigger(trigger.clone()))?
        }
        resolved_triggers.push(ResolvedTrigger { table: row.clone() });
    }

    Ok(ResolvedSpec {
        tables,
        dependencies: resolved_dependencies,
        triggers: resolved_triggers,
    })
}

/// User tables in request order, then system tables at negative positions.
fn positioned<'a, T>(user: &'a [T], system: &'a [T]) -> impl Iterator<Item = (i32, &'a T)> {
    let user = user.iter().enumerate().map(|(pos, t)| (pos as i32, t));
    let system = system
        .iter()
        .enumerate()
        .map(|(pos, t)| (-(pos as i32 + 1), t));
    user.chain(system)
}

async fn resolve_tables<S: SeesProjectRows>(
    conn: &mut DomainConnection<Apiserver, S>,
    collection: &ActiveCollectionDB,
    function_id: &FunctionId,
    spec: &FunctionSpec<'_>,
    state: &FunctionState,
    at: &AtTime,
) -> Result<Vec<ResolvedTable>, TdError> {
    let existing: HashMap<&TableName, _> = state.tables.iter().map(|t| (&t.base.name, t)).collect();
    let empty_types = HashMap::new();
    let table_types = spec.table_types.unwrap_or(&empty_types);

    let user_tables: Vec<(TableName, TableType)> = spec
        .tables
        .unwrap_or_default()
        .iter()
        .map(|dto| {
            Ok::<_, TdError>((
                TableName::try_from(dto)?,
                table_types.get(dto).cloned().unwrap_or_default(),
            ))
        })
        .try_collect()?;
    let system_tables: Vec<(TableName, TableType)> = SYSTEM_OUTPUT_TABLE_NAMES_PREFIXES
        .iter()
        .map(|prefix| {
            Ok::<_, TdError>((
                TableName::try_from(format!("{prefix}__{function_id}"))?,
                TableType::default(),
            ))
        })
        .try_collect()?;

    let new_names: Vec<&TableName> = user_tables
        .iter()
        .map(|(name, _)| name)
        .filter(|name| !existing.contains_key(name))
        .collect();
    if !new_names.is_empty() {
        let lookups: Vec<_> = new_names
            .iter()
            .map(|name| and!(&collection.id, *name))
            .collect();
        let taken: Vec<TableDBWithNames> = conn.as_of(at).chunked().fetch_all(&lookups, ()).await?;
        if !taken.is_empty() {
            let names: Vec<TableName> = taken.into_iter().map(|t| t.base.base.name).collect();
            Err(RegisterFunctionError::TableAlreadyExists(
                names.into(),
                collection.name.clone(),
            ))?
        }
    }

    let mut tables = Vec::new();
    for (pos, (name, table_type)) in positioned(&user_tables, &system_tables) {
        let reset_fn_state = *spec.reset_state && name.starts_with(TD_FN_STATE_PREFIX);
        let table_id = match existing.get(name) {
            _ if reset_fn_state => TableId::default(),
            None => TableId::default(),
            Some(current) => match current.base.status {
                TableStatus::Active => current.base.table_id,
                TableStatus::Frozen if *spec.reuse_frozen => current.base.table_id,
                TableStatus::Frozen => Err(RegisterFunctionError::FrozenTableAlreadyExists(
                    name.clone(),
                    collection.name.clone(),
                ))?,
            },
        };
        tables.push(ResolvedTable {
            pos,
            name: name.clone(),
            table_type: table_type.clone(),
            table_id,
        });
    }
    Ok(tables)
}

/// Explicit triggers, else every dependency on a table the function does not
/// produce itself.
fn trigger_list(
    spec: &FunctionSpec<'_>,
    collection: &CollectionName,
) -> Result<Vec<TableTrigger>, TdError> {
    if let Some(triggers) = spec.triggers {
        return triggers.iter().map(TableTrigger::try_from).try_collect();
    }
    let own_tables = spec.tables.unwrap_or_default();
    let derived: HashSet<TableTrigger> = spec
        .dependencies
        .unwrap_or_default()
        .iter()
        .filter(|d| {
            let own_collection = d.collection.as_ref().is_none_or(|c| c == collection);
            !(own_collection && own_tables.contains(&d.table))
        })
        .map(TableTrigger::try_from)
        .try_collect()?;
    Ok(derived.into_iter().collect())
}

/// Every table a dependency or trigger names, fetched once and keyed by
/// (collection name, table name).
async fn referenced_rows<S: SeesProjectRows>(
    conn: &mut DomainConnection<Apiserver, S>,
    collection: &CollectionName,
    dependencies: &[TableDependency],
    triggers: &[TableTrigger],
    at: &AtTime,
) -> Result<HashMap<(CollectionName, TableName), ReferencedRow>, TdError> {
    let mut lookups = Vec::new();
    for d in dependencies {
        lookups.push(and!(d.collection.as_ref().unwrap_or(collection), &d.table));
    }
    for t in triggers {
        lookups.push(and!(t.collection.as_ref().unwrap_or(collection), &t.table));
    }
    let rows: Vec<TableDBWithNames> = conn.as_of(at).chunked().fetch_all(&lookups, ()).await?;
    Ok(rows
        .into_iter()
        .map(|t| {
            let row = ReferencedRow {
                collection_id: t.base.base.collection_id,
                function_id: t.base.base.function_id,
                table_id: t.base.base.table_id,
                system: t.system,
                status: t.base.base.status,
            };
            ((t.collection, t.base.base.name), row)
        })
        .collect())
}

/// A private table is usable only from its own collection.
trait ReferencedTable {
    fn referenced_collection(&self) -> &Option<CollectionName>;
    fn referenced_table(&self) -> &TableNameDto;

    fn can_be_used_from(&self, from: &CollectionName) -> bool {
        !self.referenced_table().is_private()
            || self
                .referenced_collection()
                .as_ref()
                .is_none_or(|c| c == from)
    }
}

impl ReferencedTable for TableDependencyDto {
    fn referenced_collection(&self) -> &Option<CollectionName> {
        &self.collection
    }

    fn referenced_table(&self) -> &TableNameDto {
        &self.table
    }
}

impl ReferencedTable for TableTriggerDto {
    fn referenced_collection(&self) -> &Option<CollectionName> {
        &self.collection
    }

    fn referenced_table(&self) -> &TableNameDto {
        &self.table
    }
}

#[td_error]
pub enum PrivateTableError {
    #[error("Cannot use private tables from other collections: {0}")]
    PrivateTableCannotBeUsed(String) = 0,
}

fn check_private_tables<T: ReferencedTable>(
    this_collection: &CollectionName,
    referenced: Option<&[T]>,
) -> Result<(), TdError> {
    let out_of_reach: Vec<String> = referenced
        .unwrap_or_default()
        .iter()
        .filter(|t| !t.can_be_used_from(this_collection))
        .map(|t| {
            let collection = t
                .referenced_collection()
                .as_ref()
                .expect("unusable implies a foreign collection");
            format!("'{collection}/{}'", t.referenced_table())
        })
        .collect();
    if !out_of_reach.is_empty() {
        Err(PrivateTableError::PrivateTableCannotBeUsed(
            out_of_reach.join(", "),
        ))?
    }
    Ok(())
}
