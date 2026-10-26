//
// Copyright 2026 Tabsdata Inc.
//

use crate::function::engine::spec::{ResolvedSpec, ResolvedTable};
use crate::function::engine::state::FunctionState;
use crate::tag::parent::parent_version;
use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use td_error::TdError;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::dependency::DependencyInsertDB;
use td_objects::dxo::function::FunctionInsertDB;
use td_objects::dxo::table::{TableDB, TableInsertDB};
use td_objects::dxo::tag::{TopologyTagDB, TopologyTagInsertDB};
use td_objects::dxo::trigger::TriggerInsertDB;
use td_objects::system::SYSTEM_OUTPUT_TABLE_NAMES_PREFIXES;
use td_objects::types::basic::{
    DependencyId, DependencyPos, DependencyVersionId, FunctionId, Partitioned, Private, System,
    TableFunctionParamPos, TableId, TableStatus, TableVersionId, TagEntity, TagId, TagValue,
    TriggerId, TriggerStatus, TriggerVersionId,
};

/// A table whose status changed, so the triggers reading it follow.
pub struct TableTransition {
    pub table_id: TableId,
    pub status: TableStatus,
    /// The function producing the table after the transition.
    pub function_id: FunctionId,
}

/// The SCD2 rows that take a function from its state to what a request wants:
/// a new version only where something changed, a close where something went.
/// Frozen is a version, not a close: a frozen table or function is detached
/// but kept, and may be adopted again.
#[derive(Default)]
pub struct Changes {
    pub tables: Vec<TableInsertDB>,
    pub close_tables: Vec<TableId>,
    pub table_transitions: Vec<TableTransition>,
    pub dependencies: Vec<DependencyInsertDB>,
    pub close_dependencies: Vec<DependencyId>,
    pub triggers: Vec<TriggerInsertDB>,
    pub close_triggers: Vec<TriggerId>,
    pub tags: Vec<TopologyTagInsertDB>,
    pub close_tags: Vec<TagId>,
}

impl Changes {
    /// Register (empty state) and update (loaded state) against a spec;
    /// `parents` are the open parent tags of the resolved tables.
    pub fn for_spec(
        state: &FunctionState,
        resolved: &ResolvedSpec,
        parents: &HashMap<TableId, TopologyTagDB>,
        function: &FunctionInsertDB,
        ctx: &RequestContext,
    ) -> Result<Self, TdError> {
        let mut changes = Self::default();
        changes.diff_tables(state, resolved, function, ctx)?;
        changes.diff_table_parents(resolved, parents, ctx)?;
        changes.diff_dependencies(state, resolved, function, ctx)?;
        changes.diff_triggers(state, resolved, function, ctx);
        Ok(changes)
    }

    /// Delete: user tables freeze so their data stays adoptable, system tables
    /// close so nothing can reattach a state table, everything else closes.
    pub fn for_delete(state: &FunctionState, ctx: &RequestContext) -> Self {
        let mut changes = Self::default();
        for table in &state.tables {
            if table.status != TableStatus::Active {
                continue;
            }
            if is_system_table(table) {
                changes.close_tables.push(table.table_id);
            } else {
                changes.freeze(table, ctx);
            }
        }
        changes.close_dependencies = state.dependencies.iter().map(|d| d.dependency_id).collect();
        changes.close_triggers = state.triggers.iter().map(|t| t.trigger_id).collect();
        changes
    }

    /// A frozen table keeps every tag: its `_parent` is how deleting the
    /// parent table still finds it, and its user tags go to whoever adopts it.
    fn freeze(&mut self, table: &TableDB, ctx: &RequestContext) {
        self.tables.push(table.frozen_version(ctx));
        self.table_transitions.push(TableTransition {
            table_id: table.table_id,
            status: TableStatus::Frozen,
            function_id: table.function_id,
        });
    }

    fn diff_tables(
        &mut self,
        state: &FunctionState,
        resolved: &ResolvedSpec,
        function: &FunctionInsertDB,
        ctx: &RequestContext,
    ) -> Result<(), TdError> {
        let existing: HashMap<_, &TableDB> = state.tables.iter().map(|t| (&t.name, t)).collect();
        let mut kept = HashSet::new();
        for table in &resolved.tables {
            kept.insert(&table.name);
            let desired = desired_table(table, function, ctx)?;
            match existing.get(&table.name) {
                Some(current) if same_table(current, &desired) => {}
                current => {
                    if let Some(current) =
                        current.filter(|current| current.table_id != desired.table_id)
                    {
                        self.close_tables.push(current.table_id);
                    }
                    if table.adopted_frozen {
                        self.table_transitions.push(TableTransition {
                            table_id: desired.table_id,
                            status: TableStatus::Active,
                            function_id: function.function_id,
                        });
                    }
                    self.tables.push(desired);
                }
            }
        }
        for table in &state.tables {
            if kept.contains(&table.name) || table.status != TableStatus::Active {
                continue;
            }
            self.freeze(table, ctx);
        }
        Ok(())
    }

    fn diff_table_parents(
        &mut self,
        resolved: &ResolvedSpec,
        parents: &HashMap<TableId, TopologyTagDB>,
        ctx: &RequestContext,
    ) -> Result<(), TdError> {
        for table in &resolved.tables {
            let current = parents.get(&table.table_id);
            match table.parent {
                Some(parent) => self.tags.extend(parent_version(
                    ctx,
                    TagEntity::from(table.table_id),
                    TagValue::of_id(parent),
                    current,
                )?),
                None => self
                    .close_tags
                    .extend(current.map(|current| current.tag_id)),
            }
        }
        Ok(())
    }

    fn diff_dependencies(
        &mut self,
        state: &FunctionState,
        resolved: &ResolvedSpec,
        function: &FunctionInsertDB,
        ctx: &RequestContext,
    ) -> Result<(), TdError> {
        let key = |d: &DependencyInsertDB| {
            (
                *d.table_collection_id.deref(),
                d.table_id,
                d.table_versions.deref().clone(),
                *d.dep_pos.deref(),
            )
        };
        let existing: HashMap<_, _> = state
            .dependencies
            .iter()
            .map(|d| (key(&d.base), d))
            .collect();
        let mut kept = HashSet::new();
        for dependency in &resolved.dependencies {
            let desired = DependencyInsertDB {
                id: DependencyVersionId::default(),
                collection_id: function.collection_id,
                dependency_id: DependencyId::default(),
                function_id: function.function_id,
                table_collection_id: dependency.table.collection_id.into(),
                table_function_id: dependency.table.function_id.into(),
                table_id: dependency.table.table_id,
                table_versions: dependency.versions.clone().into(),
                dep_pos: DependencyPos::try_from(dependency.pos)?,
                defined_by_id: ctx.user_id,
                system: System::from(dependency.pos < 0),
            };
            let k = key(&desired);
            if existing.contains_key(&k) {
                kept.insert(k);
                continue;
            }
            self.dependencies.push(desired);
        }
        self.close_dependencies = existing
            .iter()
            .filter(|(k, _)| !kept.contains(*k))
            .map(|(_, d)| d.dependency_id)
            .collect();
        Ok(())
    }

    fn diff_triggers(
        &mut self,
        state: &FunctionState,
        resolved: &ResolvedSpec,
        function: &FunctionInsertDB,
        ctx: &RequestContext,
    ) {
        let existing: HashMap<_, _> = state
            .triggers
            .iter()
            .map(|t| {
                (
                    (*t.trigger_by_collection_id.deref(), t.trigger_by_table_id),
                    t,
                )
            })
            .collect();
        let mut kept = HashSet::new();
        for trigger in &resolved.triggers {
            let k = (trigger.table.collection_id, trigger.table.table_id);
            let current = existing.get(&k);
            let trigger_id = current.map_or_else(TriggerId::default, |t| t.trigger_id);
            // A trigger fires only while its source table is produced.
            let status = match trigger.table.status {
                TableStatus::Frozen => TriggerStatus::Frozen,
                TableStatus::Active => TriggerStatus::Active,
            };
            let unchanged = current.is_some_and(|t| {
                t.status == status && *t.trigger_by_function_id.deref() == trigger.table.function_id
            });
            kept.insert(k);
            if unchanged {
                continue;
            }
            self.triggers.push(TriggerInsertDB {
                id: TriggerVersionId::default(),
                collection_id: function.collection_id,
                trigger_id,
                function_id: function.function_id,
                trigger_by_collection_id: trigger.table.collection_id.into(),
                trigger_by_function_id: trigger.table.function_id.into(),
                trigger_by_table_id: trigger.table.table_id,
                status,
                defined_by_id: ctx.user_id,
                system: trigger.table.system,
            });
        }
        self.close_triggers = existing
            .iter()
            .filter(|(k, _)| !kept.contains(*k))
            .map(|(_, t)| t.trigger_id)
            .collect();
    }
}

fn desired_table(
    table: &ResolvedTable,
    function: &FunctionInsertDB,
    ctx: &RequestContext,
) -> Result<TableInsertDB, TdError> {
    Ok(TableInsertDB {
        id: TableVersionId::default(),
        collection_id: function.collection_id,
        table_id: table.table_id,
        name: table.name.clone(),
        function_id: function.function_id,
        function_version_id: function.id.clone(),
        function_param_pos: Some(TableFunctionParamPos::try_from(table.pos)?),
        private: Private::from(table.name.is_private()),
        partitioned: Partitioned::FALSE,
        defined_by_id: ctx.user_id,
        status: TableStatus::Active,
        table_type: table.table_type.clone(),
    })
}

/// Same definition: only the version identity, the defining function version
/// and the stamps may differ.
fn same_table(current: &TableInsertDB, desired: &TableInsertDB) -> bool {
    current.status == TableStatus::Active
        && current.table_id == desired.table_id
        && current.function_param_pos == desired.function_param_pos
        && current.private == desired.private
        && current.partitioned == desired.partitioned
        && current.table_type == desired.table_type
}

pub(crate) fn is_system_table(table: &TableDB) -> bool {
    SYSTEM_OUTPUT_TABLE_NAMES_PREFIXES
        .iter()
        .any(|prefix| table.name.starts_with(prefix))
}
