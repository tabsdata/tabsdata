//
// Copyright 2026 Tabsdata Inc.
//

use crate::function::engine::spec::{ResolvedSpec, ResolvedTable};
use crate::function::engine::state::FunctionState;
use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use td_error::TdError;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::dependency::DependencyInsertDB;
use td_objects::dxo::function::FunctionInsertDB;
use td_objects::dxo::table::{TableDB, TableInsertDB};
use td_objects::dxo::trigger::TriggerInsertDB;
use td_objects::system::SYSTEM_OUTPUT_TABLE_NAMES_PREFIXES;
use td_objects::types::basic::{
    DependencyId, DependencyPos, DependencyVersionId, FunctionId, Partitioned, Private, System,
    TableFunctionParamPos, TableId, TableStatus, TableVersionId, TriggerId, TriggerStatus,
    TriggerVersionId,
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
}

impl Changes {
    /// Register (empty state) and update (loaded state) against a spec.
    pub fn for_spec(
        state: &FunctionState,
        resolved: &ResolvedSpec,
        function: &FunctionInsertDB,
        ctx: &RequestContext,
    ) -> Result<Self, TdError> {
        let mut changes = Self::default();
        changes.diff_tables(state, resolved, function, ctx)?;
        changes.diff_dependencies(state, resolved, function, ctx)?;
        changes.diff_triggers(state, resolved, function, ctx);
        Ok(changes)
    }

    /// Delete: user tables freeze so their data stays adoptable, system tables
    /// close so nothing can reattach a state table, everything else closes.
    pub fn for_delete(state: &FunctionState, ctx: &RequestContext) -> Self {
        let mut changes = Self::default();
        for table in &state.tables {
            if table.base.status != TableStatus::Active {
                continue;
            }
            if is_system_table(table) {
                changes.close_tables.push(table.base.table_id);
            } else {
                changes.tables.push(table.frozen_version(ctx));
                changes.table_transitions.push(TableTransition {
                    table_id: table.base.table_id,
                    status: TableStatus::Frozen,
                    function_id: table.base.function_id,
                });
            }
        }
        changes.close_dependencies = state
            .dependencies
            .iter()
            .map(|d| d.base.dependency_id)
            .collect();
        changes.close_triggers = state
            .triggers
            .iter()
            .map(|t| t.base.base.trigger_id)
            .collect();
        changes
    }

    fn diff_tables(
        &mut self,
        state: &FunctionState,
        resolved: &ResolvedSpec,
        function: &FunctionInsertDB,
        ctx: &RequestContext,
    ) -> Result<(), TdError> {
        let existing: HashMap<_, &TableDB> =
            state.tables.iter().map(|t| (&t.base.name, t)).collect();
        let mut kept = HashSet::new();
        for table in &resolved.tables {
            kept.insert(&table.name);
            let desired = desired_table(table, function, ctx)?;
            match existing.get(&table.name) {
                Some(current) if same_table(&current.base, &desired) => {}
                _ => {
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
            if kept.contains(&table.base.name) || table.base.status != TableStatus::Active {
                continue;
            }
            self.tables.push(table.frozen_version(ctx));
            self.table_transitions.push(TableTransition {
                table_id: table.base.table_id,
                status: TableStatus::Frozen,
                function_id: table.base.function_id,
            });
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
            .map(|(_, d)| d.base.dependency_id)
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
                    (
                        *t.base.base.trigger_by_collection_id.deref(),
                        t.base.base.trigger_by_table_id,
                    ),
                    t,
                )
            })
            .collect();
        let mut kept = HashSet::new();
        for trigger in &resolved.triggers {
            let k = (trigger.table.collection_id, trigger.table.table_id);
            let current = existing.get(&k);
            let trigger_id = current.map_or_else(TriggerId::default, |t| t.base.base.trigger_id);
            // A trigger fires only while its source table is produced.
            let status = match trigger.table.status {
                TableStatus::Frozen => TriggerStatus::Frozen,
                TableStatus::Active => TriggerStatus::Active,
            };
            let unchanged = current.is_some_and(|t| {
                t.base.base.status == status
                    && *t.base.base.trigger_by_function_id.deref() == trigger.table.function_id
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
            .map(|(_, t)| t.base.base.trigger_id)
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
        .any(|prefix| table.base.name.starts_with(prefix))
}
