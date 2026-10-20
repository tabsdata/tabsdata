//
// Copyright 2026 Tabsdata Inc.
//

use crate::graph::{TdGraph, TdGraphBuilder};
use std::collections::{HashMap, HashSet};
use td_database::access::{AccessMode, InProject};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::dependency::DependencyDBWithNames;
use td_objects::dxo::function::FunctionDBWithNames;
use td_objects::dxo::table::TableDBWithNames;
use td_objects::dxo::topology::ProjectTopologyDB;
use td_objects::dxo::trigger::TriggerDBWithNames;
use td_objects::types::basic::{FunctionId, FunctionStatus, ProjectId, TableId};
use td_queries::Queries;

mod plan;
mod walk;
#[cfg(test)]
mod walk_tests;

/// A project's whole catalog at one topology version, indexed for walks.
pub struct ProjectTopology {
    pub version: ProjectTopologyDB,
    functions: HashMap<FunctionId, FunctionDBWithNames>,
    tables: HashMap<TableId, TableDBWithNames>,
    table_order: Vec<TableId>,
    triggers: Vec<TriggerDBWithNames>,
    dependencies: Vec<DependencyDBWithNames>,
    triggers_from: HashMap<FunctionId, Vec<usize>>,
    triggers_into: HashMap<FunctionId, Vec<usize>>,
    dependencies_from: HashMap<FunctionId, Vec<usize>>,
    dependencies_into: HashMap<FunctionId, Vec<usize>>,
}

/// The functions a walk goes through.
#[derive(Clone, Copy, Debug)]
pub enum FunctionState<'a> {
    Any,
    /// Active and enabled.
    Executable,
    /// The ones an execution ran.
    Ran(&'a HashSet<FunctionId>),
}

impl FunctionState<'_> {
    pub fn matches(&self, function: &FunctionDBWithNames) -> bool {
        let function = &function.base.base;
        match self {
            FunctionState::Any => true,
            FunctionState::Executable => {
                function.status == FunctionStatus::Active && *function.enabled
            }
            FunctionState::Ran(functions) => functions.contains(&function.function_id),
        }
    }
}

impl ProjectTopology {
    /// The project's open version.
    pub async fn current<M: AccessMode>(
        conn: &mut DomainConnection<Apiserver, InProject, M>,
        project: &ProjectId,
    ) -> Result<Self, TdError> {
        let version: ProjectTopologyDB = conn.scd2().fetch_one(project).await?;
        Self::of(conn, version).await
    }

    pub async fn of<M: AccessMode>(
        conn: &mut DomainConnection<Apiserver, InProject, M>,
        version: ProjectTopologyDB,
    ) -> Result<Self, TdError> {
        let at = &version.interval.valid_from;
        let functions: Vec<FunctionDBWithNames> = conn.scd2().as_of(at).fetch_all(()).await?;
        let mut tables: Vec<TableDBWithNames> = conn.scd2().as_of(at).fetch_all(()).await?;
        let mut dependencies: Vec<DependencyDBWithNames> =
            conn.scd2().as_of(at).fetch_all(()).await?;
        let mut triggers: Vec<TriggerDBWithNames> = conn.scd2().as_of(at).fetch_all(()).await?;
        // Graphs insert nodes in this order, and `dot` numbers them by it.
        tables.sort_by_key(|t| t.base.base.table_id);
        dependencies.sort_by_key(|d| d.base.base.id);
        triggers.sort_by_key(|t| t.base.base.id);

        Ok(Self {
            triggers_from: index(triggers.iter().map(|t| *t.base.base.trigger_by_function_id)),
            triggers_into: index(triggers.iter().map(|t| t.base.base.function_id)),
            dependencies_from: index(dependencies.iter().map(|d| *d.base.base.table_function_id)),
            dependencies_into: index(dependencies.iter().map(|d| d.base.base.function_id)),
            table_order: tables.iter().map(|t| t.base.base.table_id).collect(),
            tables: tables
                .into_iter()
                .map(|t| (t.base.base.table_id, t))
                .collect(),
            functions: functions
                .into_iter()
                .map(|f| (f.base.base.function_id, f))
                .collect(),
            triggers,
            dependencies,
            version,
        })
    }

    /// The whole project graph.
    pub fn graph(&self) -> Result<TdGraph, TdError> {
        // A frozen table outlives the function that wrote it: no output edge.
        let outputs = self
            .ordered_tables()
            .filter(|t| self.functions.contains_key(&t.base.base.function_id));
        Ok(TdGraphBuilder::new(&self.tables, &self.functions)
            .with_triggers(&self.triggers)?
            .with_dependencies(&self.dependencies)?
            .with_outputs(outputs)?
            .build())
    }

    fn ordered_tables(&self) -> impl Iterator<Item = &TableDBWithNames> {
        self.table_order.iter().map(|id| &self.tables[id])
    }
}

/// Edge positions by the function at one end.
fn index(ends: impl Iterator<Item = FunctionId>) -> HashMap<FunctionId, Vec<usize>> {
    let mut index: HashMap<FunctionId, Vec<usize>> = HashMap::new();
    for (i, end) in ends.enumerate() {
        index.entry(end).or_default().push(i);
    }
    index
}
