//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::HashMap;
use td_database::access::{AccessMode, InProject};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::collection::ResolvedCollectionDB;
use td_objects::dxo::topology::{
    ProjectTopologyDB, TopologyDependency, TopologyFunction, TopologyTable, TopologyTrigger,
};
use td_objects::types::basic::{CollectionId, FunctionId, FunctionStatus, ProjectId, TableId};
use td_queries::Queries;

mod graph;
mod plan;
mod walk;
#[cfg(test)]
mod walk_tests;

/// A project's whole catalog at one topology version, indexed for walks.
pub struct ProjectTopology {
    pub version: ProjectTopologyDB,
    collections: HashMap<CollectionId, ResolvedCollectionDB>,
    functions: HashMap<FunctionId, TopologyFunction>,
    tables: HashMap<TableId, TopologyTable>,
    table_order: Vec<TableId>,
    triggers: Vec<TopologyTrigger>,
    dependencies: Vec<TopologyDependency>,
    triggers_from: HashMap<FunctionId, Vec<usize>>,
    triggers_into: HashMap<FunctionId, Vec<usize>>,
    dependencies_from: HashMap<FunctionId, Vec<usize>>,
    dependencies_into: HashMap<FunctionId, Vec<usize>>,
}

/// The functions a walk goes through.
#[derive(Clone, Copy, Debug)]
pub enum FunctionState {
    Any,
    /// Active and enabled.
    Executable,
}

impl FunctionState {
    pub(crate) fn matches(&self, function: &TopologyFunction) -> bool {
        match self {
            FunctionState::Any => true,
            FunctionState::Executable => {
                function.status == FunctionStatus::Active && *function.enabled
            }
        }
    }
}

impl ProjectTopology {
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
        let collections: Vec<ResolvedCollectionDB> = conn.fetch_all(()).await?;
        let functions: Vec<TopologyFunction> = conn.scd2().as_of(at).fetch_all(()).await?;
        let tables: Vec<TopologyTable> = conn.scd2().as_of(at).fetch_all(()).await?;
        let triggers: Vec<TopologyTrigger> = conn.scd2().as_of(at).fetch_all(()).await?;
        let dependencies: Vec<TopologyDependency> = conn.scd2().as_of(at).fetch_all(()).await?;
        Ok(Self::new(
            version,
            collections,
            functions,
            tables,
            triggers,
            dependencies,
        ))
    }

    pub(crate) fn new(
        version: ProjectTopologyDB,
        collections: Vec<ResolvedCollectionDB>,
        functions: Vec<TopologyFunction>,
        mut tables: Vec<TopologyTable>,
        mut triggers: Vec<TopologyTrigger>,
        mut dependencies: Vec<TopologyDependency>,
    ) -> Self {
        // Graphs insert nodes in this order, and `dot` numbers them by it.
        tables.sort_by_key(|t| t.table_id);
        dependencies.sort_by_key(|d| d.id);
        triggers.sort_by_key(|t| t.id);

        Self {
            triggers_from: index(triggers.iter().map(|t| *t.trigger_by_function_id)),
            triggers_into: index(triggers.iter().map(|t| t.function_id)),
            dependencies_from: index(dependencies.iter().map(|d| *d.table_function_id)),
            dependencies_into: index(dependencies.iter().map(|d| d.function_id)),
            table_order: tables.iter().map(|t| t.table_id).collect(),
            collections: collections.into_iter().map(|c| (c.base.id, c)).collect(),
            tables: tables.into_iter().map(|t| (t.table_id, t)).collect(),
            functions: functions.into_iter().map(|f| (f.function_id, f)).collect(),
            triggers,
            dependencies,
            version,
        }
    }

    fn ordered_tables(&self) -> impl Iterator<Item = &TopologyTable> {
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
