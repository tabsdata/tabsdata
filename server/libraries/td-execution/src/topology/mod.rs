//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::HashMap;
use td_database::access::{AccessMode, InProject};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::and;
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::group::GroupDB;
use td_objects::dxo::topology::{
    ProjectTopologyDB, TopologyDependency, TopologyFunction, TopologyTable, TopologyTag,
    TopologyTrigger,
};
use td_objects::types::basic::{
    CollectionId, CollectionLabel, FunctionId, FunctionStatus, GroupId, GroupLabel, ProjectId,
    TableId, TagEntityType, TagKind, TopologyTagKey,
};
use td_objects::types::label::Label;
use td_queries::Queries;

mod graph;
mod plan;
mod walk;
#[cfg(test)]
mod walk_tests;

/// A project's whole catalog at one topology version, indexed for walks.
pub struct ProjectTopology {
    pub version: ProjectTopologyDB,
    collections: HashMap<CollectionId, CollectionDB>,
    groups: HashMap<GroupId, GroupDB>,
    functions: HashMap<FunctionId, TopologyFunction>,
    parents: HashMap<FunctionId, FunctionId>,
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
        let collections: Vec<CollectionDB> = conn.scd2().last().fetch_all(()).await?;
        let groups: Vec<GroupDB> = conn.scd2().last().fetch_all(()).await?;
        let functions: Vec<TopologyFunction> = conn.scd2().as_of(at).fetch_all(()).await?;
        let tables: Vec<TopologyTable> = conn.scd2().as_of(at).fetch_all(()).await?;
        let triggers: Vec<TopologyTrigger> = conn.scd2().as_of(at).fetch_all(()).await?;
        let dependencies: Vec<TopologyDependency> = conn.scd2().as_of(at).fetch_all(()).await?;
        let parents: Vec<TopologyTag> = conn
            .scd2()
            .as_of(at)
            .fetch_all(and!(
                TagKind::Topology,
                TagEntityType::Function,
                TopologyTagKey::Parent
            ))
            .await?;
        Ok(Self::new(
            version,
            collections,
            groups,
            functions,
            parents
                .iter()
                .map(|t| Ok((FunctionId::try_from(&t.entity_id)?, t.value.id()?)))
                .collect::<Result<_, TdError>>()?,
            tables,
            triggers,
            dependencies,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        version: ProjectTopologyDB,
        collections: Vec<CollectionDB>,
        groups: Vec<GroupDB>,
        functions: Vec<TopologyFunction>,
        parents: HashMap<FunctionId, FunctionId>,
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
            collections: collections
                .into_iter()
                .map(|c| (c.collection_id, c))
                .collect(),
            groups: groups.into_iter().map(|g| (g.group_id, g)).collect(),
            tables: tables.into_iter().map(|t| (t.table_id, t)).collect(),
            functions: functions.into_iter().map(|f| (f.function_id, f)).collect(),
            parents,
            triggers,
            dependencies,
            version,
        }
    }

    fn collection_label(&self, collection: &CollectionId) -> CollectionLabel {
        match self.collections.get(collection) {
            Some(c) if c.interval.valid_to.is_infinity() => CollectionLabel::new(c.name.clone()),
            _ => CollectionLabel::new(Label::Deleted(*collection)),
        }
    }

    fn group_label(&self, group: &GroupId) -> GroupLabel {
        match self.groups.get(group) {
            Some(g) if g.interval.valid_to.is_infinity() => GroupLabel::new(g.name.clone()),
            _ => GroupLabel::new(Label::Deleted(*group)),
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
