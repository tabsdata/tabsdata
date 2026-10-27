//
// Copyright 2026 Tabsdata Inc.
//

//! Display names for what a `/metrics` window mentions.

use std::collections::{HashMap, HashSet};
use td_objects::types::label::Label;

use td_database::access::{AccessMode, SeesProjectRows};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::connection::ConnectionDB;
use td_objects::dxo::group::GroupDB;
use td_objects::dxo::metrics_facts::MetricsTableRow;
use td_objects::dxo::project::ProjectDB;
use td_objects::types::basic::{
    CollectionId, CollectionLabel, CollectionName, ConnectionId, ConnectionKind, GroupId,
    GroupLabel, GroupName, ProjectId, ProjectLabel, ProjectName,
};
use td_queries::Queries;

#[derive(Default)]
pub(crate) struct Names {
    collections: HashMap<CollectionId, CollectionName>,
    connections: HashMap<CollectionId, ConnectionKind>,
    groups: HashMap<GroupId, GroupName>,
    projects: HashMap<ProjectId, ProjectName>,
}

impl Names {
    /// A deleted collection, group or project is labelled by its id.
    pub(crate) async fn of<S: SeesProjectRows, M: AccessMode>(
        conn: &mut DomainConnection<Apiserver, S, M>,
        rows: &[MetricsTableRow],
    ) -> Result<Self, TdError> {
        if rows.is_empty() {
            return Ok(Self::default());
        }
        let collection_ids: Vec<CollectionId> = rows
            .iter()
            .flat_map(|r| [Some(r.collection_id), r.from_collection_id])
            .flatten()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        let group_ids: Vec<GroupId> = rows
            .iter()
            .flat_map(|r| [Some(r.group_id), r.from_group_id])
            .flatten()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        let project_ids: Vec<ProjectId> = rows
            .iter()
            .map(|r| r.project_id)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        let collections: Vec<CollectionDB> =
            conn.scd2().chunked().fetch_all(&collection_ids, ()).await?;
        let groups: Vec<GroupDB> = conn.scd2().chunked().fetch_all(&group_ids, ()).await?;
        let projects: Vec<ProjectDB> = conn.scd2().chunked().fetch_all(&project_ids, ()).await?;

        let connection_of: HashMap<ConnectionId, CollectionId> = collections
            .iter()
            .filter_map(|c| c.connection_id.map(|id| (id, c.collection_id)))
            .collect();
        let connection_ids: Vec<ConnectionId> = connection_of.keys().copied().collect();
        let connections: Vec<ConnectionDB> = conn.chunked().fetch_all(&connection_ids, ()).await?;

        Ok(Self {
            connections: connections
                .iter()
                .filter_map(|c| connection_of.get(&c.id).map(|id| (*id, c.kind.clone())))
                .collect(),
            collections: collections
                .iter()
                .map(|c| (c.collection_id, c.name.clone()))
                .collect(),
            groups: groups
                .iter()
                .map(|g| (g.group_id, g.name.clone()))
                .collect(),
            projects: projects
                .iter()
                .map(|p| (p.project_id, p.name.clone()))
                .collect(),
        })
    }

    pub(crate) fn collection(&self, id: &CollectionId) -> Option<CollectionLabel> {
        Some(self.collections.get(id).cloned().map_or_else(
            || CollectionLabel::new(Label::Deleted(*id)),
            CollectionLabel::new,
        ))
    }

    pub(crate) fn connection(&self, id: &CollectionId) -> Option<ConnectionKind> {
        self.connections.get(id).cloned()
    }

    pub(crate) fn group(&self, id: &GroupId) -> Option<GroupLabel> {
        Some(
            self.groups
                .get(id)
                .cloned()
                .map_or_else(|| GroupLabel::new(Label::Deleted(*id)), GroupLabel::new),
        )
    }

    pub(crate) fn project(&self, id: &ProjectId) -> Option<ProjectLabel> {
        Some(
            self.projects
                .get(id)
                .cloned()
                .map_or_else(|| ProjectLabel::new(Label::Deleted(*id)), ProjectLabel::new),
        )
    }
}
