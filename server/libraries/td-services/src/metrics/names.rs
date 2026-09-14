//
// Copyright 2026 Tabsdata Inc.
//

//! Display names for what a `/metrics` window mentions.

use std::collections::{HashMap, HashSet};

use td_database::access::SeesProjectRows;
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::collection::ResolvedCollectionDB;
use td_objects::dxo::group::GroupDB;
use td_objects::dxo::metrics_facts::MetricsTableRow;
use td_objects::dxo::project::ProjectDB;
use td_objects::types::basic::{
    CollectionId, CollectionName, GroupId, GroupName, ProjectId, ProjectName,
};
use td_queries::Queries;

#[derive(Default)]
pub(crate) struct Names {
    collections: HashMap<CollectionId, CollectionName>,
    groups: HashMap<GroupId, GroupName>,
    projects: HashMap<ProjectId, ProjectName>,
}

impl Names {
    /// A soft-deleted collection keeps its pre-deletion name.
    pub(crate) async fn of<S: SeesProjectRows>(
        conn: &mut DomainConnection<Apiserver, S>,
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

        let collections: Vec<ResolvedCollectionDB> =
            conn.chunked().fetch_all(&collection_ids, ()).await?;
        let groups: Vec<GroupDB> = conn.chunked().fetch_all(&group_ids, ()).await?;
        let projects: Vec<ProjectDB> = conn.chunked().fetch_all(&project_ids, ()).await?;

        Ok(Self {
            collections: collections
                .into_iter()
                .map(|c| (c.base.id, c.base.name))
                .collect(),
            groups: groups
                .into_iter()
                .map(|g| (g.base.id, g.base.name))
                .collect(),
            projects: projects
                .into_iter()
                .map(|p| (p.base.id, p.base.name))
                .collect(),
        })
    }

    pub(crate) fn collection(&self, id: &CollectionId) -> Option<CollectionName> {
        self.collections.get(id).cloned()
    }

    pub(crate) fn group(&self, id: &GroupId) -> Option<GroupName> {
        self.groups.get(id).cloned()
    }

    pub(crate) fn project(&self, id: &ProjectId) -> Option<ProjectName> {
        self.projects.get(id).cloned()
    }
}
