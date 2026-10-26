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
use td_objects::dxo::group::GroupDB;
use td_objects::dxo::metrics_facts::MetricsTableRow;
use td_objects::dxo::project::ProjectDB;
use td_objects::types::basic::{
    CollectionId, CollectionLabel, CollectionName, GroupId, GroupLabel, GroupName, ProjectId,
    ProjectLabel, ProjectName,
};
use td_queries::Queries;

#[derive(Default)]
pub(crate) struct Names {
    collections: HashMap<CollectionId, CollectionName>,
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

        Ok(Self {
            collections: collections
                .into_iter()
                .map(|c| (c.base.collection_id, c.base.name))
                .collect(),
            groups: groups
                .into_iter()
                .map(|g| (g.base.group_id, g.base.name))
                .collect(),
            projects: projects
                .into_iter()
                .map(|p| (p.base.project_id, p.base.name))
                .collect(),
        })
    }

    pub(crate) fn collection(&self, id: &CollectionId) -> Option<CollectionLabel> {
        Some(self.collections.get(id).cloned().map_or_else(
            || CollectionLabel::new(Label::Deleted(*id)),
            CollectionLabel::new,
        ))
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
