//
// Copyright 2026 Tabsdata Inc.
//

use crate::types::basic::ProjectId;
use std::collections::HashSet;

/// The set of projects a requester may view: every project ([`Self::All`], from
/// a `project:__all__` view grant) or an explicit set ([`Self::Only`], possibly
/// empty — the requester then sees nothing). `td_authz` derives it from grants;
/// a cross-project listing filters its result set to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VisibleProjects {
    All,
    Only(HashSet<ProjectId>),
}

impl VisibleProjects {
    /// Whether `project` is viewable.
    pub fn contains(&self, project: &ProjectId) -> bool {
        match self {
            VisibleProjects::All => true,
            VisibleProjects::Only(ids) => ids.contains(project),
        }
    }
}
