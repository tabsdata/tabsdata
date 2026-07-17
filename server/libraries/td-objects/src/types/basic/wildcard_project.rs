//
// Copyright 2026 Tabsdata Inc.
//

use crate::types::basic::ProjectId;
use td_tower::project_scope::WildcardScope;

/// URL marker in the `{project}` path segment selecting every project the
/// requester may view rather than one named project. Reserved implicitly: the
/// project-name grammar rejects it.
pub const WILDCARD_PROJECT_MARKER: &str = "*";

/// The project a wildcard-capable request targets: a single named project, or
/// the wildcard ([`WILDCARD_PROJECT_MARKER`]) spanning every project the
/// requester may view (the service filters to the viewable set).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WildcardProject {
    Project(ProjectId),
    Wildcard,
}

impl From<ProjectId> for WildcardProject {
    fn from(id: ProjectId) -> Self {
        WildcardProject::Project(id)
    }
}

impl WildcardScope for WildcardProject {
    fn project_id(&self) -> Option<String> {
        match self {
            WildcardProject::Project(p) => Some(p.to_string()),
            WildcardProject::Wildcard => None,
        }
    }
}
