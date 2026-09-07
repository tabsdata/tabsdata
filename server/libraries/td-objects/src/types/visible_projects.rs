//
// Copyright 2026 Tabsdata Inc.
//

use crate::types::basic::ProjectId;
use sea_query::{Condition, Expr, ExprTrait};
use std::collections::HashSet;
use ta_objects::sql_condition::SqlCondition;
use ta_objects::{HasColumn, SeaValue};

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

/// As a WHERE clause: `All` matches everything, `Only(ids)` restricts the
/// DAO's `ProjectId` column to the set, an empty set matches nothing.
impl<D: HasColumn<ProjectId>> SqlCondition<D> for VisibleProjects {
    fn resolve(&self) -> Condition {
        let ids = match self {
            VisibleProjects::All => return Condition::all(),
            VisibleProjects::Only(ids) => ids,
        };
        if ids.is_empty() {
            return Condition::all().add(Expr::cust("1 = 0"));
        }
        let values: Vec<_> = ids.iter().map(|id| id.as_sea_value()).collect();
        Condition::all().add(Expr::col(D::COL).is_in(values))
    }

    fn matches_all(&self) -> bool {
        matches!(self, VisibleProjects::All)
    }
}
