//
// Copyright 2026 Tabsdata Inc.
//

use crate::dxo::project::{ActiveProjectDB, ActiveProjectDBIden};
use crate::types::basic::ProjectId;
use sea_query::{Condition, Expr, ExprTrait, Query, SimpleExpr};
use std::collections::HashSet;
use ta_objects::sql_condition::SqlCondition;
use ta_objects::{Dao, HasColumn, SeaValue};

/// The set of projects a requester may view: every live project ([`Self::All`],
/// from a `project:__all__` view grant) or an explicit set ([`Self::Only`],
/// possibly empty — the requester then sees nothing). `td_authz` derives it
/// from grants; a cross-project listing filters its result set to it. A
/// soft-deleted project is in neither: its roles are dropped with it, and
/// `All` reaches only `projects_active`.
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

/// As a WHERE clause: `All` restricts the DAO's `ProjectId` column to the live
/// projects, `Only(ids)` to the set, an empty set matches nothing.
impl VisibleProjects {
    /// The visibility boundary as a predicate over `project_id`: part of the
    /// query plan, so a tenant-wide statement only ever aggregates the rows
    /// the requester may see.
    pub fn narrows(&self, project_id: impl Into<SimpleExpr>) -> Condition {
        let ids = match self {
            VisibleProjects::All => {
                let live = Query::select()
                    .column(ActiveProjectDBIden::Id)
                    .from(ActiveProjectDB::TABLE)
                    .to_owned();
                return Condition::all().add(project_id.into().in_subquery(live));
            }
            VisibleProjects::Only(ids) => ids,
        };
        if ids.is_empty() {
            return Condition::all().add(Expr::cust("1 = 0"));
        }
        let values: Vec<_> = ids.iter().map(|id| id.as_sea_value()).collect();
        Condition::all().add(project_id.into().is_in(values))
    }
}

impl<D: HasColumn<ProjectId>> SqlCondition<D> for VisibleProjects {
    fn resolve(&self) -> Condition {
        self.narrows(Expr::col(D::COL))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_query::{Alias, PostgresQueryBuilder};

    fn render(visible: &VisibleProjects) -> String {
        Query::select()
            .column(Alias::new("id"))
            .from(Alias::new("plans"))
            .cond_where(visible.narrows(Expr::col(Alias::new("project_id"))))
            .to_string(PostgresQueryBuilder)
    }

    #[test]
    fn test_all_narrows_to_live_projects() {
        assert_eq!(
            render(&VisibleProjects::All),
            r#"SELECT "id" FROM "plans" WHERE "project_id" IN (SELECT "id" FROM "projects_active")"#
        );
        assert!(!SqlCondition::<ActiveProjectDB>::matches_all(
            &VisibleProjects::All
        ));
    }

    #[test]
    fn test_only_narrows_to_the_set() {
        let p = ProjectId::default();
        assert_eq!(
            render(&VisibleProjects::Only(HashSet::from([p]))),
            format!(r#"SELECT "id" FROM "plans" WHERE "project_id" IN ('{p}')"#)
        );
        assert_eq!(
            render(&VisibleProjects::Only(HashSet::new())),
            r#"SELECT "id" FROM "plans" WHERE 1 = 0"#
        );
    }
}
