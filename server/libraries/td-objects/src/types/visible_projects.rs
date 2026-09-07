//
// Copyright 2026 Tabsdata Inc.
//

use crate::types::basic::ProjectId;
use sea_query::{Condition, Expr, ExprTrait, IntoTableRef};
use std::any::TypeId;
use std::collections::HashSet;
use ta_objects::errors::SqlError;
use ta_objects::sql_condition::SqlCondition;
use ta_objects::{DaoCore, SeaValue};

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
/// DAO's `project_id` column to the set, an empty set matches nothing.
impl SqlCondition for VisibleProjects {
    fn resolve<D: DaoCore>(&self) -> Result<Condition, SqlError> {
        let ids = match self {
            VisibleProjects::All => return Ok(Condition::all()),
            VisibleProjects::Only(ids) => ids,
        };
        if ids.is_empty() {
            return Ok(Condition::all().add(Expr::cust("1 = 0")));
        }
        let col = D::col_for_type(TypeId::of::<ProjectId>()).ok_or_else(|| {
            SqlError::TypeNotFound(
                std::any::type_name::<ProjectId>().to_string(),
                Box::new(D::table().into_table_ref()),
            )
        })?;
        let values: Vec<_> = ids.iter().map(|id| id.as_sea_value()).collect();
        Ok(Condition::all().add(Expr::col(col).is_in(values)))
    }

    fn matches_all(&self) -> bool {
        matches!(self, VisibleProjects::All)
    }
}
