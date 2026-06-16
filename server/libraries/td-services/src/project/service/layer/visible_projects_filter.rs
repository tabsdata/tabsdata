//
// Copyright 2026 Tabs Data Inc.
//

//! `ListFilter` that restricts a project-keyed list to the projects the
//! requester may view.
//!
//! Projects are silos: a user sees a project only if they hold a project
//! permission on it (or `project:__all__`, i.e. a sys_admin). The viewable set
//! is computed from the requester's grants by `td_authz::rbac::Visible::projects`
//! (which places a [`VisibleProjects`] in the service context); this filter
//! reads it and turns it into the SQL predicate:
//!
//! - [`VisibleProjects::All`] → no restriction (`Condition::all()`);
//! - [`VisibleProjects::Only`] with ids → `id IN (..)`;
//! - [`VisibleProjects::Only`] empty → matches nothing.
//!
//! Used both for listing the projects visible to a user and (via the same
//! [`VisibleProjects`]) for narrowing the global metrics aggregation.

use sea_query::{Condition, Expr, ExprTrait, IntoTableRef};
use std::any::TypeId;
use td_authz::rbac::VisibleProjects;
use td_objects::errors::SqlError;
use td_objects::types::basic::ProjectId;
use td_objects::{DaoCore, ListFilter, ListFilterExtract, SeaValue};
use td_tower::error::FromHandlerError;
use td_tower::extractors::{FromHandler, Ref};
use td_tower::handler::Handler;

/// See module docs.
#[derive(Clone)]
pub struct VisibleProjectsFilter(VisibleProjects);

impl ListFilter for VisibleProjectsFilter {
    fn condition<D: DaoCore>(&self) -> Result<Condition, SqlError> {
        let ids = match &self.0 {
            VisibleProjects::All => return Ok(Condition::all()),
            VisibleProjects::Only(ids) => ids,
        };
        if ids.is_empty() {
            // Viewable set is empty → no project is visible.
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
}

impl ListFilterExtract for VisibleProjectsFilter {
    fn extract(handler: &mut Handler) -> Result<Self, FromHandlerError> {
        // `VisibleProjects` is placed in the context by `Visible::projects`.
        let visible = Ref::<VisibleProjects>::from_handler(handler)?;
        Ok(VisibleProjectsFilter((*visible).clone()))
    }

    #[cfg(feature = "test_tower_metadata")]
    fn metadata_used_types() -> Vec<String> {
        vec![td_tower::metadata::type_of::<VisibleProjects>()]
    }
}
