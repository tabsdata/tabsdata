//
// Copyright 2026 Tabsdata Inc.
//

//! `ListFilter`s applied to `By::list` — query fragments narrowing a listing.
//!
//! [`VisibleProjectsFilter`] restricts a project-keyed list to the projects the
//! requester may view. The viewable set ([`VisibleProjects`]) is derived from
//! the requester's grants by `td_authz::rbac::Visible::projects` (which places
//! it in the service context); this filter reads it and turns it into the SQL
//! predicate:
//!
//! - [`VisibleProjects::All`] → no restriction (`Condition::all()`);
//! - [`VisibleProjects::Only`] with ids → `project_id IN (..)`;
//! - [`VisibleProjects::Only`] empty → matches nothing.

use sea_query::{Condition, Expr, ExprTrait, IntoTableRef};
use std::any::TypeId;
use td_objects::errors::SqlError;
use td_objects::types::basic::ProjectId;
use td_objects::types::visible_projects::VisibleProjects;
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
