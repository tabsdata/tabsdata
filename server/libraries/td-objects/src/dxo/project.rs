//
// Copyright 2026 Tabsdata Inc.
//

//! Project DXOs (Data Exchange Objects) — mirrors the collection DXOs but
//! one rung "above" collections: projects are the tenant boundary for
//! row-level security and are themselves a global registry (not RLS-scoped).
//!
//! A project is an SCD2 timeline of versions. The live projects are the open
//! versions; a delete closes the timeline.

use crate::dxo::crudl::RequestContext;
use crate::dxo::scd2_interval::Scd2Interval;
use crate::dxo::secret::SecretUpsert;
use crate::types::basic::{
    AtTime, BasePath, Description, ProjectId, ProjectIdName, ProjectName, ProjectVersionId,
    Scd2ValidFrom, UserId, UserLabel,
};
use ta_objects::FromDao;
use td_error::TdError;

#[ta_objects::Dao]
#[dao(table = "projects", ops(insert), scd2)]
pub struct ProjectInsertDB {
    #[dao(pk)]
    pub id: ProjectVersionId,
    #[dao(partition_by)]
    pub project_id: ProjectId,
    pub name: ProjectName,
    pub description: Description,
    // Fixed for the project's life, from here down. Logical storage prefix,
    // mapped from `ProjectCreate::base_path` (defaults `/`).
    pub base_path: BasePath,
    pub defined_by_id: UserId,
}

#[ta_objects::Dao]
#[dao(
    table = "projects",
    ops(select, close),
    scd2,
    unique_at(ProjectName),
    unique_at(ProjectIdName)
)]
pub struct ProjectDB {
    #[dao(flatten)]
    pub base: ProjectInsertDB,
    #[dao(flatten)]
    pub interval: Scd2Interval,
}

impl ProjectDB {
    /// The version an update opens: this one's values with the update's
    /// changes on top.
    pub fn updated(&self, update: &ProjectUpdate, ctx: &RequestContext) -> ProjectInsertDB {
        ProjectInsertDB {
            id: ProjectVersionId::default(),
            name: update
                .name
                .clone()
                .unwrap_or_else(|| self.base.name.clone()),
            description: update
                .description
                .clone()
                .unwrap_or_else(|| self.base.description.clone()),
            defined_by_id: ctx.user_id,
            ..self.base.clone()
        }
    }
}

#[ta_objects::Dao]
#[dao(
    view = "projects__with_names",
    scd2,
    unique_at(ProjectName),
    unique_at(ProjectIdName)
)]
pub struct ProjectDBWithNames {
    #[dao(flatten)]
    pub base: ProjectDB,
    pub defined_by: UserLabel,
}

#[ta_objects::Dto]
pub struct ProjectCreate {
    pub name: ProjectName,
    pub description: Description,
    // Logical storage prefix for the project's artifacts. Defaults to `/`.
    // Create-only for now: not mutable via `ProjectUpdate` (project "move" deferred).
    #[serde(default)]
    pub base_path: BasePath,
    #[serde(default)]
    pub secrets: Vec<SecretUpsert>,
}

impl ProjectCreate {
    pub fn to_dao(&self, ctx: &RequestContext) -> ProjectInsertDB {
        ProjectInsertDB {
            id: ProjectVersionId::default(),
            project_id: ProjectId::default(),
            name: self.name.clone(),
            description: self.description.clone(),
            base_path: self.base_path.clone(),
            defined_by_id: ctx.user_id,
        }
    }
}

#[ta_objects::Dto]
pub struct ProjectUpdate {
    pub name: Option<ProjectName>,
    pub description: Option<Description>,
    #[serde(default)]
    pub secrets: Option<Vec<SecretUpsert>>,
}

#[ta_objects::Dto]
#[dto(list(on = ProjectDBWithNames))]
pub struct ProjectRead {
    #[dto(list(pagination_by = "+"))]
    pub id: ProjectVersionId,
    #[dto(list(filter))]
    pub project_id: ProjectId,
    #[dto(list(filter, filter_like, order_by))]
    pub name: ProjectName,
    #[dto(list(filter, filter_like, order_by))]
    pub description: Description,
    #[dto(list(filter, filter_like, order_by))]
    pub base_path: BasePath,
    #[dto(list(filter, order_by, col = Scd2ValidFrom))]
    pub defined_on: AtTime,
    pub defined_by_id: UserId,
    #[dto(list(filter, filter_like, order_by))]
    pub defined_by: UserLabel,
}

impl FromDao for ProjectRead {
    type Dao = ProjectDBWithNames;

    fn from_dao(dao: ProjectDBWithNames) -> Result<Self, TdError> {
        let ProjectDB { base, interval } = dao.base;
        Ok(Self {
            id: base.id,
            project_id: base.project_id,
            name: base.name,
            description: base.description,
            base_path: base.base_path,
            defined_on: interval.valid_from.into(),
            defined_by_id: base.defined_by_id,
            defined_by: dao.defined_by,
        })
    }
}

#[ta_objects::UrlParam]
pub struct ProjectParam {
    pub project: ProjectIdName,
}
