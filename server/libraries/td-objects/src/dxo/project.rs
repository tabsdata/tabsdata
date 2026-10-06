//
// Copyright 2026 Tabsdata Inc.
//

//! Project DXOs (Data Exchange Objects) — mirrors the collection DXOs but
//! one rung "above" collections: projects are the tenant boundary for
//! row-level security and are themselves a global registry (not RLS-scoped).

use crate::dxo::crudl::RequestContext;
use crate::types::basic::{
    AtTime, BasePath, Created, Description, Modified, ProjectId, ProjectIdName, ProjectName,
    ProjectWhenDeleted, UserId, UserLabel, WhenDeleted,
};
use ta_objects::FromDao;
use td_common::id::Id;
use td_error::TdError;

#[ta_objects::Dao]
#[dao(table = "projects", ops(insert))]
pub struct ProjectInsertDB {
    #[dao(pk)]
    pub id: ProjectId,
    pub name: ProjectName,
    pub description: Description,
    // Logical storage prefix, mapped from `ProjectCreate::base_path` (defaults `/`).
    pub base_path: BasePath,
    pub created_on: Created<AtTime>,
    pub created_by_id: Created<UserId>,
    pub modified_on: Modified<AtTime>,
    pub modified_by_id: Modified<UserId>,
    pub name_when_deleted: Option<WhenDeleted<ProjectName>>,
}

#[ta_objects::Dao]
#[dao(view = "projects_active", unique(ProjectName))]
pub struct ActiveProjectDB {
    #[dao(pk)]
    pub id: ProjectId,
    pub name: ProjectName,
    pub description: Description,
    pub base_path: BasePath,
    pub created_on: Created<AtTime>,
    pub created_by_id: Created<UserId>,
    pub modified_on: Modified<AtTime>,
    pub modified_by_id: Modified<UserId>,
}

impl ActiveProjectDB {
    /// The soft-delete update: the row is renamed to `deleted_<id>`,
    /// freeing the name for reuse.
    pub fn to_delete_dao(&self, ctx: &RequestContext) -> ProjectDeleteDB {
        ProjectDeleteDB {
            name: generate_deleted_name(),
            modified_on: ctx.time.into(),
            modified_by_id: ctx.user_id.into(),
            name_when_deleted: self.name.clone().into(),
        }
    }
}

// Canonical read/join surface: the `projects_resolved` view — every row, real
// name resolved. Same columns as `ActiveProjectDB` plus the `deleted` flag.
// The name is not unique here: the view resolves the freed name back, so a
// delete/recreate cycle leaves two rows under it. Only the id addresses a row.
#[ta_objects::Dao]
#[dao(view = "projects_resolved")]
pub struct ProjectDB {
    #[dao(flatten)]
    pub base: ActiveProjectDB,
    pub deleted: ProjectWhenDeleted,
}

#[ta_objects::Dao]
#[dao(table = "projects", ops(update(ProjectInsertDB)))]
pub struct ProjectDeleteDB {
    pub name: ProjectName,

    pub modified_on: Modified<AtTime>,
    pub modified_by_id: Modified<UserId>,

    pub name_when_deleted: WhenDeleted<ProjectName>,
}

pub fn generate_deleted_name() -> ProjectName {
    ProjectName::try_from(format!("deleted_{}", Id::default())).unwrap()
}

#[ta_objects::Dao]
#[dao(view = "projects__with_names", unique(ProjectName))]
pub struct ActiveProjectDBWithNames {
    #[dao(flatten)]
    pub base: ActiveProjectDB,
    pub created_by: Created<UserLabel>,
    pub modified_by: Modified<UserLabel>,
}

#[ta_objects::Dto]
pub struct ProjectCreate {
    pub name: ProjectName,
    pub description: Description,
    // Logical storage prefix for the project's artifacts. Defaults to `/`.
    // Create-only for now: not mutable via `ProjectUpdate` (project "move" deferred).
    #[serde(default)]
    pub base_path: BasePath,
}

impl ProjectCreate {
    pub fn to_dao(self, ctx: &RequestContext) -> ProjectInsertDB {
        ProjectInsertDB {
            id: ProjectId::default(),
            name: self.name,
            description: self.description,
            base_path: self.base_path,
            created_on: ctx.time.into(),
            created_by_id: ctx.user_id.into(),
            modified_on: ctx.time.into(),
            modified_by_id: ctx.user_id.into(),
            name_when_deleted: None,
        }
    }
}

#[ta_objects::Dto]
pub struct ProjectUpdate {
    pub name: Option<ProjectName>,
    pub description: Option<Description>,
}

#[ta_objects::Dao]
#[dao(table = "projects", ops(update(ProjectInsertDB)))]
pub struct ProjectUpdateDB {
    pub name: ProjectName,
    pub description: Description,
    pub modified_on: Modified<AtTime>,
    pub modified_by_id: Modified<UserId>,
}

#[ta_objects::Dto]
#[dto(list(on = ActiveProjectDBWithNames))]
pub struct ProjectRead {
    #[dto(list(pagination_by = "+"))]
    pub id: ProjectId,
    #[dto(list(filter, filter_like, order_by))]
    pub name: ProjectName,
    #[dto(list(filter, filter_like, order_by))]
    pub description: Description,
    #[dto(list(filter, filter_like, order_by))]
    pub base_path: BasePath,
    pub created_on: Created<AtTime>,
    pub created_by_id: Created<UserId>,
    pub modified_on: Modified<AtTime>,
    pub modified_by_id: Modified<UserId>,
    pub created_by: Created<UserLabel>,
    pub modified_by: Modified<UserLabel>,
}

impl FromDao for ProjectRead {
    type Dao = ActiveProjectDBWithNames;

    fn from_dao(dao: ActiveProjectDBWithNames) -> Result<Self, TdError> {
        Ok(Self {
            id: dao.base.id,
            name: dao.base.name,
            description: dao.base.description,
            base_path: dao.base.base_path,
            created_on: dao.base.created_on,
            created_by_id: dao.base.created_by_id,
            modified_on: dao.base.modified_on,
            modified_by_id: dao.base.modified_by_id,
            created_by: dao.created_by,
            modified_by: dao.modified_by,
        })
    }
}

impl From<ProjectInsertDB> for ActiveProjectDB {
    fn from(src: ProjectInsertDB) -> Self {
        ActiveProjectDB {
            id: src.id,
            name: src.name,
            description: src.description,
            base_path: src.base_path,
            created_on: src.created_on,
            created_by_id: src.created_by_id,
            modified_on: src.modified_on,
            modified_by_id: src.modified_by_id,
        }
    }
}
impl From<ActiveProjectDB> for ProjectUpdateDB {
    fn from(src: ActiveProjectDB) -> Self {
        ProjectUpdateDB {
            name: src.name,
            description: src.description,
            modified_on: src.modified_on,
            modified_by_id: src.modified_by_id,
        }
    }
}

#[ta_objects::UrlParam]
pub struct ProjectParam {
    pub project: ProjectIdName,
}
