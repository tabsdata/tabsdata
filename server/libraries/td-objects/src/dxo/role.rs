//
// Copyright 2025 Tabsdata Inc.
//

use crate::dxo::crudl::RequestContext;
use crate::types::basic::{
    AtTime, Description, Fixed, ProjectId, RoleId, RoleIdName, RoleKind, RoleName, UserId, UserName,
};
use ta_objects::FromDao;
use td_error::TdError;

#[ta_objects::Dao]
#[dao(sql_table = "roles")]
pub struct RoleDB {
    pub id: RoleId,
    pub name: RoleName,
    pub description: Description,
    pub created_on: AtTime,
    pub created_by_id: UserId,
    pub modified_on: AtTime,
    pub modified_by_id: UserId,
    // The role's kind. `default` for API-created custom roles; the
    // built-in/per-project kinds are set by the system at seed/mint time.
    pub kind: RoleKind,
    // NULL for system roles (sys_admin/default), set for per-project roles.
    pub project_id: Option<ProjectId>,
    pub fixed: Fixed,
}

#[ta_objects::Dao]
#[dao(sql_table = "roles__with_names")]
pub struct RoleDBWithNames {
    #[dao(flatten)]
    pub base: RoleDB,
    pub created_by: UserName,
    pub modified_by: UserName,
}

#[ta_objects::Dto]
pub struct RoleCreate {
    pub name: RoleName,
    #[serde(default)]
    pub description: Description,
}

impl RoleCreate {
    pub fn to_dao(self, ctx: &RequestContext) -> RoleDB {
        RoleDB {
            id: RoleId::default(),
            name: self.name,
            description: self.description,
            created_on: ctx.time,
            created_by_id: ctx.user_id,
            modified_on: ctx.time,
            modified_by_id: ctx.user_id,
            // API-created roles are tagged `Custom` (seeded roles are `Default`),
            // so the RBAC->Simple representability check can detect them
            // (condition 1).
            kind: RoleKind::Custom,
            project_id: None,
            fixed: Fixed::FALSE,
        }
    }
}

#[ta_objects::Dao]
#[dao(update, sql_table = "roles")]
pub struct RoleDBUpdate {
    pub name: Option<RoleName>,
    pub description: Option<Description>,
    pub modified_on: AtTime,
    pub modified_by_id: UserId,
}

#[ta_objects::Dto]
pub struct RoleUpdate {
    pub name: Option<RoleName>,
    pub description: Option<Description>,
}

#[ta_objects::Dto]
#[dto(list(on = RoleDBWithNames))]
pub struct Role {
    #[dto(list(pagination_by = "+"))]
    pub id: RoleId,
    #[dto(list(filter, filter_like, order_by))]
    pub name: RoleName,
    #[dto(list(filter, filter_like, order_by))]
    pub description: Description,
    pub created_on: AtTime,
    pub created_by_id: UserId,
    pub modified_on: AtTime,
    pub modified_by_id: UserId,
    pub kind: RoleKind,
    pub project_id: Option<ProjectId>,
    pub fixed: Fixed,
    pub created_by: UserName,
    pub modified_by: UserName,
}

impl FromDao for Role {
    type Dao = RoleDBWithNames;

    fn from_dao(dao: RoleDBWithNames) -> Result<Self, TdError> {
        Ok(Self {
            id: dao.base.id,
            name: dao.base.name,
            description: dao.base.description,
            created_on: dao.base.created_on,
            created_by_id: dao.base.created_by_id,
            modified_on: dao.base.modified_on,
            modified_by_id: dao.base.modified_by_id,
            kind: dao.base.kind,
            project_id: dao.base.project_id,
            fixed: dao.base.fixed,
            created_by: dao.created_by,
            modified_by: dao.modified_by,
        })
    }
}

#[ta_objects::UrlParam]
pub struct RoleParam {
    pub role: RoleIdName,
}
