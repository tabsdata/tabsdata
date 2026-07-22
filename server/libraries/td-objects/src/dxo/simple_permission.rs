//
// Copyright 2026 Tabsdata Inc.
//

//! DXO types for the Simple Security Mode permission facade.
//!
//! These are pure surface types: they carry the flat
//! [`SimplePermissionType`](crate::types::basic::SimplePermissionType) the user
//! sees. The `td-services` `simple_permission` module translates them into RBAC
//! role-membership / `role_permission` operations — there is no
//! `simple_permission` table.

use crate::dxo::crudl::RequestContext;
use crate::rest_urls::{PermissionGrantProjectQuery, PermissionGrantUserQuery};

#[ta_objects::dxo]
mod definitions {
    use crate::dxo::crudl::RequestContext;
    use crate::types::basic::{
        ProjectIdName, ProjectName, SimplePermissionType, UserIdName, UserName,
    };

    /// Grant / revoke request body: grant `permission` to `user`, optionally on
    /// `project`.
    ///
    /// `project` is REQUIRED for the three project permissions
    /// (`view`/`develop`/`manage_project`) and FORBIDDEN for the two system
    /// permissions (`create_projects`/`manage_system`); the service validates
    /// this and rejects the org-wide read/develop wildcards (which have no
    /// Simple-surface name).
    #[ta_objects::Dto]
    pub struct SimplePermissionGrant {
        #[dxo(extractor)]
        pub user: UserName,
        #[dxo(extractor)]
        pub permission: SimplePermissionType,
        #[dxo(extractor)]
        #[serde(default)]
        pub project: Option<ProjectIdName>,
    }

    /// One row of a user's effective flat permission set
    /// (`tdk permission list --user X`). `project` is set for project-scoped
    /// permissions, `None` for system-scoped ones.
    #[ta_objects::Dto]
    #[derive(Eq, PartialEq, Hash)]
    pub struct SimplePermission {
        pub permission: SimplePermissionType,
        pub project: Option<ProjectName>,
    }

    /// One row of "who has access to a project" (`tdk permission list
    /// --project P`), with the source that conferred it: `project (<rolename>)` for per-project role
    /// memberships, `system (manage_system)` for system admins.
    #[ta_objects::Dto]
    #[derive(Eq, PartialEq, Hash)]
    pub struct SimpleProjectAccess {
        pub user: UserName,
        pub permission: SimplePermissionType,
        pub source: String,
    }

    /// Service request for the list-by-user endpoint: the requester context and
    /// the target user.
    #[ta_objects::Dlo]
    pub struct ListByUserRequest {
        #[dxo(extractor)]
        pub context: RequestContext,
        #[dxo(extractor)]
        pub user: UserIdName,
    }

    /// Service request for the list-by-project endpoint: the requester context
    /// and the target project.
    #[ta_objects::Dlo]
    pub struct ListByProjectRequest {
        #[dxo(extractor)]
        pub context: RequestContext,
        #[dxo(extractor)]
        pub project: ProjectIdName,
    }
}

impl ListByUserRequest {
    pub fn new(context: RequestContext, query: PermissionGrantUserQuery) -> Self {
        Self {
            context,
            user: query.user,
        }
    }
}

impl ListByProjectRequest {
    pub fn new(context: RequestContext, query: PermissionGrantProjectQuery) -> Self {
        Self {
            context,
            project: query.project,
        }
    }
}
