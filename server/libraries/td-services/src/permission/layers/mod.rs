//
// Copyright 2025 Tabs Data Inc.
//

use crate::permission::PermissionError;
use std::ops::Deref;
use td_database::dynamic::DynamicConnection;
use td_error::TdError;
use td_objects::dxo::permission::{
    PermissionCreate, PermissionDB, PermissionDBBuilder, PermissionDBWithNames,
};
use td_objects::dxo::project::ProjectDB;
use td_objects::dxo::role::RoleDB;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{EntityId, PermissionEntityType, ProjectName, RoleIdName};
use td_queries::LiveEntry;
use td_tower::default_services::Condition;
use td_tower::extractors::{MutRef, Ref};

pub trait PermissionBuildService {
    fn build_permission_db(
        conn: MutRef<DynamicConnection<'static>>,
        input: Ref<PermissionDBBuilder>,
        permission_create: Ref<PermissionCreate>,
    ) -> impl Future<Output = Result<PermissionDB, TdError>>;
}

impl PermissionBuildService for With<PermissionDBBuilder> {
    async fn build_permission_db(
        mut conn: MutRef<DynamicConnection<'static>>,
        input: Ref<PermissionDBBuilder>,
        permission_create: Ref<PermissionCreate>,
    ) -> Result<PermissionDB, TdError> {
        let permission_type = &permission_create.permission_type;
        let entity_name = &permission_create.entity_name;
        let entity_type = permission_type.on_entity_type();

        let entity_id = if let Some(entity_name) = entity_name {
            let project_name = ProjectName::try_from(entity_name.to_string())?;
            let project = conn.live().fetch_one::<ProjectDB>(&project_name).await?;

            EntityId::try_from(project.id)?
        } else {
            EntityId::all_entities()
        };

        let mut input = input.deref().clone();
        let permission_db = input
            .entity_type(entity_type)
            .entity_id(entity_id)
            .build()?;
        Ok(permission_db)
    }
}

pub async fn is_permission_with_names_on_a_single_project(
    permission: Ref<PermissionDBWithNames>,
) -> Result<Condition, TdError> {
    Ok(Condition(
        permission.permission_type.on_entity_type() == PermissionEntityType::Project
            && permission.entity_id.is_some(),
    ))
}

pub async fn is_permission_on_a_single_project(
    permission: Ref<PermissionDB>,
) -> Result<Condition, TdError> {
    Ok(Condition(
        permission.permission_type.on_entity_type() == PermissionEntityType::Project
            && !permission.entity_id.is_all_entities(),
    ))
}

pub async fn assert_permission_is_not_fixed(
    permission: Ref<PermissionDBWithNames>,
) -> Result<(), TdError> {
    if *permission.fixed {
        Err(PermissionError::PermissionIsFixed)?
    } else {
        Ok(())
    }
}

/// Reject adding a permission to a system-managed role from the user surface.
///
/// System-managed roles: the `sys_admin` role, the `project_creator`
/// role, and the three per-project roles (`prj_admin`/`prj_dev`/`prj_view`)
/// have permission sets fixed by the system — only their *membership* is
/// editable. The role/permission delete
/// services already block removal via the `fixed` flag; this mirrors that on
/// the create path so a `manage_access`/`manage_project` holder cannot grow a
/// system-managed role's permission set.
///
/// Gating is on `kind`, not the row's `fixed` flag: the built-in `default` role
/// is seeded `fixed = true` yet the model mandates its composition be editable (via
/// `manage_roles`); custom roles (`kind = Custom`) are user-defined and freely
/// editable. So `Default` (the built-in default role) and `Custom` (API-created
/// roles) stay editable, while the four system-managed kinds are blocked.
pub async fn assert_role_is_not_fixed(role: Ref<RoleDB>) -> Result<(), TdError> {
    use td_objects::types::basic::RoleKind;
    match role.kind {
        RoleKind::SysAdmin
        | RoleKind::ProjectCreator
        | RoleKind::PrjAdmin
        | RoleKind::PrjDev
        | RoleKind::PrjView => Err(PermissionError::RoleIsFixed(role.name.clone()))?,
        RoleKind::Default | RoleKind::Custom => Ok(()),
    }
}

pub async fn assert_role_in_permission(
    role_id_name: Ref<RoleIdName>,
    permission: Ref<PermissionDBWithNames>,
) -> Result<(), TdError> {
    match role_id_name.deref() {
        RoleIdName::Id(id) => {
            if id != &permission.role_id {
                Err(PermissionError::RolePermissionMismatch)?
            }
        }
        RoleIdName::Name(name) => {
            if name != &permission.role {
                Err(PermissionError::RolePermissionMismatch)?
            }
        }
    }
    Ok(())
}
