//
// Copyright 2025 Tabs Data Inc.
//

use crate::permission::PermissionError;
use std::ops::Deref;
use td_database::dynamic::DynamicConnection;
use td_error::TdError;
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::permission::{
    PermissionCreate, PermissionDB, PermissionDBBuilder, PermissionDBWithNames,
};
use td_objects::tower_service::with::With;
use td_objects::types::basic::{CollectionName, EntityId, PermissionEntityType, RoleIdName};
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
            let collection_name = CollectionName::try_from(entity_name.to_string())?;
            let collection = conn
                .live()
                .fetch_one::<CollectionDB>(&collection_name)
                .await?;

            EntityId::try_from(collection.id)?
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

pub async fn is_permission_with_names_on_a_single_collection(
    permission: Ref<PermissionDBWithNames>,
) -> Result<Condition, TdError> {
    Ok(Condition(
        permission.permission_type.on_entity_type() == PermissionEntityType::Collection
            && permission.entity_id.is_some(),
    ))
}

pub async fn is_permission_on_a_single_collection(
    permission: Ref<PermissionDB>,
) -> Result<Condition, TdError> {
    Ok(Condition(
        permission.permission_type.on_entity_type() == PermissionEntityType::Collection
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
