//
// Copyright 2025 Tabs Data Inc.
//

use crate::permission::PermissionError;
use async_trait::async_trait;
use std::ops::Deref;
use td_error::TdError;
use td_objects::entity_finder::collections::CollectionFinder;
use td_objects::tower_service::from::With;
use td_objects::types::basic::{EntityId, PermissionEntityType, RoleIdName};
use td_objects::types::permission::{
    PermissionCreate, PermissionDB, PermissionDBBuilder, PermissionDBWithNames,
};
use td_objects::types::IdOrName;
use td_tower::default_services::Condition;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

#[async_trait]
pub trait PermissionBuildService {
    async fn build_permission_db(
        connection: Connection,
        // queries: SrvCtx<Q>, TODO should we use queries here??
        input: Input<PermissionDBBuilder>,
        permission_create: Input<PermissionCreate>,
    ) -> Result<PermissionDB, TdError>;
}

#[async_trait]
impl PermissionBuildService for With<PermissionDBBuilder> {
    async fn build_permission_db(
        Connection(connection): Connection,
        // SrvCtx(queries): SrvCtx<Q>,
        Input(input): Input<PermissionDBBuilder>,
        Input(permission_create): Input<PermissionCreate>,
    ) -> Result<PermissionDB, TdError> {
        let mut conn = connection.lock().await;
        let conn = conn.get_mut_connection()?;

        let permission_type = permission_create.permission_type();
        let entity_name = permission_create.entity_name();
        let entity_type = permission_type.on_entity_type();

        let entity_id = if let Some(entity_name) = entity_name {
            let collection = CollectionFinder::default()
                .find_by_name(conn, entity_name)
                .await?;
            let entity_id = EntityId::try_from(collection.id().as_str())?;
            Some(entity_id)
        } else {
            None
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
    Input(permission): Input<PermissionDBWithNames>,
) -> Result<Condition, TdError> {
    Ok(Condition(
        permission.permission_type().on_entity_type() == PermissionEntityType::Collection
            && permission.entity_id().is_some(),
    ))
}

pub async fn is_permission_on_a_single_collection(
    Input(permission): Input<PermissionDB>,
) -> Result<Condition, TdError> {
    Ok(Condition(
        permission.permission_type().on_entity_type() == PermissionEntityType::Collection
            && permission.entity_id().is_some(),
    ))
}

pub async fn assert_permission_is_not_fixed(
    Input(permission): Input<PermissionDBWithNames>,
) -> Result<(), TdError> {
    if **permission.fixed() {
        Err(PermissionError::PermissionIsFixed)?
    } else {
        Ok(())
    }
}

pub async fn assert_role_in_permission(
    Input(role_id_name): Input<RoleIdName>,
    Input(permission): Input<PermissionDBWithNames>,
) -> Result<(), TdError> {
    if let Some(role_id) = role_id_name.id() {
        if role_id != permission.role_id() {
            Err(PermissionError::RolePermissionMismatch)?
        }
    }
    if let Some(role_name) = role_id_name.name() {
        if role_name != permission.role() {
            Err(PermissionError::RolePermissionMismatch)?
        }
    }
    Ok(())
}
