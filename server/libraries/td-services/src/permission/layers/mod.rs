//
// Copyright 2025 Tabs Data Inc.
//

use async_trait::async_trait;
use std::ops::Deref;
use td_error::TdError;
use td_objects::entity_finder::collections::CollectionFinder;
use td_objects::tower_service::from::With;
use td_objects::types::basic::EntityId;
use td_objects::types::permission::{PermissionCreate, PermissionDB, PermissionDBBuilder};
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
