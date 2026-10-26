//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::HashMap;
use td_database::access::{AccessMode, InProject, SeesProjectRows, WriteMode};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::and;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::tag::{TopologyTagDB, TopologyTagInsertDB};
use td_objects::dxo::topology::TopologyInstant;
use td_objects::types::basic::{TagEntity, TagEntityId, TagId, TagValue, Taggable, TopologyTagKey};
use td_queries::Queries;

/// The open `td.parent` tag of one entity.
pub async fn parent_tag<S: SeesProjectRows, M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    entity: &TagEntity,
) -> Result<Option<TopologyTagDB>, TdError> {
    Ok(conn
        .scd2()
        .fetch_optional(and!(
            &entity.entity_type,
            &entity.entity_id,
            &TopologyTagKey::Parent,
            TopologyTagDB::topology(),
        ))
        .await?)
}

pub async fn parent_of<T: Taggable, S: SeesProjectRows, M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    child: T,
) -> Result<Option<T>, TdError> {
    parent_tag(conn, &TagEntity::from(child))
        .await?
        .map(|tag| tag.value.id())
        .transpose()
}

/// The entities of `T`'s type whose open `td.parent` tag names `parent`.
pub async fn children_of<T: Taggable, S: SeesProjectRows, M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    parent: T,
) -> Result<Vec<T>, TdError> {
    let tags: Vec<TopologyTagDB> = conn
        .scd2()
        .fetch_all(and!(
            &T::ENTITY_TYPE,
            &TopologyTagKey::Parent,
            &TagValue::of_id(parent),
            TopologyTagDB::topology(),
        ))
        .await?;
    tags.iter().map(|tag| T::try_from(&tag.entity_id)).collect()
}

/// The open `td.parent` tags of the entities, by entity.
pub async fn parents_of<T: Taggable + Eq + std::hash::Hash, S: SeesProjectRows, M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    entities: &[T],
) -> Result<HashMap<T, TopologyTagDB>, TdError> {
    let entity_ids: Vec<TagEntityId> = entities.iter().map(|id| id.tag_entity_id()).collect();
    let tags: Vec<TopologyTagDB> = conn
        .scd2()
        .chunked()
        .fetch_all(
            &entity_ids,
            and!(
                &T::ENTITY_TYPE,
                &TopologyTagKey::Parent,
                TopologyTagDB::topology(),
            ),
        )
        .await?;
    tags.into_iter()
        .map(|tag| Ok((T::try_from(&tag.entity_id)?, tag)))
        .collect()
}

/// The `td.parent` version to open for `entity` naming `parent`, if its open
/// tag does not already: a new tag, or the next version of the current one.
pub fn parent_version(
    ctx: &RequestContext,
    entity: TagEntity,
    parent: TagValue,
    current: Option<&TopologyTagDB>,
) -> Result<Option<TopologyTagInsertDB>, TdError> {
    if current.is_some_and(|current| current.value == parent) {
        return Ok(None);
    }
    let tag_id = current.map_or_else(TagId::default, |current| current.tag_id);
    TopologyTagInsertDB::new(tag_id, entity, TopologyTagKey::Parent, parent, ctx.user_id).map(Some)
}

pub async fn stamp_parent<T: Taggable>(
    conn: &mut DomainConnection<Apiserver, InProject, impl WriteMode>,
    ctx: &RequestContext,
    child: T,
    parent: T,
    topology_at: TopologyInstant,
) -> Result<(), TdError> {
    let entity = TagEntity::from(child);
    let current = parent_tag(conn, &entity).await?;
    if let Some(tag) = parent_version(ctx, entity, TagValue::of_id(parent), current.as_ref())? {
        conn.scd2().open(&tag, topology_at).await?;
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod test_support {
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::and;
    use td_objects::dxo::tag::TopologyTagDB;
    use td_objects::types::basic::{
        FunctionId, TableId, TagEntity, TagValue, Taggable, TopologyTagKey,
    };
    use td_queries::Queries;

    async fn parent<T: Taggable>(db: &TdPool, entity: T) -> Result<Option<T>, TdError> {
        let entity = TagEntity::from(entity);
        db.scd2()
            .fetch_optional::<TopologyTagDB>(and!(
                &entity.entity_type,
                &entity.entity_id,
                &TopologyTagKey::Parent,
                TopologyTagDB::topology(),
            ))
            .await?
            .map(|tag| tag.value.id())
            .transpose()
    }

    async fn children<T: Taggable>(db: &TdPool, parent: T) -> Result<Vec<T>, TdError> {
        db.scd2()
            .fetch_all::<TopologyTagDB>(and!(
                &T::ENTITY_TYPE,
                &TopologyTagKey::Parent,
                &TagValue::of_id(parent),
                TopologyTagDB::topology(),
            ))
            .await?
            .iter()
            .map(|tag| T::try_from(&tag.entity_id))
            .collect()
    }

    pub async fn function_children(
        db: &TdPool,
        function: &FunctionId,
    ) -> Result<Vec<FunctionId>, TdError> {
        children(db, *function).await
    }

    pub async fn function_parent(
        db: &TdPool,
        function: &FunctionId,
    ) -> Result<Option<FunctionId>, TdError> {
        parent(db, *function).await
    }

    pub async fn table_parent(db: &TdPool, table: &TableId) -> Result<Option<TableId>, TdError> {
        parent(db, *table).await
    }
}
