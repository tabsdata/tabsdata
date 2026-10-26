//
// Copyright 2026 Tabsdata Inc.
//

//! `td.parent`: a child function's parent function, a derived table's parent
//! table.

use std::collections::HashMap;
use td_database::access::{AccessMode, InProject, SeesProjectRows, WriteMode};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::and;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::tag::{TopologyTagDB, TopologyTagInsertDB};
use td_objects::dxo::topology::TopologyInstant;
use td_objects::types::basic::{
    FunctionId, TableId, TagEntityId, TagEntityType, TagId, TagValue, TopologyTagKey,
};
use td_queries::Queries;

pub async fn parent_function<S: SeesProjectRows, M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    function: &FunctionId,
) -> Result<Option<FunctionId>, TdError> {
    let tag: Option<TopologyTagDB> = conn
        .scd2()
        .fetch_optional(and!(
            &TagEntityType::Function,
            &TagEntityId::try_from(*function)?,
            &TopologyTagKey::Parent,
            TopologyTagDB::topology(),
        ))
        .await?;
    tag.map(|tag| tag.base.value.id()).transpose()
}

pub async fn child_functions<S: SeesProjectRows, M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    parent: &FunctionId,
) -> Result<Vec<FunctionId>, TdError> {
    children(conn, TagEntityType::Function, TagValue::of_id(*parent))
        .await?
        .iter()
        .map(FunctionId::try_from)
        .collect()
}

pub async fn child_tables<S: SeesProjectRows, M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    parent: &TableId,
) -> Result<Vec<TableId>, TdError> {
    children(conn, TagEntityType::Table, TagValue::of_id(*parent))
        .await?
        .iter()
        .map(TableId::try_from)
        .collect()
}

async fn children<S: SeesProjectRows, M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    entity_type: TagEntityType,
    parent: TagValue,
) -> Result<Vec<TagEntityId>, TdError> {
    let tags: Vec<TopologyTagDB> = conn
        .scd2()
        .fetch_all(and!(
            &entity_type,
            &TopologyTagKey::Parent,
            &parent,
            TopologyTagDB::topology(),
        ))
        .await?;
    Ok(tags.into_iter().map(|tag| tag.base.entity_id).collect())
}

pub async fn table_parents<S: SeesProjectRows, M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    tables: &[TableId],
) -> Result<HashMap<TableId, TopologyTagDB>, TdError> {
    let entity_ids = tables
        .iter()
        .map(|id| TagEntityId::try_from(*id))
        .collect::<Result<Vec<_>, _>>()?;
    let tags: Vec<TopologyTagDB> = conn
        .scd2()
        .chunked()
        .fetch_all(
            &entity_ids,
            and!(
                &TagEntityType::Table,
                &TopologyTagKey::Parent,
                TopologyTagDB::topology(),
            ),
        )
        .await?;
    tags.into_iter()
        .map(|tag| Ok((TableId::try_from(&tag.base.entity_id)?, tag)))
        .collect()
}

/// The version that makes `parent` the entity's parent, continuing the
/// entity's `current` parent tag when there is one; `None` when it already is.
pub fn parent_version(
    ctx: &RequestContext,
    entity_type: TagEntityType,
    entity_id: TagEntityId,
    parent: TagValue,
    current: Option<&TopologyTagDB>,
) -> Result<Option<TopologyTagInsertDB>, TdError> {
    if current.is_some_and(|current| current.base.value == parent) {
        return Ok(None);
    }
    let tag_id = current.map_or_else(TagId::default, |current| current.base.tag_id);
    TopologyTagInsertDB::new(
        tag_id,
        entity_type,
        entity_id,
        TopologyTagKey::Parent,
        parent,
        ctx.user_id,
    )
    .map(Some)
}

pub async fn stamp_function_parent(
    conn: &mut DomainConnection<Apiserver, InProject, impl WriteMode>,
    ctx: &RequestContext,
    function: &FunctionId,
    parent: &FunctionId,
    topology_at: TopologyInstant,
) -> Result<(), TdError> {
    let entity_id = TagEntityId::try_from(*function)?;
    let current: Option<TopologyTagDB> = conn
        .scd2()
        .fetch_optional(and!(
            &TagEntityType::Function,
            &entity_id,
            &TopologyTagKey::Parent,
            TopologyTagDB::topology(),
        ))
        .await?;
    if let Some(tag) = parent_version(
        ctx,
        TagEntityType::Function,
        entity_id,
        TagValue::of_id(*parent),
        current.as_ref(),
    )? {
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
        FunctionId, TableId, TagEntityId, TagEntityType, TopologyTagKey,
    };
    use td_queries::Queries;

    async fn parent(
        db: &TdPool,
        entity_type: TagEntityType,
        entity_id: TagEntityId,
    ) -> Result<Option<TopologyTagDB>, TdError> {
        Ok(db
            .scd2()
            .fetch_optional::<TopologyTagDB>(and!(
                &entity_type,
                &entity_id,
                &TopologyTagKey::Parent,
                TopologyTagDB::topology(),
            ))
            .await?)
    }

    pub async fn function_parent(
        db: &TdPool,
        function: &FunctionId,
    ) -> Result<Option<FunctionId>, TdError> {
        parent(
            db,
            TagEntityType::Function,
            TagEntityId::try_from(*function)?,
        )
        .await?
        .map(|tag| tag.base.value.id())
        .transpose()
    }

    pub async fn table_parent(db: &TdPool, table: &TableId) -> Result<Option<TableId>, TdError> {
        parent(db, TagEntityType::Table, TagEntityId::try_from(*table)?)
            .await?
            .map(|tag| tag.base.value.id())
            .transpose()
    }
}
