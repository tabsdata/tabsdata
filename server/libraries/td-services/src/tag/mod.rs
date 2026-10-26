//
// Copyright 2026 Tabsdata Inc.
//

pub mod parent;
pub mod services;
pub mod user;

use td_database::access::{InProject, WriteMode};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::tag::TopologyTagDB;
use td_objects::dxo::topology::TopologyInstant;
use td_objects::types::basic::{TagEntityId, Taggable};
use td_queries::Queries;

/// Every open tag of the entities, closed with them at the topology instant.
pub async fn close_tags_of<T: Taggable>(
    conn: &mut DomainConnection<Apiserver, InProject, impl WriteMode>,
    entities: &[T],
    topology_at: TopologyInstant,
) -> Result<(), TdError> {
    if entities.is_empty() {
        return Ok(());
    }
    let entity_ids: Vec<TagEntityId> = entities.iter().map(|id| id.tag_entity_id()).collect();
    conn.scd2()
        .chunked()
        .close::<TopologyTagDB>(&entity_ids, &T::ENTITY_TYPE, topology_at)
        .await?;
    Ok(())
}
