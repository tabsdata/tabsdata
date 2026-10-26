//
// Copyright 2026 Tabsdata Inc.
//

pub mod parent;

use td_database::access::{InProject, WriteMode};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::tag::TopologyTagDB;
use td_objects::dxo::topology::TopologyInstant;
use td_objects::types::basic::{TagEntityId, TagEntityType};
use td_queries::Queries;

/// Close every open tag, of either kind, of entities that are going away.
pub async fn close_tags_of(
    conn: &mut DomainConnection<Apiserver, InProject, impl WriteMode>,
    entity_type: TagEntityType,
    entity_ids: &[TagEntityId],
    topology_at: TopologyInstant,
) -> Result<(), TdError> {
    if entity_ids.is_empty() {
        return Ok(());
    }
    conn.scd2()
        .chunked()
        .close::<TopologyTagDB>(entity_ids, &entity_type, topology_at)
        .await?;
    Ok(())
}
