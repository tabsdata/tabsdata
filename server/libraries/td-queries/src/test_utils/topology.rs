//
// Copyright 2026 Tabsdata Inc.
//

use crate::Queries;
use ta_objects::errors::SqlError;
use td_database::access::{DbScope, InProject, LockInstant, RW};
use td_database::domain::{Apiserver, DomainConnection};
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::topology::TopologyInstant;
use td_objects::types::basic::{AtTime, ProjectId};

/// A serializable transaction in `project` that opened the project's next
/// topology version, as a topology-writing service's does.
pub async fn topology_write(
    db: &TdPool,
    project: &ProjectId,
) -> Result<(DomainConnection<Apiserver, InProject, RW>, TopologyInstant), TdError> {
    topology_write_at(db, project, LockInstant::asserted(*AtTime::now())).await
}

/// [`topology_write`] for a unit of work writing at `at`.
pub async fn topology_write_at(
    db: &TdPool,
    project: &ProjectId,
    at: LockInstant,
) -> Result<(DomainConnection<Apiserver, InProject, RW>, TopologyInstant), TdError> {
    let failed = |e| SqlError::AnySqlxError("topology".to_string(), e);
    let mut trx = db.begin_serializable().await.map_err(failed)?;
    InProject::new(project).apply(&mut trx).await?;
    let mut conn = DomainConnection::new(trx);
    let topology_at = conn.topology().bump(project, at).await?;
    Ok((conn, topology_at))
}

/// The lock instant a seed opens its project, collection and group versions
/// at.
pub fn seed_instant() -> LockInstant {
    LockInstant::asserted(*AtTime::now())
}

pub async fn commit_topology_write(
    conn: DomainConnection<Apiserver, InProject, RW>,
) -> Result<(), TdError> {
    conn.commit()
        .await
        .map_err(|e| SqlError::AnySqlxError("topology".to_string(), e))?;
    Ok(())
}
