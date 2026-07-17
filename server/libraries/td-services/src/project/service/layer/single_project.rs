//
// Copyright 2026 Tabs Data Inc.
//

//! Single-project backend guard.
//!
//! SQLite/MySQL have no row-level security to isolate tenants, so they do not
//! support projects at all; this layer rejects `create` on them. PostgreSQL is
//! unrestricted.

use crate::project::ProjectError;
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_tower::extractors::MutRef;

/// Reject `create` on a non-PostgreSQL backend, which cannot isolate projects.
pub async fn reject_create_on_single_project_backend(
    conn: MutRef<DomainConnection<Apiserver>>,
) -> Result<(), TdError> {
    if !conn.is_postgres() {
        Err(ProjectError::SingleProjectBackend)?;
    }
    Ok(())
}
