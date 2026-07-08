//
// Copyright 2026 Tabs Data Inc.
//

//! Single-project backend guard.
//!
//! SQLite/MySQL have no row-level security, so the system operates as a single
//! (sandbox) project on those backends — creating additional projects would
//! produce data silos with no isolation. This layer enforces the constraint at
//! the service boundary. (The complementary "sandbox cannot be deleted" rule is
//! backend-independent and lives in [`super::guards`].)

use crate::project::ProjectError;
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_tower::extractors::MutRef;

/// Reject create on a non-PostgreSQL backend: only `sandbox` is allowed there.
pub async fn reject_create_on_single_project_backend(
    conn: MutRef<DomainConnection<Apiserver>>,
) -> Result<(), TdError> {
    if !conn.is_postgres() {
        Err(ProjectError::SingleProjectBackend)?;
    }
    Ok(())
}
