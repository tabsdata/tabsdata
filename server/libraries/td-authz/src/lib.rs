//
// Copyright 2025 Tabs Data Inc.
//

//! Tabsdata authorization.
//!
//! The authorization model is the project-oriented RBAC model (TEDA): a
//! permission targets an `(entity_type, entity_id)` entity, users gain
//! authority through role assignment, and a request is authorized by checking
//! the requester's effective [`rbac::Grants`] (the union across all their
//! assigned roles) against the operation's requirement.
//!
//! Services wire two layers — a [`rbac::Scope`] layer (the entity the
//! operation targets) and a [`rbac::Rbac`] enforcement layer (the
//! requirement(s)) — reading a `Ref<SqlRbacSource>` placed in the service
//! context. See [`rbac`] for the full contract.

use crate::rbac::SqlRbacSource;
use crate::rbac::source::RbacSource;
use td_database::dynamic::DynamicConnection;
use td_error::TdError;
use td_tower::extractors::{MutRef, Ref};
use tracing::instrument;

pub mod jwt;
pub mod rbac;
pub mod session_provider;

/// The authorization context placed in the service context.
///
/// This is the production [`rbac::RbacSource`] (SQL-backed, cached). Services
/// declare `context = AuthzContext` on their service factory so the
/// `Ref<AuthzContext>` the [`rbac::Rbac`] enforcement layer reads is
/// available. Construct it with [`Default`].
pub type AuthzContext = SqlRbacSource;

/// Layer to refresh the authorization context. It must be called every time
/// role assignments or role permissions change.
#[instrument(name = "authz.refresh", skip_all, level = "debug", err(Debug))]
pub async fn refresh_authz_context(
    context: Ref<AuthzContext>,
    mut conn: MutRef<DynamicConnection<'static>>,
) -> Result<(), TdError> {
    context.refresh(&mut conn).await
}
