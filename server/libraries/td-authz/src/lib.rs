//
// Copyright 2025 Tabsdata Inc.
//

//! Tabsdata authorization.
//!
//! The authorization model is project-oriented RBAC: a
//! permission targets an `(entity_type, entity_id)` entity, users gain
//! authority through role assignment, and a request is authorized by checking
//! the requester's effective [`rbac::Grants`] (the union across all their
//! assigned roles) against the operation's requirement.
//!
//! Services build a [`rbac::Scope`] (the entity the operation targets) and
//! call the [`rbac::Enforce`] check (the requirement(s)) against their
//! [`AuthzContext`]. See [`rbac`] for the full contract.

use crate::rbac::SqlRbacSource;

pub mod rbac;
pub mod security;
pub mod session_provider;

/// The authorization context services check against.
///
/// This is the production [`rbac::RbacSource`] (SQL-backed, cached), shared
/// as a `Ctx<AuthzContext>` service dependency. Construct it with
/// [`Default`]. Refresh it (`AuthzContext::refresh`) after committing any
/// change to role assignments or role permissions.
pub type AuthzContext = SqlRbacSource;
