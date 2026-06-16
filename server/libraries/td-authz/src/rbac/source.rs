//
// Copyright 2026 Tabs Data Inc.
//

//! Grant sources: where a request's [`Grants`] come from.
//!
//! The async boundary of the new API lives here and only here — the
//! enforcement decision itself ([`Grants::allows`]) is pure. A source
//! resolves a user to their effective grants (the union across the user's
//! assigned roles, per the TEDA doc); the SQL-backed implementation mirrors
//! [`crate::authz_provider::SqlAuthzDataProvider`] (load `role_permission`
//! rows, group, cache behind `CachedProvider`) once the new schema lands.

use crate::rbac::model::{Grant, Grants, IdentityClaim, User};
use std::collections::HashMap;
use std::sync::Arc;
use td_database::dynamic::DynamicConnection;
use td_error::TdError;
use td_objects::dxo::crudl::RequestContext;
use td_objects::types::basic::UserId;

/// Resolves the effective grants of a user.
///
/// Implementations are expected to cache: the contract is "return an
/// `Arc<Grants>` snapshot", so the per-check cost is a map lookup plus a
/// clone of the `Arc`, exactly like the current `AuthzContextT` role cache.
pub trait RbacSource: Send + Sync {
    /// The effective grants for the user: the union of the grants of every
    /// role the user is assigned to. A user with no grants returns an empty
    /// [`Grants`], not an error.
    fn user_grants(
        &self,
        conn: &mut DynamicConnection<'_>,
        user: &UserId,
    ) -> impl Future<Output = Result<Arc<Grants>, TdError>> + Send;

    /// Invalidate any cached state. Must be called when role assignments or
    /// role permissions change (same contract as `AuthzContextT::refresh`).
    fn refresh(
        &self,
        conn: &mut DynamicConnection<'_>,
    ) -> impl Future<Output = Result<(), TdError>> + Send;
}

/// An in-memory [`RbacSource`].
///
/// This is the unit-test double — services and layers are tested against it
/// with no database — and the reference for what the SQL-backed source must
/// produce per user.
#[derive(Debug)]
pub struct FixedRbacSource {
    users: HashMap<UserId, Arc<Grants>>,
    empty: Arc<Grants>,
}

impl Default for FixedRbacSource {
    fn default() -> Self {
        Self {
            users: HashMap::new(),
            empty: Arc::new(Grants::new()),
        }
    }
}

impl FixedRbacSource {
    /// A source with no users (every user resolves to empty grants).
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the effective grants for a user.
    pub fn with_user(mut self, user: UserId, grants: Grants) -> Self {
        self.users.insert(user, Arc::new(grants));
        self
    }

    /// Set the effective grants for a user as the union of per-role grants —
    /// the doc's "a user's effective permission is the union across all
    /// their assigned roles".
    pub fn with_user_roles(self, user: UserId, roles: impl IntoIterator<Item = Grants>) -> Self {
        let grants = roles
            .into_iter()
            .fold(Grants::new(), |acc, role| acc.union(&role));
        self.with_user(user, grants)
    }
}

impl RbacSource for FixedRbacSource {
    async fn user_grants(
        &self,
        _conn: &mut DynamicConnection<'_>,
        user: &UserId,
    ) -> Result<Arc<Grants>, TdError> {
        Ok(self.users.get(user).unwrap_or(&self.empty).clone())
    }

    async fn refresh(&self, _conn: &mut DynamicConnection<'_>) -> Result<(), TdError> {
        Ok(())
    }
}

/// The implicit grants of a request: the requester holds the
/// [`IdentityClaim::Requester`] claim on their own user id.
///
/// This replaces the old API's `Requester` / `SystemOrUser` special cases:
/// "the requester may act on their own user" is an ordinary requirement
/// checked against these grants, with no dedicated branch in the enforcement
/// path.
pub fn identity_grants(request: &RequestContext) -> Grants {
    Grants::new().with(Grant::<User>::on(request.user_id, IdentityClaim::Requester))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rbac::model::{
        ProjectId, ProjectPermission, ProjectRole, Requires, SystemPermission, SystemRole,
    };
    use td_objects::types::basic::AccessTokenId;

    #[test]
    fn test_identity_grants() {
        let user_id = UserId::default();
        let other_user = UserId::default();
        let request = RequestContext::with(AccessTokenId::default(), user_id);
        let grants = identity_grants(&request);
        assert!(grants.allows(&Requires::<User>::on(user_id, IdentityClaim::Requester)));
        assert!(!grants.allows(&Requires::<User>::on(other_user, IdentityClaim::Requester)));
    }

    #[test]
    fn test_fixed_source_union_across_roles() {
        let user_id = UserId::default();
        let p = ProjectId::default();
        let source = FixedRbacSource::new().with_user_roles(
            user_id,
            [SystemRole::Default.grants(), ProjectRole::Dev.grants(p)],
        );
        let grants = source.users.get(&user_id).unwrap();
        assert!(grants.allows(&Requires::system(SystemPermission::CreateProjects)));
        assert!(grants.allows(&ProjectPermission::DevelopProject.required_on(p)));
        assert!(!grants.allows(&ProjectPermission::ManageProject.required_on(p)));
    }
}
