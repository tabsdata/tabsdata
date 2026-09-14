//
// Copyright 2026 Tabsdata Inc.
//

//! RoleGrant sources: where a request's [`Grants`] come from.
//!
//! The async boundary lives here and only here — the enforcement decision
//! itself ([`Grants::allows`]) is pure. A source resolves a user to their
//! effective grants (the union across the user's assigned roles); the
//! SQL-backed implementation ([`super::sql_source`]) loads `role_permission`
//! rows, groups them per user, and caches the result.

use crate::rbac::model::{EntityKind, Grants, IdentityClaim, RoleGrant, User};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use td_database::access::DbScope;
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::crudl::RequestContext;
use td_objects::types::basic::UserId;

/// Resolves the effective grants of a user.
///
/// Implementations are expected to cache: the contract is "return an
/// `Arc<Grants>` snapshot", so the per-check cost is a map lookup plus a
/// clone of the `Arc`.
pub trait RbacSource: Send + Sync {
    /// The effective grants for the user: the union across their assigned
    /// roles; no grants is an empty [`Grants`], not an error. The slot may be
    /// latent — implementations should touch it only on a cache miss.
    fn user_grants<S: DbScope>(
        &self,
        slot: &mut DomainConnection<Apiserver, S>,
        user: &UserId,
    ) -> impl Future<Output = Result<Arc<Grants>, TdError>> + Send;

    /// Invalidate any cached state. Must be called when role assignments or
    /// role permissions change.
    fn refresh<S: DbScope>(
        &self,
        slot: &mut DomainConnection<Apiserver, S>,
    ) -> impl Future<Output = Result<(), TdError>> + Send;

    /// Whether the kind's source of truth still knows the entity; nothing is
    /// authorized on one it does not.
    fn is_current<E: EntityKind, S: DbScope>(
        &self,
        slot: &mut DomainConnection<Apiserver, S>,
        id: &E::Id,
    ) -> impl Future<Output = Result<bool, TdError>> + Send;
}

/// An in-memory [`RbacSource`].
///
/// This is the unit-test double — services are tested against it
/// with no database — and the reference for what the SQL-backed source must
/// produce per user.
#[derive(Debug)]
pub struct FixedRbacSource {
    users: HashMap<UserId, Arc<Grants>>,
    gone: HashSet<(&'static str, String)>,
    empty: Arc<Grants>,
}

impl Default for FixedRbacSource {
    fn default() -> Self {
        Self {
            users: HashMap::new(),
            gone: HashSet::new(),
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

    /// Mark an entity as no longer current; every other entity is.
    pub fn with_gone<E: EntityKind>(mut self, id: E::Id) -> Self {
        self.gone.insert((E::NAME, id.to_string()));
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
    async fn user_grants<S: DbScope>(
        &self,
        _slot: &mut DomainConnection<Apiserver, S>,
        user: &UserId,
    ) -> Result<Arc<Grants>, TdError> {
        Ok(self.users.get(user).unwrap_or(&self.empty).clone())
    }

    async fn refresh<S: DbScope>(
        &self,
        _slot: &mut DomainConnection<Apiserver, S>,
    ) -> Result<(), TdError> {
        Ok(())
    }

    async fn is_current<E: EntityKind, S: DbScope>(
        &self,
        _slot: &mut DomainConnection<Apiserver, S>,
        id: &E::Id,
    ) -> Result<bool, TdError> {
        Ok(!self.gone.contains(&(E::NAME, id.to_string())))
    }
}

/// The implicit grants of a request: the requester holds the
/// [`IdentityClaim::Requester`] claim on their own user id.
///
/// "the requester may act on their own user" is then an ordinary requirement
/// checked against these grants, with no dedicated branch in the enforcement
/// path.
pub fn identity_grants(request: &RequestContext) -> Grants {
    Grants::new().with(RoleGrant::<User>::on(
        request.user_id,
        IdentityClaim::Requester,
    ))
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
