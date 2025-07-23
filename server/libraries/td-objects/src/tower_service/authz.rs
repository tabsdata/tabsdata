//
// Copyright 2025 Tabs Data Inc.
//

//! Services Authorization layers.
//!
//!
//! Assuming `PermissionsStore` implements [`AuthzContextT`].
//!
//! ```ignore
//!     type Authz<
//!        C1,
//!        C2 = NoPermissions,
//!        C3 = NoPermissions,
//!        C4 = NoPermissions,
//!        C5 = NoPermissions,
//!        C6 = NoPermissions,
//!        C7 = NoPermissions,
//!    > = authz::Authz<PermissionsStore, C1, C2, C3, C4, C5, C6, C7>;
//! ```
//!
//! System permission check:
//!
//! The requester's role must have [`SysAdmin`] or [`SecAdmin`] permissions.
//!
//! ```ignore
//!   ...
//!   layer(AuthzOn<System>::set).
//!   layer(Authz<SysAdmin, SecAdmin>::check).
//!   ...
//! ```
//!
//! User check:
//!
//! The requester's user must match the set [`UserId`].
//!
//! ```ignore
//!   ...
//!   layer(from_fn(extract_id::<User, UserId>)). // get  the user ID from somewhere
//!   layer(AuthzOn<UserId>::set).
//!   layer(Authz<Requester>::check).
//!   ...
//! ```
//!
//! Role check:
//!
//! The requester's role must match the set [`RoleId`].
//!
//! ```ignore
//!   ...
//!   layer(from_fn(extract_id::<Role, RoleId>)). // get  the role ID from somewhere
//!   layer(AuthzOn<RoleId>::set).
//!   layer(Authz<Requester>::check).
//!   ...
//! ```
//!
//! System permission or User check.
//!
//! The requester's user must match the set [`UserId`]
//! or the requester's role must have [`SecAdmin`] permission.
//!
//! ```ignore
//!   ...
//!   layer(from_fn(extract_id::<User, UserId>)). // get  the user ID from somewhere
//!   layer(AuthzOn<SystemOrUserId>::set).
//!   layer(Authz<Requester, SecAdmin>::check).
//!   ...
//! ```
//!
//! System permission or Role check.
//!
//! The requester's role must match the set [`RoleId`]
//! or the requester's role must have [`SecAdmin`] permission.
//!
//! ```ignore
//!   ...
//!   layer(from_fn(extract_id::<Role, RoleId>)). // get  the role ID from somewhere
//!   layer(AuthzOn<SystemOrRoleId>::set).
//!   layer(Authz<Requester, SecAdmin>::check).
//!   ...
//! ```
//!
//! CollectionAdmin on System scope check.
//!
//! The requester's role must have a [`CollectionAdmin`] permission on any collection.
//!
//! ```ignore
//!   ...
//!   layer(AuthzOn<System>::set).
//!   layer(Authz<CollectionAdmin>::check).
//!   ...
//! ```
//!
//! Collections permissions check:
//!
//! The requester's role must have a [`CollAdmin`] permission either on the set [`CollectionId`]
//! or on [`All`] collections
//!
//! ```ignore
//!   ...
//!   .layer(from_fn(extract_id::<Collection, CollectionId>)) // get the collection ID from somewhere
//!   .layer(AuthzOn<CollectionId>::set).
//!   .layer(Authz<CollAdmin>::check).
//!   ...
//! ```
//!
//! System or Collections permissions check:
//!
//! The requester's role must have a system [`SecAdmin`] permission, a [`CollAdmin`] permission
//! either on the set [`CollectionId`] or on [`All`] collections.
//!
//! ```ignore
//!   ...
//!   .layer(from_fn(extract_id::<Collection, CollectionId>)) // get the collection ID from somewhere
//!   .layer(AuthzOn<CollectionId>::set).
//!   .layer(Authz<SecAdmin, CollAdmin>::check).
//!   ...
//! ```

use crate::crudl::{handle_sql_err, RequestContext};
use crate::sql::{DaoQueries, FindBy};
use crate::types::basic::{CollectionId, RoleId, ToCollectionId, UserId, VisibleCollections};
use crate::types::collection::CollectionDB;
use crate::types::permission::InterCollectionAccess;
use async_trait::async_trait;
use itertools::Itertools;
use sqlx::SqliteConnection;
use std::any::type_name;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Display, Formatter};
use std::marker::PhantomData;
use std::mem;
use std::ops::Deref;
use std::sync::Arc;
use td_error::TdError;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection, SrvCtx};

#[async_trait]
pub trait AuthzContextT: Send + Sync {
    /// Return the permissions for the given role.
    ///
    /// If the role has no permissions it returns [`None`].
    async fn role_permissions(
        &self,
        conn: &mut SqliteConnection,
        role: &RoleId,
    ) -> Result<Option<Arc<Vec<Permission>>>, TdError>;

    /// Return the collection permissions for a given role.
    ///
    /// If the role has an [`AuthzEntity::All`] on a permission, any specific [`AuthzEntity::On(<collection>`]
    /// with the same permission is not returned.
    async fn role_collections_permissions(
        &self,
        conn: &mut SqliteConnection,
        role: &RoleId,
    ) -> Result<Option<Vec<Permission>>, TdError> {
        let perms = self.role_permissions(conn, role).await?.map(|permissions| {
            permissions
                .iter()
                .filter(|p| p.is_on_collection())
                .map(Permission::clone)
                .collect::<Vec<_>>()
        });
        let perms = if let Some(ref perms) = perms {
            let on_all_collections = perms
                .iter()
                .filter(|p| (*p).is_on_all_collections())
                .collect::<Vec<_>>();
            let mut simplified_perms = on_all_collections
                .iter()
                .map(|p| (*p).clone())
                .collect::<Vec<_>>();

            for perm in perms {
                if on_all_collections.is_empty() {
                    simplified_perms.push(perm.clone());
                } else {
                    for on_all in on_all_collections.iter() {
                        if !perm.is_same_type(on_all) {
                            //we only add the collection permission if we don't have the same type of permission for all collections.
                            simplified_perms.push(perm.clone());
                        }
                    }
                }
            }
            Some(simplified_perms)
        } else {
            None
        };
        Ok(perms)
    }

    /// Return the collections that can access public tables in the the given collection.
    async fn inter_collections_permissions_value_can_read_key(
        &self,
        conn: &mut SqliteConnection,
        collection_id: &CollectionId,
    ) -> Result<Option<Arc<Vec<ToCollectionId>>>, TdError>;

    /// Return the collections that can read from the given collection.
    async fn inter_collections_permissions_key_can_read_value(
        &self,
        conn: &mut SqliteConnection,
        collection_id: &ToCollectionId,
    ) -> Result<Option<Arc<Vec<CollectionId>>>, TdError>;

    /// Return a [`(Vec<CollectionId>`,`Vec<CollectionId>)`] pair where the first vector contains the
    /// collections the given role has at least one permission for, and the second vector contains
    /// all the collections that any of the collections from the first vector have inter-collection
    /// permission to.
    ///
    /// The first vector may contain the [`CollectionId::all_collections()`] to indicate that the role
    /// has permissions on all collections. If that is the case, the first vector does not have any
    /// other
    async fn visible_collections(
        &self,
        conn: &mut SqliteConnection,
        role: &RoleId,
    ) -> Result<VisibleCollections, TdError> {
        let collection_permissions = self
            .role_collections_permissions(conn, role)
            .await?
            .map(|collections| {
                collections
                    .into_iter()
                    .filter(Permission::is_on_collection)
                    .collect::<HashSet<_>>()
            })
            .unwrap_or_default();

        let mut a_perm_on_all_collections = false;
        let mut collections_with_permissions = HashSet::new();
        let mut need_inter_collection_check_for = HashSet::new();
        for perm in collection_permissions {
            let collection_id = match perm {
                Permission::CollectionAdmin(AuthzEntity::On(collection_id)) => {
                    // admin can do dev stuff, needs to see tables in inter-collections of the collection
                    need_inter_collection_check_for.insert(collection_id);
                    collection_id
                }
                Permission::CollectionDev(AuthzEntity::On(collection_id)) => {
                    // dev needs to see tables in inter-collections of the collection
                    need_inter_collection_check_for.insert(collection_id);
                    collection_id
                }
                Permission::CollectionExec(AuthzEntity::On(collection_id)) => collection_id,
                Permission::CollectionRead(AuthzEntity::On(collection_id)) => collection_id,
                Permission::CollectionAdmin(AuthzEntity::All)
                | Permission::CollectionDev(AuthzEntity::All)
                | Permission::CollectionRead(AuthzEntity::All)
                | Permission::CollectionExec(AuthzEntity::All) => {
                    a_perm_on_all_collections = true;
                    CollectionId::all_collections()
                }
                _ => unreachable!(),
            };
            collections_with_permissions.insert(collection_id);
        }
        if a_perm_on_all_collections {
            collections_with_permissions = HashSet::from([CollectionId::all_collections()]);
            // already has access to all collections, no need to check inter-collection access
            need_inter_collection_check_for = HashSet::new();
        }

        let mut inter_collection_permissions = HashSet::new();
        if !need_inter_collection_check_for.is_empty() {
            for coll in need_inter_collection_check_for.iter() {
                let inter_collection_for_coll = self
                    .inter_collections_permissions_key_can_read_value(
                        conn,
                        &coll.try_into().unwrap(),
                    )
                    .await?
                    .map(|perms| perms.iter().copied().collect::<HashSet<_>>())
                    .unwrap_or_default();
                inter_collection_permissions.extend(inter_collection_for_coll);
            }
        }
        Ok(VisibleCollections::new(
            collections_with_permissions,
            inter_collection_permissions,
        ))
    }

    async fn refresh(&self, _conn: &mut SqliteConnection) -> Result<(), TdError> {
        Ok(())
    }
}

#[td_error::td_error]
pub enum AuthzError {
    #[error("Forbidden inter collection access: {0}")]
    ForbiddenInterCollectionAccess(String) = 3000,

    #[error("Forbidden access '{0}'")]
    Forbidden(String) = 3001,

    #[error("Invalid authorization scope, '{0}' cannot be on {1}")]
    InvalidAuthzScope(String, String) = 5000,

    #[error("The entity returned by a '{0}::any_of()' cannot be `{1}`")]
    AuthEntityCannotBeAll(String, String) = 5001,
}

/// Enum that denotes an Entity to check authorization on.
#[derive(Debug, Clone, PartialEq, Eq, Hash, strum::IntoStaticStr)]
pub enum AuthzEntity<E> {
    /// A single entity of the used generic by its ID.
    On(E),
    /// All entities of the used generic.
    All,
}

impl<E> AuthzEntity<E> {
    pub fn to_str(&self) -> &'static str {
        self.into()
    }
}

impl<E: Debug> Display for AuthzEntity<E> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::On(e) => write!(f, "On({e:?})"),
            Self::All => write!(f, "All"),
        }
    }
}

/// Enum that denotes the scope for permissions.
#[derive(Debug, Clone, PartialEq, Eq, Hash, strum::IntoStaticStr)]
pub enum AuthzScope {
    /// A system permission.
    System,
    /// A collection permission.
    Collection(AuthzEntity<CollectionId>),
    /// A user permission.
    User(AuthzEntity<UserId>),
    /// A role permission.
    Role(AuthzEntity<RoleId>),
    /// A system or user permission.
    SystemOrUser(AuthzEntity<UserId>),
    /// A system or user permission.
    SystemOrRole(AuthzEntity<RoleId>),
}

impl AuthzScope {
    pub fn to_str(&self) -> &'static str {
        self.into()
    }
}

impl Display for AuthzScope {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::System => write!(f, "All"),
            Self::Collection(c) => write!(f, "Collection({c:?})"),
            Self::User(u) => write!(f, "User({u:?})"),
            Self::Role(u) => write!(f, "Role({u:?})"),
            Self::SystemOrUser(u) => write!(f, "SystemOrUser({u:?})"),
            Self::SystemOrRole(u) => write!(f, "SystemOrRole({u:?})"),
        }
    }
}

/// System marker type to set the Authorization scope to [`AuthzScope::System`].
pub struct System {
    #[allow(dead_code)]
    instance_blocker: (),
}

/// Used to set the [`AuthzScope`] in the service context.
pub struct AuthzOn<S> {
    _s: PhantomData<S>,
}

impl AuthzOn<System> {
    /// Set the Authorization scope to [`AuthzScope::System`] in the service context.
    pub async fn set() -> Result<AuthzScope, TdError> {
        Ok(AuthzScope::System)
    }
}

impl AuthzOn<CollectionId> {
    /// Set the Authorization scope to [`AuthzScope::Collection`] for a [`CollectionId`] in the service context.
    pub async fn set(Input(collection_id): Input<CollectionId>) -> Result<AuthzScope, TdError> {
        Ok(AuthzScope::Collection(AuthzEntity::On(*collection_id)))
    }
}

impl AuthzOn<UserId> {
    /// Set the Authorization scope to [`AuthzScope::User`] for a [`UserId`] in the service context.
    pub async fn set(Input(user_id): Input<UserId>) -> Result<AuthzScope, TdError> {
        Ok(AuthzScope::User(AuthzEntity::On(*user_id)))
    }
}

impl AuthzOn<RoleId> {
    /// Set the Authorization scope to [`AuthzScope::Role`] for a [`RoleId`] in the service context.
    pub async fn set(Input(role_id): Input<RoleId>) -> Result<AuthzScope, TdError> {
        Ok(AuthzScope::Role(AuthzEntity::On(*role_id)))
    }
}

pub struct SystemOrUserId {
    #[allow(dead_code)]
    instance_blocker: (),
}

impl AuthzOn<SystemOrUserId> {
    /// Set the Authorization scope to [`AuthzScope::User`] for a [`UserId`] in the service context.
    pub async fn set(Input(user_id): Input<UserId>) -> Result<AuthzScope, TdError> {
        Ok(AuthzScope::User(AuthzEntity::On(*user_id)))
    }
}

pub struct SystemOrRoleId {
    #[allow(dead_code)]
    instance_blocker: (),
}

impl AuthzOn<SystemOrRoleId> {
    /// Set the Authorization scope to [`AuthzScope::Role`] for a [`RoleId`] in the service context.
    pub async fn set(Input(role_id): Input<RoleId>) -> Result<AuthzScope, TdError> {
        Ok(AuthzScope::Role(AuthzEntity::On(*role_id)))
    }
}

/// Enum with all defined permissions.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Permission {
    /// Requester permission (this permission does not go into Permissions Table).
    User(AuthzEntity<UserId>),
    /// Current role permission (this permission does not go into Permissions Table).
    Role(AuthzEntity<RoleId>),
    /// System wide operations, collections creation and deletion.
    SysAdmin,
    /// User management, role management.
    SecAdmin,
    /// Collection permission management, assigning removing roles from a collection.
    CollectionAdmin(AuthzEntity<CollectionId>), //collection admin
    /// Creating, Updating and deleting functions in a collection.
    CollectionDev(AuthzEntity<CollectionId>), //CRUD functions
    /// Execution management in a collection (trigger, cancel, recover).
    CollectionExec(AuthzEntity<CollectionId>), //trigger functions
    /// Accessing public tables (schema and data) in a collection.
    CollectionRead(AuthzEntity<CollectionId>), //read (public) tables
}

impl Permission {
    /// Return if the permission is a collection permission.
    pub fn is_on_collection(&self) -> bool {
        matches!(
            self,
            Permission::CollectionAdmin(_)
                | Permission::CollectionDev(_)
                | Permission::CollectionExec(_)
                | Permission::CollectionRead(_)
        )
    }

    /// Return if the permission is a collection permission and applies to all collections.
    pub fn is_on_all_collections(&self) -> bool {
        matches!(
            self,
            Permission::CollectionAdmin(AuthzEntity::All)
                | Permission::CollectionDev(AuthzEntity::All)
                | Permission::CollectionExec(AuthzEntity::All)
                | Permission::CollectionRead(AuthzEntity::All)
        )
    }

    pub fn on_collection(&self) -> Option<CollectionId> {
        match self {
            Permission::CollectionAdmin(AuthzEntity::On(collection_id))
            | Permission::CollectionDev(AuthzEntity::On(collection_id))
            | Permission::CollectionExec(AuthzEntity::On(collection_id))
            | Permission::CollectionRead(AuthzEntity::On(collection_id)) => Some(*collection_id),
            Permission::CollectionAdmin(AuthzEntity::All)
            | Permission::CollectionDev(AuthzEntity::All)
            | Permission::CollectionExec(AuthzEntity::All)
            | Permission::CollectionRead(AuthzEntity::All) => Some(CollectionId::all_collections()),
            _ => None,
        }
    }

    /// Return if the given permission is the same permission type as [`self`].
    pub fn is_same_type(&self, other: &Self) -> bool {
        mem::discriminant(self) == mem::discriminant(other)
    }
}

/// Trait that provides the required permissions to use a service.
pub trait AuthzRequirements {
    /// Return the required permissions for the given [`AuthzScope`].
    fn any_of(scope: &AuthzScope) -> Result<Option<HashSet<Permission>>, TdError>;
}

/// No required permissions.
#[derive(Debug)]
pub struct NoPermissions {
    #[allow(dead_code)]
    instance_blocker: (),
}

impl AuthzRequirements for NoPermissions {
    fn any_of(_scope: &AuthzScope) -> Result<Option<HashSet<Permission>>, TdError> {
        Ok(None)
    }
}

/// For inter collection check.
#[derive(Debug)]
pub struct InterColl {
    #[allow(dead_code)]
    instance_blocker: (),
}

impl AuthzRequirements for InterColl {
    fn any_of(_scope: &AuthzScope) -> Result<Option<HashSet<Permission>>, TdError> {
        panic!("InterColl should not be used as a permission requirement, it is only used for inter collection access checks");
    }
}

/// Requester permission.
#[derive(Debug)]
pub struct Requester {
    #[allow(dead_code)]
    instance_blocker: (),
}

impl AuthzRequirements for Requester {
    fn any_of(scope: &AuthzScope) -> Result<Option<HashSet<Permission>>, TdError> {
        match scope {
            AuthzScope::System => Err(AuthzError::InvalidAuthzScope(
                "Requester".to_string(),
                AuthzScope::System.to_str().to_string(),
            ))?,
            AuthzScope::User(authz_on) => {
                Ok(Some(HashSet::from([Permission::User(authz_on.clone())])))
            }
            AuthzScope::Role(authz_on) => {
                Ok(Some(HashSet::from([Permission::Role(authz_on.clone())])))
            }
            AuthzScope::SystemOrUser(authz_on) => {
                Ok(Some(HashSet::from([Permission::User(authz_on.clone())])))
            }
            AuthzScope::SystemOrRole(authz_on) => {
                Ok(Some(HashSet::from([Permission::Role(authz_on.clone())])))
            }
            AuthzScope::Collection(authz_on) => Err(AuthzError::AuthEntityCannotBeAll(
                authz_on.to_string(),
                AuthzEntity::<CollectionId>::All.to_str().to_string(),
            ))?,
        }
    }
}

/// System admin permission.
#[derive(Debug)]
pub struct SysAdmin {
    #[allow(dead_code)]
    instance_blocker: (),
}

impl AuthzRequirements for SysAdmin {
    fn any_of(_scope: &AuthzScope) -> Result<Option<HashSet<Permission>>, TdError> {
        Ok(Some(HashSet::from([Permission::SysAdmin])))
    }
}

/// Security admin permission
#[derive(Debug)]
pub struct SecAdmin {
    #[allow(dead_code)]
    instance_blocker: (),
}

impl AuthzRequirements for SecAdmin {
    fn any_of(_scope: &AuthzScope) -> Result<Option<HashSet<Permission>>, TdError> {
        Ok(Some(HashSet::from([Permission::SecAdmin])))
    }
}

fn collection_any_of<R: AuthzRequirements>(
    scope: &AuthzScope,
    permission_creator: fn(&CollectionId) -> HashSet<Permission>,
) -> Result<Option<HashSet<Permission>>, TdError> {
    match scope {
        AuthzScope::System => Err(AuthzError::InvalidAuthzScope(
            type_name::<R>().to_owned(),
            AuthzScope::System.to_str().to_string(),
        ))?,
        AuthzScope::User(authz_on) => Err(AuthzError::InvalidAuthzScope(
            type_name::<R>().to_owned(),
            AuthzScope::User(authz_on.clone()).to_str().to_string(),
        ))?,
        AuthzScope::Role(authz_on) => Err(AuthzError::InvalidAuthzScope(
            type_name::<R>().to_owned(),
            AuthzScope::Role(authz_on.clone()).to_str().to_string(),
        ))?,
        AuthzScope::SystemOrUser(authz_on) => Err(AuthzError::InvalidAuthzScope(
            type_name::<R>().to_owned(),
            AuthzScope::User(authz_on.clone()).to_str().to_string(),
        ))?,
        AuthzScope::SystemOrRole(authz_on) => Err(AuthzError::InvalidAuthzScope(
            type_name::<R>().to_owned(),
            AuthzScope::Role(authz_on.clone()).to_str().to_string(),
        ))?,
        AuthzScope::Collection(AuthzEntity::On(collection_id)) => {
            Ok(Some(permission_creator(collection_id)))
        }
        AuthzScope::Collection(AuthzEntity::All) => Err(AuthzError::AuthEntityCannotBeAll(
            type_name::<R>().to_owned(),
            AuthzEntity::<CollectionId>::All.to_str().to_string(),
        ))?,
    }
}

/// Collection admin permission.
#[derive(Debug)]
pub struct CollAdmin {
    #[allow(dead_code)]
    instance_blocker: (),
}

impl AuthzRequirements for CollAdmin {
    fn any_of(scope: &AuthzScope) -> Result<Option<HashSet<Permission>>, TdError> {
        match scope {
            AuthzScope::System | AuthzScope::SystemOrUser(_) => {
                Ok(Some(HashSet::from([Permission::CollectionAdmin(
                    AuthzEntity::All,
                )])))
            }
            other => collection_any_of::<Self>(other, |collection_id| {
                HashSet::from([Permission::CollectionAdmin(AuthzEntity::On(*collection_id))])
            }),
        }
    }
}

/// Collection development permission.
#[derive(Debug)]
pub struct CollDev {
    #[allow(dead_code)]
    instance_blocker: (),
}

impl AuthzRequirements for CollDev {
    fn any_of(scope: &AuthzScope) -> Result<Option<HashSet<Permission>>, TdError> {
        collection_any_of::<Self>(scope, |collection_id| {
            HashSet::from([Permission::CollectionDev(AuthzEntity::On(*collection_id))])
        })
    }
}

/// Collection execution permission.
#[derive(Debug)]
pub struct CollExec {
    #[allow(dead_code)]
    instance_blocker: (),
}

impl AuthzRequirements for CollExec {
    fn any_of(scope: &AuthzScope) -> Result<Option<HashSet<Permission>>, TdError> {
        collection_any_of::<Self>(scope, |collection_id| {
            HashSet::from([Permission::CollectionExec(AuthzEntity::On(*collection_id))])
        })
    }
}

/// Collection read public tables permission.
#[derive(Debug)]
pub struct CollRead {
    #[allow(dead_code)]
    instance_blocker: (),
}

impl AuthzRequirements for CollRead {
    fn any_of(scope: &AuthzScope) -> Result<Option<HashSet<Permission>>, TdError> {
        collection_any_of::<Self>(scope, |collection_id| {
            HashSet::from([Permission::CollectionRead(AuthzEntity::On(*collection_id))])
        })
    }
}

/// Internally used to add [`AuthzEntity::All`] to the required permissions for
/// a given [`AuthzScope`] to match current permissions that have wildcards.
fn augment_with_wildcards(required_permissions: HashSet<Permission>) -> HashSet<Permission> {
    let mut with_wildcards = required_permissions.clone();
    for perm in required_permissions {
        match perm {
            Permission::CollectionAdmin(AuthzEntity::On(_)) => {
                with_wildcards.insert(Permission::CollectionAdmin(AuthzEntity::All));
            }
            Permission::CollectionDev(AuthzEntity::On(_)) => {
                with_wildcards.insert(Permission::CollectionDev(AuthzEntity::All));
            }
            Permission::CollectionExec(AuthzEntity::On(_)) => {
                with_wildcards.insert(Permission::CollectionExec(AuthzEntity::All));
            }
            Permission::CollectionRead(AuthzEntity::On(_)) => {
                with_wildcards.insert(Permission::CollectionRead(AuthzEntity::All));
            }
            _ => {}
        }
    }
    with_wildcards
}

/// Service Authorization enforcer.
pub struct Authz<
    AC: AuthzContextT,
    C1: AuthzRequirements,
    C2: AuthzRequirements = NoPermissions,
    C3: AuthzRequirements = NoPermissions,
    C4: AuthzRequirements = NoPermissions,
    C5: AuthzRequirements = NoPermissions,
    C6: AuthzRequirements = NoPermissions,
    C7: AuthzRequirements = NoPermissions,
> {
    _ac: PhantomData<AC>,
    _c1: PhantomData<C1>,
    _c2: PhantomData<C2>,
    _c3: PhantomData<C3>,
    _c4: PhantomData<C4>,
    _c5: PhantomData<C5>,
    _c6: PhantomData<C6>,
    _c7: PhantomData<C7>,
}

impl<
        AC: AuthzContextT,
        C1: AuthzRequirements,
        C2: AuthzRequirements,
        C3: AuthzRequirements,
        C4: AuthzRequirements,
        C5: AuthzRequirements,
        C6: AuthzRequirements,
        C7: AuthzRequirements,
    > Authz<AC, C1, C2, C3, C4, C5, C6, C7>
{
    /// Perform authorization check for the given [`AuthScope`]
    /// (set via [`AuthzOn<System>::set`] or [`AuthzOn<CollectionId>::set`].
    ///
    /// The generics fixed on the function define the type of required permissions
    /// from [`AuthzScope`] in the service context.
    ///
    /// The user role is obtained from the [`RequestContext`] in the service context.
    ///
    /// The role permissions are from the [`AuthzContextT`] in the service context.
    pub async fn check(
        SrvCtx(authz_context): SrvCtx<AC>,
        Connection(conn): Connection,
        Input(request_context): Input<RequestContext>,
        Input(scope): Input<AuthzScope>,
    ) -> Result<(), TdError> {
        let mut required_permissions = HashSet::new();
        if let Some(permissions) = C1::any_of(&scope)? {
            required_permissions.extend(permissions)
        }
        if let Some(permissions) = C2::any_of(&scope)? {
            required_permissions.extend(permissions)
        }
        if let Some(permissions) = C3::any_of(&scope)? {
            required_permissions.extend(permissions)
        }
        if let Some(permissions) = C4::any_of(&scope)? {
            required_permissions.extend(permissions)
        }
        if let Some(permissions) = C5::any_of(&scope)? {
            required_permissions.extend(permissions)
        }
        if let Some(permissions) = C6::any_of(&scope)? {
            required_permissions.extend(permissions)
        }
        if let Some(permissions) = C7::any_of(&scope)? {
            required_permissions.extend(permissions)
        }

        // if a authz requires a collection permissions, the specific collection or the wildcard collection
        // are a match, thus, we augment the required permissions with the wildcard permissions before
        // checking against the available permissions.
        let required_permissions = augment_with_wildcards(required_permissions);

        if required_permissions.is_empty() {
            Ok(())
        } else {
            let mut conn = conn.lock().await;
            let conn = conn.get_mut_connection()?;
            if let Some(role_permissions) = authz_context
                .role_permissions(conn, request_context.role_id())
                .await?
            {
                for perm in role_permissions.deref() {
                    if required_permissions.contains(perm) {
                        return Ok(());
                    }
                }

                // scope: System or SystemUser, CollectionAdmin required permission
                #[allow(clippy::collapsible_if)]
                if matches!(
                    scope.deref(),
                    &AuthzScope::System | &AuthzScope::SystemOrUser(_)
                ) {
                    #[allow(clippy::collapsible_if)]
                    if required_permissions.contains(&Permission::CollectionAdmin(AuthzEntity::All))
                    {
                        if role_permissions
                            .iter()
                            .any(|perm| matches!(*perm, Permission::CollectionAdmin(_)))
                        {
                            return Ok(());
                        }
                    }
                }
            }

            // scope: User, SystemOrUserId, Requester required permission
            let user_id: UserId = *request_context.user_id();
            if required_permissions.contains(&Permission::User(AuthzEntity::On(user_id))) {
                return Ok(());
            }

            // scope: Role, SystemOrRoleId, Requester required permission
            let role_id: RoleId = *request_context.role_id();
            if required_permissions.contains(&Permission::Role(AuthzEntity::On(role_id))) {
                return Ok(());
            }

            Err(AuthzError::Forbidden(scope.to_string()))?
        }
    }

    pub async fn check_inter_collection(
        SrvCtx(authz_context): SrvCtx<AC>,
        Connection(conn): Connection,
        Input(inter_collection_access_list): Input<Vec<InterCollectionAccess>>,
    ) -> Result<(), TdError> {
        let mut conn = conn.lock().await;
        let conn = conn.get_mut_connection()?;

        // within the same collection access is always allowed, so we filter them out
        let inter_collection_access_list = inter_collection_access_list
            .deref()
            .iter()
            .filter(|access| access.source().deref() != access.target().deref())
            .collect::<HashSet<_>>();
        if inter_collection_access_list.is_empty() {
            return Ok(());
        }
        let mut no_access = vec![];
        for inter_collection_access in inter_collection_access_list {
            if let Some(collections) = authz_context
                .inter_collections_permissions_value_can_read_key(
                    conn,
                    inter_collection_access.source(),
                )
                .await?
            {
                if !collections
                    .deref()
                    .contains(inter_collection_access.target())
                {
                    no_access.push(inter_collection_access);
                }
            } else {
                no_access.push(inter_collection_access);
            }
        }
        if no_access.is_empty() {
            return Ok(());
        }
        let collection_ids = no_access
            .iter()
            .flat_map(|access| vec![access.source().deref(), access.target().deref()])
            .map(CollectionId::from)
            .collect::<Vec<_>>();
        let collection_ids = collection_ids.iter().collect::<Vec<_>>();
        let collections: Vec<CollectionDB> = DaoQueries::default()
            .find_by::<CollectionDB>(&collection_ids)?
            .build_query_as()
            .fetch_all(&mut *conn)
            .await
            .map_err(handle_sql_err)?;

        let collection_id_name_map: HashMap<_, _> = collections
            .into_iter()
            .map(|collection| (**collection.id(), collection.name().to_string()))
            .collect();
        let no_access = no_access
            .into_iter()
            .map(|access| {
                format!(
                    "collection '{}' cannot access collection '{}'",
                    collection_id_name_map
                        .get(access.target().deref())
                        .unwrap_or(&"<Unknown>".to_string()),
                    collection_id_name_map
                        .get(access.source().deref())
                        .unwrap_or(&"<Unknown>".to_string()),
                )
            })
            .join(", ");
        Err(AuthzError::ForbiddenInterCollectionAccess(no_access))?
    }

    pub async fn visible_collections(
        SrvCtx(authz_context): SrvCtx<AC>,
        Connection(conn): Connection,
        Input(request_context): Input<RequestContext>,
    ) -> Result<VisibleCollections, TdError> {
        // TODO use C1, C2, etc??
        let mut conn = conn.lock().await;
        let conn = conn.get_mut_connection()?;
        authz_context
            .visible_collections(conn, request_context.role_id())
            .await
    }
}

#[cfg(test)]
mod tests {
    use crate::crudl::RequestContext;
    use crate::tower_service::authz::{
        AuthzContextT, AuthzEntity, AuthzError, AuthzRequirements, AuthzScope, CollAdmin, CollDev,
        CollExec, CollRead, InterColl, NoPermissions, Permission, Requester, SecAdmin, SysAdmin,
    };
    use crate::types::basic::{AccessTokenId, CollectionId, RoleId, ToCollectionId, UserId};
    use crate::types::permission::InterCollectionAccess;
    use async_trait::async_trait;
    use sqlx::SqliteConnection;
    use std::collections::HashMap;
    use std::marker::PhantomData;
    use std::sync::Arc;
    use td_common::id;
    use td_error::TdError;
    use td_tower::extractors::{Connection, ConnectionType, Input, SrvCtx};

    #[test]
    fn test_is_on_collection() {
        assert!(
            Permission::CollectionAdmin(AuthzEntity::On(CollectionId::default()))
                .is_on_collection()
        );
        assert!(
            Permission::CollectionDev(AuthzEntity::On(CollectionId::default())).is_on_collection()
        );
        assert!(
            Permission::CollectionExec(AuthzEntity::On(CollectionId::default())).is_on_collection()
        );
        assert!(
            Permission::CollectionRead(AuthzEntity::On(CollectionId::default())).is_on_collection()
        );

        assert!(Permission::CollectionAdmin(AuthzEntity::All).is_on_collection());
        assert!(Permission::CollectionDev(AuthzEntity::All).is_on_collection());
        assert!(Permission::CollectionExec(AuthzEntity::All).is_on_collection());
        assert!(Permission::CollectionRead(AuthzEntity::All).is_on_collection());

        assert!(!Permission::SysAdmin.is_on_collection());
        assert!(!Permission::SecAdmin.is_on_collection());
        assert!(!Permission::User(AuthzEntity::On(UserId::default())).is_on_collection());
    }

    #[test]
    fn test_is_on_all_collections() {
        assert!(
            !Permission::CollectionAdmin(AuthzEntity::On(CollectionId::default()))
                .is_on_all_collections()
        );
        assert!(
            !Permission::CollectionDev(AuthzEntity::On(CollectionId::default()))
                .is_on_all_collections()
        );
        assert!(
            !Permission::CollectionExec(AuthzEntity::On(CollectionId::default()))
                .is_on_all_collections()
        );
        assert!(
            !Permission::CollectionRead(AuthzEntity::On(CollectionId::default()))
                .is_on_all_collections()
        );

        assert!(Permission::CollectionAdmin(AuthzEntity::All).is_on_all_collections());
        assert!(Permission::CollectionDev(AuthzEntity::All).is_on_all_collections());
        assert!(Permission::CollectionExec(AuthzEntity::All).is_on_all_collections());
        assert!(Permission::CollectionRead(AuthzEntity::All).is_on_all_collections());

        assert!(!Permission::SysAdmin.is_on_all_collections());
        assert!(!Permission::SecAdmin.is_on_all_collections());
        assert!(!Permission::User(AuthzEntity::On(UserId::default())).is_on_all_collections());
    }

    #[test]
    fn test_is_same_type() {
        assert!(
            Permission::CollectionAdmin(AuthzEntity::On(CollectionId::default())).is_same_type(
                &Permission::CollectionAdmin(AuthzEntity::On(CollectionId::default()))
            )
        );
        assert!(Permission::CollectionAdmin(AuthzEntity::All).is_same_type(
            &Permission::CollectionAdmin(AuthzEntity::On(CollectionId::default()))
        ));
        assert!(Permission::SysAdmin.is_same_type(&Permission::SysAdmin));

        assert!(
            !Permission::CollectionAdmin(AuthzEntity::On(CollectionId::default())).is_same_type(
                &Permission::CollectionDev(AuthzEntity::On(CollectionId::default()))
            )
        );
        assert!(!Permission::CollectionAdmin(AuthzEntity::All).is_same_type(
            &Permission::CollectionDev(AuthzEntity::On(CollectionId::default()))
        ));
        assert!(
            !Permission::CollectionAdmin(AuthzEntity::On(CollectionId::default()))
                .is_same_type(&Permission::SysAdmin)
        );
        assert!(!Permission::SysAdmin.is_same_type(&Permission::SecAdmin));
    }

    #[derive(Debug)]
    struct AuthzContextForTest {
        role_permissions_map: HashMap<RoleId, Arc<Vec<Permission>>>,
        inter_collections_permissions_value_can_read_key:
            HashMap<CollectionId, Arc<Vec<ToCollectionId>>>,
        inter_collections_permissions_key_can_read_value:
            HashMap<ToCollectionId, Arc<Vec<CollectionId>>>,
    }

    impl AuthzContextForTest {
        pub fn add_permissions(
            mut self,
            role: impl Into<RoleId>,
            permissions: impl Into<Vec<Permission>>,
        ) -> Self {
            self.role_permissions_map
                .insert(role.into(), Arc::new(permissions.into()));
            self
        }

        pub fn remove_permissions(mut self, role: &RoleId) -> Self {
            self.role_permissions_map.remove(role);
            self
        }

        pub fn add_inter_collection_permission(
            mut self,
            source: &CollectionId,
            target: &ToCollectionId,
        ) -> Self {
            let entry = self
                .inter_collections_permissions_value_can_read_key
                .entry(*source)
                .or_insert_with(|| Arc::new(Vec::new()));
            let entry_vec = Arc::make_mut(entry);
            if !entry_vec.contains(target) {
                entry_vec.push(*target);
            }

            let entry = self
                .inter_collections_permissions_key_can_read_value
                .entry(*target)
                .or_insert_with(|| Arc::new(Vec::new()));
            let entry_vec = Arc::make_mut(entry);
            if !entry_vec.contains(source) {
                entry_vec.push(*source);
            }

            self
        }

        pub fn default() -> Self {
            Self {
                role_permissions_map: HashMap::new(),
                inter_collections_permissions_value_can_read_key: HashMap::new(),
                inter_collections_permissions_key_can_read_value: HashMap::new(),
            }
            .add_permissions(
                RoleId::sys_admin(),
                [
                    Permission::SysAdmin,
                    Permission::SecAdmin,
                    Permission::CollectionAdmin(AuthzEntity::All),
                    Permission::CollectionDev(AuthzEntity::All),
                    Permission::CollectionExec(AuthzEntity::All),
                    Permission::CollectionRead(AuthzEntity::All),
                ],
            )
            .add_permissions(
                RoleId::sec_admin(),
                [
                    Permission::SecAdmin,
                    Permission::CollectionAdmin(AuthzEntity::All),
                ],
            )
            .add_permissions(
                RoleId::user(),
                [
                    Permission::CollectionDev(AuthzEntity::All),
                    Permission::CollectionExec(AuthzEntity::All),
                    Permission::CollectionRead(AuthzEntity::All),
                ],
            )
        }
    }

    #[async_trait]
    impl AuthzContextT for AuthzContextForTest {
        async fn role_permissions(
            &self,
            _conn: &mut SqliteConnection,
            role: &RoleId,
        ) -> Result<Option<Arc<Vec<Permission>>>, TdError> {
            Ok(self.role_permissions_map.get(role).map(Arc::clone))
        }

        async fn inter_collections_permissions_value_can_read_key(
            &self,
            _conn: &mut SqliteConnection,
            collection_id: &CollectionId,
        ) -> Result<Option<Arc<Vec<ToCollectionId>>>, TdError> {
            Ok(self
                .inter_collections_permissions_value_can_read_key
                .get(collection_id)
                .map(Arc::clone))
        }

        async fn inter_collections_permissions_key_can_read_value(
            &self,
            _conn: &mut SqliteConnection,
            collection_id: &ToCollectionId,
        ) -> Result<Option<Arc<Vec<CollectionId>>>, TdError> {
            Ok(self
                .inter_collections_permissions_key_can_read_value
                .get(collection_id)
                .map(Arc::clone))
        }
    }

    // This is how it should be done in a module fixing the AuthzContext to a concrete impl
    type Authz<
        C1,
        C2 = NoPermissions,
        C3 = NoPermissions,
        C4 = NoPermissions,
        C5 = NoPermissions,
        C6 = NoPermissions,
        C7 = NoPermissions,
    > = super::Authz<AuthzContextForTest, C1, C2, C3, C4, C5, C6, C7>;

    impl<
            C1: AuthzRequirements,
            C2: AuthzRequirements,
            C3: AuthzRequirements,
            C4: AuthzRequirements,
            C5: AuthzRequirements,
            C6: AuthzRequirements,
            C7: AuthzRequirements,
        > Authz<C1, C2, C3, C4, C5, C6, C7>
    {
        fn new() -> Self {
            Self {
                _ac: PhantomData,
                _c1: PhantomData,
                _c2: PhantomData,
                _c3: PhantomData,
                _c4: PhantomData,
                _c5: PhantomData,
                _c6: PhantomData,
                _c7: PhantomData,
            }
        }
    }

    async fn assert_ok<
        C1: AuthzRequirements,
        C2: AuthzRequirements,
        C3: AuthzRequirements,
        C4: AuthzRequirements,
        C5: AuthzRequirements,
        C6: AuthzRequirements,
        C7: AuthzRequirements,
    >(
        authz_context: &Arc<AuthzContextForTest>,
        request_context: &Arc<RequestContext>,
        scope: &Arc<AuthzScope>,
        _authz: Authz<C1, C2, C3, C4, C5, C6, C7>,
    ) {
        let db = td_database::test_utils::db().await.unwrap();
        let conn = db.acquire().await.unwrap();
        let conn = ConnectionType::PoolConnection(conn).into();
        let conn = Connection::new(conn);
        let res = Authz::<C1, C2, C3, C4, C5, C6, C7>::check(
            SrvCtx(authz_context.clone()),
            conn,
            Input(request_context.clone()),
            Input(scope.clone()),
        )
        .await;
        match res {
            Ok(()) => {}
            Err(err) => {
                panic!("Check failed with {err:?}");
            }
        }
    }

    async fn assert_error<
        C1: AuthzRequirements,
        C2: AuthzRequirements,
        C3: AuthzRequirements,
        C4: AuthzRequirements,
        C5: AuthzRequirements,
        C6: AuthzRequirements,
        C7: AuthzRequirements,
    >(
        authz_context: &Arc<AuthzContextForTest>,
        request_context: &Arc<RequestContext>,
        scope: &Arc<AuthzScope>,
        _authz: Authz<C1, C2, C3, C4, C5, C6, C7>,
        expected_err: AuthzError,
    ) {
        let db = td_database::test_utils::db().await.unwrap();
        let conn = db.acquire().await.unwrap();
        let conn = ConnectionType::PoolConnection(conn).into();
        let conn = Connection::new(conn);
        let res = Authz::<C1, C2, C3, C4, C5, C6, C7>::check(
            SrvCtx(authz_context.clone()),
            conn,
            Input(request_context.clone()),
            Input(scope.clone()),
        )
        .await;
        match res {
            Ok(()) => {
                panic!("Check passed, it should have failed with {expected_err:?}");
            }
            Err(err) => {
                let authz_err: &AuthzError = err.domain_err();
                assert_eq!(
                    std::mem::discriminant(&expected_err),
                    std::mem::discriminant(authz_err),
                    "Expected {expected_err}, Got {authz_err}"
                );
            }
        }
    }

    #[tokio::test]
    async fn test_default_roles_and_permissions() {
        let authz_context = AuthzContextForTest::default();

        let sys_admin_context = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sys_admin(),
            true,
        );
        let sec_admin_context = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sec_admin(),
            true,
        );
        let user_context = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
            false,
        );

        let system_scope = Arc::new(AuthzScope::System);

        let collection_scope = Arc::new(AuthzScope::Collection(AuthzEntity::On(id::id().into())));

        let authz_context = Arc::new(authz_context);

        // sys_admin role
        let scope = &system_scope;
        let request_context = Arc::new(sys_admin_context);
        assert_ok(
            &authz_context,
            &request_context,
            scope,
            Authz::<SysAdmin>::new(),
        )
        .await;
        assert_ok(
            &authz_context,
            &request_context,
            scope,
            Authz::<SecAdmin>::new(),
        )
        .await;
        let scope = &collection_scope;
        assert_ok(
            &authz_context,
            &request_context,
            scope,
            Authz::<CollAdmin>::new(),
        )
        .await;
        assert_ok(
            &authz_context,
            &request_context,
            scope,
            Authz::<CollDev>::new(),
        )
        .await;
        assert_ok(
            &authz_context,
            &request_context,
            scope,
            Authz::<CollExec>::new(),
        )
        .await;
        assert_ok(
            &authz_context,
            &request_context,
            scope,
            Authz::<CollRead>::new(),
        )
        .await;

        // sec_admin role
        let request_context = Arc::new(sec_admin_context);
        let scope = &system_scope;
        assert_error(
            &authz_context,
            &request_context,
            scope,
            Authz::<SysAdmin>::new(),
            AuthzError::Forbidden("".to_string()),
        )
        .await;
        assert_ok(
            &authz_context,
            &request_context,
            scope,
            Authz::<SecAdmin>::new(),
        )
        .await;
        let scope = &collection_scope;
        assert_ok(
            &authz_context,
            &request_context,
            scope,
            Authz::<CollAdmin>::new(),
        )
        .await;
        assert_error(
            &authz_context,
            &request_context,
            scope,
            Authz::<CollDev>::new(),
            AuthzError::Forbidden("".to_string()),
        )
        .await;
        assert_error(
            &authz_context,
            &request_context,
            scope,
            Authz::<CollExec>::new(),
            AuthzError::Forbidden("".to_string()),
        )
        .await;
        assert_error(
            &authz_context,
            &request_context,
            scope,
            Authz::<CollRead>::new(),
            AuthzError::Forbidden("".to_string()),
        )
        .await;

        // user role
        let request_context = Arc::new(user_context);
        let scope = &system_scope;
        assert_error(
            &authz_context,
            &request_context,
            scope,
            Authz::<SysAdmin>::new(),
            AuthzError::Forbidden("".to_string()),
        )
        .await;
        assert_error(
            &authz_context,
            &request_context,
            scope,
            Authz::<SecAdmin>::new(),
            AuthzError::Forbidden("".to_string()),
        )
        .await;
        let scope = &collection_scope;
        assert_error(
            &authz_context,
            &request_context,
            scope,
            Authz::<CollAdmin>::new(),
            AuthzError::Forbidden("".to_string()),
        )
        .await;
        assert_ok(
            &authz_context,
            &request_context,
            scope,
            Authz::<CollDev>::new(),
        )
        .await;
        assert_ok(
            &authz_context,
            &request_context,
            scope,
            Authz::<CollExec>::new(),
        )
        .await;
        assert_ok(
            &authz_context,
            &request_context,
            scope,
            Authz::<CollRead>::new(),
        )
        .await;
    }

    #[tokio::test]
    async fn test_multiple_any_of_all_avail() {
        let authz_context = AuthzContextForTest::default();

        let request_context = Arc::new(RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sys_admin(),
            false,
        ));

        let scope = Arc::new(AuthzScope::System);

        let authz_context = Arc::new(authz_context);

        // sys_admin role
        assert_ok(
            &authz_context,
            &request_context,
            &scope,
            Authz::<SysAdmin, SecAdmin>::new(),
        )
        .await;
    }

    #[tokio::test]
    async fn test_multiple_any_of_one_avail() {
        let authz_context = AuthzContextForTest::default();

        let request_context = Arc::new(RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sec_admin(),
            false,
        ));

        let scope = Arc::new(AuthzScope::System);

        let authz_context = Arc::new(authz_context);

        // sys_admin role
        assert_ok(
            &authz_context,
            &request_context,
            &scope,
            Authz::<SysAdmin, SecAdmin>::new(),
        )
        .await;
    }

    #[tokio::test]
    async fn test_multiple_any_of_none_avail() {
        let authz_context = AuthzContextForTest::default();

        let request_context = Arc::new(RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
            false,
        ));

        let scope = Arc::new(AuthzScope::System);

        let authz_context = Arc::new(authz_context);

        // sys_admin role
        assert_error(
            &authz_context,
            &request_context,
            &scope,
            Authz::<SysAdmin, SecAdmin>::new(),
            AuthzError::Forbidden("".to_string()),
        )
        .await;
    }

    #[tokio::test]
    async fn test_collections_all_and_one() {
        let collection0 = CollectionId::default();
        let collection1 = CollectionId::default();
        let all_collections = RoleId::default();
        let one_collection = RoleId::default();

        let authz_context = AuthzContextForTest::default()
            .remove_permissions(&RoleId::user())
            .add_permissions(
                all_collections,
                [Permission::CollectionRead(AuthzEntity::All)],
            )
            .add_permissions(
                one_collection,
                [Permission::CollectionRead(AuthzEntity::On(collection0))],
            );
        let authz_context = Arc::new(authz_context);

        // role with permission granted on all collections
        let request_context = Arc::new(RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            all_collections,
            false,
        ));
        let scope = Arc::new(AuthzScope::Collection(AuthzEntity::On(collection0)));
        assert_ok(
            &authz_context,
            &request_context,
            &scope,
            Authz::<CollRead>::new(),
        )
        .await;
        let scope = Arc::new(AuthzScope::Collection(AuthzEntity::On(collection1)));
        assert_ok(
            &authz_context,
            &request_context,
            &scope,
            Authz::<CollRead>::new(),
        )
        .await;

        // role with permission granted on one collection
        let request_context = Arc::new(RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            one_collection,
            false,
        ));
        let scope = Arc::new(AuthzScope::Collection(AuthzEntity::On(collection0)));
        assert_ok(
            &authz_context,
            &request_context,
            &scope,
            Authz::<CollRead>::new(),
        )
        .await;
        let scope = Arc::new(AuthzScope::Collection(AuthzEntity::On(collection1)));
        assert_error(
            &authz_context,
            &request_context,
            &scope,
            Authz::<CollRead>::new(),
            AuthzError::Forbidden("".to_string()),
        )
        .await;
    }

    #[tokio::test]
    async fn test_any_of_generics_1_to_7() {
        let collection = CollectionId::default();
        let role = RoleId::default();

        let authz_context = AuthzContextForTest::default()
            .remove_permissions(&RoleId::user())
            .add_permissions(role, [Permission::CollectionRead(AuthzEntity::All)]);
        let authz_context = Arc::new(authz_context);

        // positive
        let request_context = Arc::new(RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            role,
            true,
        ));
        let scope = Arc::new(AuthzScope::Collection(AuthzEntity::On(collection)));
        assert_ok(
            &authz_context,
            &request_context,
            &scope,
            Authz::<CollRead>::new(),
        )
        .await;
        assert_ok(
            &authz_context,
            &request_context,
            &scope,
            Authz::<NoPermissions, CollRead>::new(),
        )
        .await;
        assert_ok(
            &authz_context,
            &request_context,
            &scope,
            Authz::<NoPermissions, NoPermissions, CollRead>::new(),
        )
        .await;
        assert_ok(
            &authz_context,
            &request_context,
            &scope,
            Authz::<NoPermissions, NoPermissions, NoPermissions, CollRead>::new(),
        )
        .await;
        assert_ok(
            &authz_context,
            &request_context,
            &scope,
            Authz::<NoPermissions, NoPermissions, NoPermissions, NoPermissions, CollRead>::new(),
        )
        .await;
        assert_ok(
            &authz_context,
            &request_context,
            &scope,
            Authz::<
                NoPermissions,
                NoPermissions,
                NoPermissions,
                NoPermissions,
                NoPermissions,
                CollRead,
            >::new(),
        )
        .await;
        assert_ok(
            &authz_context,
            &request_context,
            &scope,
            Authz::<
                NoPermissions,
                NoPermissions,
                NoPermissions,
                NoPermissions,
                NoPermissions,
                NoPermissions,
                CollRead,
            >::new(),
        )
        .await;

        // negative

        let request_context = Arc::new(RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            role,
            true,
        ));
        let scope = Arc::new(AuthzScope::Collection(AuthzEntity::On(collection)));
        assert_error(
            &authz_context,
            &request_context,
            &scope,
            Authz::<CollExec>::new(),
            AuthzError::Forbidden("".to_string()),
        )
        .await;
        assert_error(
            &authz_context,
            &request_context,
            &scope,
            Authz::<NoPermissions, CollExec>::new(),
            AuthzError::Forbidden("".to_string()),
        )
        .await;
        assert_error(
            &authz_context,
            &request_context,
            &scope,
            Authz::<NoPermissions, NoPermissions, CollExec>::new(),
            AuthzError::Forbidden("".to_string()),
        )
        .await;
        assert_error(
            &authz_context,
            &request_context,
            &scope,
            Authz::<NoPermissions, NoPermissions, NoPermissions, CollExec>::new(),
            AuthzError::Forbidden("".to_string()),
        )
        .await;
        assert_error(
            &authz_context,
            &request_context,
            &scope,
            Authz::<NoPermissions, NoPermissions, NoPermissions, NoPermissions, CollExec>::new(),
            AuthzError::Forbidden("".to_string()),
        )
        .await;
        assert_error(
            &authz_context,
            &request_context,
            &scope,
            Authz::<
                NoPermissions,
                NoPermissions,
                NoPermissions,
                NoPermissions,
                NoPermissions,
                CollExec,
            >::new(),
            AuthzError::Forbidden("".to_string()),
        )
        .await;
        assert_error(
            &authz_context,
            &request_context,
            &scope,
            Authz::<
                NoPermissions,
                NoPermissions,
                NoPermissions,
                NoPermissions,
                NoPermissions,
                NoPermissions,
                CollExec,
            >::new(),
            AuthzError::Forbidden("".to_string()),
        )
        .await;
    }

    #[tokio::test]
    async fn test_collection_system_permission_on_collection_scope() {
        let collection = CollectionId::default();
        let role = RoleId::default();

        let authz_context = AuthzContextForTest::default().add_permissions(
            role,
            [
                Permission::SecAdmin,
                Permission::CollectionRead(AuthzEntity::All),
            ],
        );
        let authz_context = Arc::new(authz_context);

        let request_context = Arc::new(RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            role,
            false,
        ));
        let scope = Arc::new(AuthzScope::Collection(AuthzEntity::On(collection)));
        assert_ok(
            &authz_context,
            &request_context,
            &scope,
            Authz::<SecAdmin, CollRead>::new(),
        )
        .await;
    }

    #[tokio::test]
    async fn test_collection_system_permission_on_system_scope() {
        let role = RoleId::default();
        let authz_context = AuthzContextForTest::default().add_permissions(
            role,
            [
                Permission::SecAdmin,
                Permission::CollectionRead(AuthzEntity::All),
            ],
        );
        let authz_context = Arc::new(authz_context);

        let request_context = Arc::new(RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            role,
            false,
        ));
        let scope = Arc::new(AuthzScope::System);
        assert_error(
            &authz_context,
            &request_context,
            &scope,
            Authz::<SecAdmin, CollRead>::new(),
            AuthzError::InvalidAuthzScope("".to_string(), "".to_string()),
        )
        .await;
    }

    #[tokio::test]
    async fn test_collection_all_in_scope_error() {
        let role = RoleId::default();
        let authz_context = AuthzContextForTest::default().add_permissions(
            role,
            [
                Permission::SecAdmin,
                Permission::CollectionRead(AuthzEntity::All),
            ],
        );
        let authz_context = Arc::new(authz_context);

        let request_context = Arc::new(RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            role,
            false,
        ));
        let scope = Arc::new(AuthzScope::Collection(AuthzEntity::All));
        assert_error(
            &authz_context,
            &request_context,
            &scope,
            Authz::<SecAdmin, CollRead>::new(),
            AuthzError::AuthEntityCannotBeAll("".to_string(), "".to_string()),
        )
        .await;
    }

    #[tokio::test]
    async fn test_user_scope_ok() {
        let user = UserId::default();
        let role = RoleId::default();

        let authz_context = AuthzContextForTest::default().add_permissions(role, []);
        let authz_context = Arc::new(authz_context);

        let request_context = Arc::new(RequestContext::with(
            AccessTokenId::default(),
            user,
            role,
            false,
        ));

        let scope = Arc::new(AuthzScope::User(AuthzEntity::On(user)));

        assert_ok(
            &authz_context,
            &request_context,
            &scope,
            Authz::<Requester>::new(),
        )
        .await;
    }

    #[tokio::test]
    async fn test_user_scope_error() {
        let user = UserId::default();
        let role = RoleId::default();

        let authz_context = AuthzContextForTest::default().add_permissions(role, []);
        let authz_context = Arc::new(authz_context);

        // different user
        let request_context = Arc::new(RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            role,
            false,
        ));

        let scope = Arc::new(AuthzScope::User(AuthzEntity::On(user)));

        assert_error(
            &authz_context,
            &request_context,
            &scope,
            Authz::<Requester>::new(),
            AuthzError::Forbidden("".to_string()),
        )
        .await;

        let authz_context = AuthzContextForTest::default()
            .add_permissions(role, [Permission::SecAdmin, Permission::SysAdmin]);
        let authz_context = Arc::new(authz_context);

        // different user
        let request_context = Arc::new(RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            role,
            false,
        ));

        let scope = Arc::new(AuthzScope::User(AuthzEntity::On(user)));

        assert_error(
            &authz_context,
            &request_context,
            &scope,
            Authz::<Requester>::new(),
            AuthzError::Forbidden("".to_string()),
        )
        .await;
    }

    #[tokio::test]
    async fn test_role_scope_ok() {
        let user = UserId::default();
        let role = RoleId::default();

        let authz_context = AuthzContextForTest::default().add_permissions(role, []);
        let authz_context = Arc::new(authz_context);

        let request_context = Arc::new(RequestContext::with(
            AccessTokenId::default(),
            user,
            role,
            false,
        ));

        let scope = Arc::new(AuthzScope::Role(AuthzEntity::On(role)));

        assert_ok(
            &authz_context,
            &request_context,
            &scope,
            Authz::<Requester>::new(),
        )
        .await;
    }

    #[tokio::test]
    async fn test_role_scope_error() {
        let user = UserId::default();
        let role = RoleId::default();

        let authz_context = AuthzContextForTest::default().add_permissions(role, []);
        let authz_context = Arc::new(authz_context);

        // different user
        let request_context = Arc::new(RequestContext::with(
            AccessTokenId::default(),
            user,
            RoleId::default(),
            false,
        ));

        let scope = Arc::new(AuthzScope::Role(AuthzEntity::On(role)));

        assert_error(
            &authz_context,
            &request_context,
            &scope,
            Authz::<Requester>::new(),
            AuthzError::Forbidden("".to_string()),
        )
        .await;

        let authz_context = AuthzContextForTest::default()
            .add_permissions(role, [Permission::SecAdmin, Permission::SysAdmin]);
        let authz_context = Arc::new(authz_context);

        // different user
        let request_context = Arc::new(RequestContext::with(
            AccessTokenId::default(),
            user,
            RoleId::default(),
            false,
        ));

        let scope = Arc::new(AuthzScope::Role(AuthzEntity::On(role)));

        assert_error(
            &authz_context,
            &request_context,
            &scope,
            Authz::<Requester>::new(),
            AuthzError::Forbidden("".to_string()),
        )
        .await;
    }

    #[tokio::test]
    async fn test_system_user_scope_ok() {
        let user = UserId::default();
        let role = RoleId::default();

        // by role permission only

        let authz_context =
            AuthzContextForTest::default().add_permissions(role, [Permission::SecAdmin]);
        let authz_context = Arc::new(authz_context);

        let request_context = Arc::new(RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sys_admin(),
            true,
        ));

        let scope = Arc::new(AuthzScope::SystemOrUser(AuthzEntity::On(user)));

        assert_ok(
            &authz_context,
            &request_context,
            &scope,
            Authz::<Requester, SecAdmin>::new(),
        )
        .await;

        // by requester only

        let authz_context = AuthzContextForTest::default().add_permissions(role, []);
        let authz_context = Arc::new(authz_context);

        let request_context = Arc::new(RequestContext::with(
            AccessTokenId::default(),
            user,
            RoleId::sys_admin(),
            true,
        ));

        let scope = Arc::new(AuthzScope::SystemOrUser(AuthzEntity::On(user)));

        assert_ok(
            &authz_context,
            &request_context,
            &scope,
            Authz::<Requester, SecAdmin>::new(),
        )
        .await;
    }

    #[tokio::test]
    async fn test_system_user_scope_error() {
        let user = UserId::default();
        let role = RoleId::default();

        // by role permission only

        let authz_context =
            AuthzContextForTest::default().add_permissions(role, [Permission::SecAdmin]);
        let authz_context = Arc::new(authz_context);

        let request_context = Arc::new(RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            role,
            false,
        ));

        let scope = Arc::new(AuthzScope::SystemOrUser(AuthzEntity::On(user)));

        assert_error(
            &authz_context,
            &request_context,
            &scope,
            Authz::<Requester, SysAdmin>::new(),
            AuthzError::Forbidden("".to_string()),
        )
        .await;

        // by requester only

        let authz_context = AuthzContextForTest::default().add_permissions(role, []);
        let authz_context = Arc::new(authz_context);

        let request_context = Arc::new(RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            role,
            false,
        ));

        let scope = Arc::new(AuthzScope::SystemOrUser(AuthzEntity::On(user)));

        assert_error(
            &authz_context,
            &request_context,
            &scope,
            Authz::<Requester, SysAdmin>::new(),
            AuthzError::Forbidden("".to_string()),
        )
        .await;
    }

    #[tokio::test]
    async fn test_system_role_scope_ok() {
        let role0 = RoleId::default();
        let role1 = RoleId::default();

        // by role permission only

        let authz_context = AuthzContextForTest::default()
            .add_permissions(role0, [Permission::SysAdmin])
            .add_permissions(role1, []);
        let authz_context = Arc::new(authz_context);

        let request_context = Arc::new(RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            role0,
            false,
        ));

        let scope = Arc::new(AuthzScope::SystemOrRole(AuthzEntity::On(role1)));

        assert_ok(
            &authz_context,
            &request_context,
            &scope,
            Authz::<Requester, SysAdmin>::new(),
        )
        .await;

        // by requester only

        let authz_context = Arc::new(authz_context);

        let request_context = Arc::new(RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            role1,
            true,
        ));

        let scope = Arc::new(AuthzScope::SystemOrRole(AuthzEntity::On(role1)));

        assert_ok(
            &authz_context,
            &request_context,
            &scope,
            Authz::<Requester, SecAdmin>::new(),
        )
        .await;
    }

    #[tokio::test]
    async fn test_system_role_scope_error() {
        let role0 = RoleId::default();
        let role1 = RoleId::default();

        // by role permission only

        let authz_context = AuthzContextForTest::default()
            .add_permissions(role0, [Permission::SecAdmin])
            .add_permissions(role1, []);
        let authz_context = Arc::new(authz_context);

        let request_context = Arc::new(RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            role0,
            false,
        ));

        let scope = Arc::new(AuthzScope::SystemOrRole(AuthzEntity::On(role1)));

        assert_error(
            &authz_context,
            &request_context,
            &scope,
            Authz::<Requester, SysAdmin>::new(),
            AuthzError::Forbidden("".to_string()),
        )
        .await;

        // by requester only

        let authz_context = Arc::new(authz_context);

        let request_context = Arc::new(RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::default(),
            true,
        ));

        let scope = Arc::new(AuthzScope::SystemOrRole(AuthzEntity::On(role1)));

        assert_error(
            &authz_context,
            &request_context,
            &scope,
            Authz::<Requester, SysAdmin>::new(),
            AuthzError::Forbidden("".to_string()),
        )
        .await;
    }

    #[tokio::test]
    async fn test_role_collections_permissions() {
        let db = td_database::test_utils::db().await.unwrap();
        let mut conn = db.acquire().await.unwrap();

        let role = RoleId::default();
        let sys_perm = Permission::SecAdmin;
        let perm_on_collection_1 =
            Permission::CollectionRead(AuthzEntity::On(CollectionId::default()));
        let perm_on_collection_2 =
            Permission::CollectionDev(AuthzEntity::On(CollectionId::default()));

        let authz_context = AuthzContextForTest::default().add_permissions(
            role,
            [
                sys_perm.clone(),
                perm_on_collection_1.clone(),
                perm_on_collection_2.clone(),
            ],
        );

        let perms = authz_context
            .role_collections_permissions(&mut conn, &role)
            .await
            .unwrap();
        let perms = perms.unwrap();
        assert_eq!(perms.len(), 2);
        assert!(perms.contains(&perm_on_collection_1));
        assert!(perms.contains(&perm_on_collection_2));
    }

    #[tokio::test]
    async fn test_role_collections_permissions_all() {
        let db = td_database::test_utils::db().await.unwrap();
        let mut conn = db.acquire().await.unwrap();

        let role = RoleId::default();
        let sys_perm = Permission::SecAdmin;
        let perm_on_collection =
            Permission::CollectionRead(AuthzEntity::On(CollectionId::default()));
        let perm_on_collection_shadowed =
            Permission::CollectionDev(AuthzEntity::On(CollectionId::default()));
        let perm_on_all = Permission::CollectionDev(AuthzEntity::All);

        let authz_context = AuthzContextForTest::default().add_permissions(
            role,
            [
                sys_perm.clone(),
                perm_on_collection.clone(),
                perm_on_collection_shadowed,
                perm_on_all.clone(),
            ],
        );

        let perms = authz_context
            .role_collections_permissions(&mut conn, &role)
            .await
            .unwrap();
        let perms = perms.unwrap();
        assert_eq!(perms.len(), 2);
        assert!(perms.contains(&perm_on_collection));
        assert!(perms.contains(&perm_on_all));
    }

    async fn test_admin_collection_permission_on_scope(scope: AuthzScope) {
        let collection = CollectionId::default();
        let role = RoleId::default();

        let authz_context = AuthzContextForTest::default().add_permissions(
            role,
            [Permission::CollectionAdmin(AuthzEntity::On(collection))],
        );
        let authz_context = Arc::new(authz_context);

        let request_context = Arc::new(RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            role,
            false,
        ));
        let scope = Arc::new(scope);
        assert_ok(
            &authz_context,
            &request_context,
            &scope,
            Authz::<CollAdmin>::new(),
        )
        .await;

        let authz_context = AuthzContextForTest::default().add_permissions(
            role,
            [Permission::CollectionDev(AuthzEntity::On(collection))],
        );
        let authz_context = Arc::new(authz_context);

        let request_context = Arc::new(RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            role,
            false,
        ));
        let scope = Arc::new(scope);
        assert_error(
            &authz_context,
            &request_context,
            &scope,
            Authz::<CollAdmin>::new(),
            AuthzError::Forbidden("".to_string()),
        )
        .await;
    }

    #[tokio::test]
    async fn test_admin_collection_permission_on_system_scope() {
        test_admin_collection_permission_on_scope(AuthzScope::System).await;
    }

    #[tokio::test]
    async fn test_admin_collection_permission_on_system_user_scope() {
        test_admin_collection_permission_on_scope(AuthzScope::SystemOrUser(AuthzEntity::On(
            UserId::default(),
        )))
        .await;
    }

    #[tokio::test]
    async fn test_inter_collection_permission_ok() {
        let source = CollectionId::default();
        let target = ToCollectionId::default();
        let mut authz_context = AuthzContextForTest::default();
        authz_context = authz_context.add_inter_collection_permission(&source, &target);
        let authz_context = Arc::new(authz_context);

        let db = td_database::test_utils::db().await.unwrap();
        let conn = db.acquire().await.unwrap();
        let conn = ConnectionType::PoolConnection(conn).into();
        let conn = Connection::new(conn);

        let list = Arc::new(vec![]);

        let res = Authz::<InterColl>::check_inter_collection(
            SrvCtx(authz_context.clone()),
            conn.clone(),
            Input(list),
        )
        .await;
        assert!(matches!(res, Ok(())));

        let list = Arc::new(vec![InterCollectionAccess::builder()
            .source(source)
            .target(ToCollectionId::try_from(&source).unwrap())
            .build()
            .unwrap()]);

        let res = Authz::<InterColl>::check_inter_collection(
            SrvCtx(authz_context.clone()),
            conn.clone(),
            Input(list),
        )
        .await;
        assert!(matches!(res, Ok(())));

        let list = Arc::new(vec![InterCollectionAccess::builder()
            .source(source)
            .target(target)
            .build()
            .unwrap()]);

        let res = Authz::<InterColl>::check_inter_collection(
            SrvCtx(authz_context.clone()),
            conn.clone(),
            Input(list),
        )
        .await;
        assert!(matches!(res, Ok(())));
    }

    #[tokio::test]
    async fn test_inter_collection_permission_err() {
        let source = CollectionId::default();
        let target = ToCollectionId::default();
        let mut authz_context = AuthzContextForTest::default();
        authz_context = authz_context.add_inter_collection_permission(&source, &target);
        let authz_context = Arc::new(authz_context);

        let db = td_database::test_utils::db().await.unwrap();
        let conn = db.acquire().await.unwrap();
        let conn = ConnectionType::PoolConnection(conn).into();
        let conn = Connection::new(conn);

        let list = Arc::new(vec![InterCollectionAccess::builder()
            .source(CollectionId::default())
            .target(ToCollectionId::default())
            .build()
            .unwrap()]);

        let res = Authz::<InterColl>::check_inter_collection(
            SrvCtx(authz_context.clone()),
            conn.clone(),
            Input(list),
        )
        .await;
        let err = res.err().unwrap();
        assert_eq!(
            std::mem::discriminant(&AuthzError::ForbiddenInterCollectionAccess("".to_string())),
            std::mem::discriminant(err.domain_err()),
        );

        let list = Arc::new(vec![InterCollectionAccess::builder()
            .source(source)
            .target(ToCollectionId::default())
            .build()
            .unwrap()]);

        let res = Authz::<InterColl>::check_inter_collection(
            SrvCtx(authz_context.clone()),
            conn.clone(),
            Input(list),
        )
        .await;
        let err = res.err().unwrap();
        assert_eq!(
            std::mem::discriminant(&AuthzError::ForbiddenInterCollectionAccess("".to_string())),
            std::mem::discriminant(err.domain_err()),
        );

        let list = Arc::new(vec![InterCollectionAccess::builder()
            .source(CollectionId::default())
            .target(target)
            .build()
            .unwrap()]);

        let res = Authz::<InterColl>::check_inter_collection(
            SrvCtx(authz_context.clone()),
            conn.clone(),
            Input(list),
        )
        .await;
        let err = res.err().unwrap();
        assert_eq!(
            std::mem::discriminant(&AuthzError::ForbiddenInterCollectionAccess("".to_string())),
            std::mem::discriminant(err.domain_err()),
        );
    }

    #[tokio::test]
    async fn test_visible_collections_no_collection_perm_in_role() {
        let authz_context = AuthzContextForTest::default();
        let authz_context = Arc::new(authz_context);

        let db = td_database::test_utils::db().await.unwrap();
        let mut conn = db.acquire().await.unwrap();

        let role = RoleId::default();

        let visible = authz_context
            .visible_collections(&mut conn, &role)
            .await
            .unwrap();
        assert!(visible.direct().is_empty());
        assert!(visible.indirect().is_empty());
    }

    #[tokio::test]
    async fn test_visible_collections_all_collection_perm_in_role() {
        let authz_context = AuthzContextForTest::default();
        let authz_context = Arc::new(authz_context);

        let db = td_database::test_utils::db().await.unwrap();
        let mut conn = db.acquire().await.unwrap();

        let role = RoleId::user();

        let visible = authz_context
            .visible_collections(&mut conn, &role)
            .await
            .unwrap();
        assert!(visible.direct().contains(&CollectionId::all_collections()));
        assert!(visible.indirect().is_empty());
    }

    #[tokio::test]
    async fn test_visible_collections_collection_permission_no_inter_collection() {
        let db = td_database::test_utils::db().await.unwrap();
        let mut conn = db.acquire().await.unwrap();

        let role = RoleId::default();
        let collection = CollectionId::default();
        let mut authz_context = AuthzContextForTest::default();
        authz_context = authz_context.add_permissions(
            role,
            vec![Permission::CollectionRead(AuthzEntity::On(collection))],
        );
        let authz_context = Arc::new(authz_context);

        let visible = authz_context
            .visible_collections(&mut conn, &role)
            .await
            .unwrap();
        assert_eq!(visible.direct().len(), 1);
        assert!(visible.direct().contains(&collection));
        assert!(visible.indirect().is_empty());
    }

    #[tokio::test]
    async fn test_visible_collections_no_admin_dev_collection_permission_inter_collection() {
        let db = td_database::test_utils::db().await.unwrap();
        let mut conn = db.acquire().await.unwrap();

        let role = RoleId::default();
        let collection = CollectionId::default();
        let inter_collection = CollectionId::default();
        let mut authz_context = AuthzContextForTest::default();
        authz_context = authz_context.add_permissions(
            role,
            vec![Permission::CollectionRead(AuthzEntity::On(collection))],
        );
        authz_context =
            authz_context.add_inter_collection_permission(&collection, &(*inter_collection).into());
        let authz_context = Arc::new(authz_context);

        let visible = authz_context
            .visible_collections(&mut conn, &role)
            .await
            .unwrap();
        assert_eq!(visible.direct().len(), 1);
        assert!(visible.direct().contains(&collection));
        assert_eq!(visible.indirect().len(), 0);
    }

    #[tokio::test]
    async fn test_visible_collections_admin_collection_permission_inter_collection() {
        let db = td_database::test_utils::db().await.unwrap();
        let mut conn = db.acquire().await.unwrap();

        let role = RoleId::default();
        let collection = CollectionId::default();
        let inter_collection = CollectionId::default();
        let mut authz_context = AuthzContextForTest::default();
        authz_context = authz_context.add_permissions(
            role,
            vec![Permission::CollectionAdmin(AuthzEntity::On(collection))],
        );
        authz_context =
            authz_context.add_inter_collection_permission(&inter_collection, &(*collection).into());
        let authz_context = Arc::new(authz_context);

        let visible = authz_context
            .visible_collections(&mut conn, &role)
            .await
            .unwrap();
        assert_eq!(visible.direct().len(), 1);
        assert!(visible.direct().contains(&collection));
        assert_eq!(visible.indirect().len(), 1);
        assert!(visible.indirect().contains(&inter_collection));
    }

    #[tokio::test]
    async fn test_visible_collections_dev_collection_permission_inter_collection() {
        let db = td_database::test_utils::db().await.unwrap();
        let mut conn = db.acquire().await.unwrap();

        let role = RoleId::default();
        let collection = CollectionId::default();
        let inter_collection = CollectionId::default();
        let mut authz_context = AuthzContextForTest::default();
        authz_context = authz_context.add_permissions(
            role,
            vec![Permission::CollectionDev(AuthzEntity::On(collection))],
        );
        authz_context =
            authz_context.add_inter_collection_permission(&inter_collection, &(*collection).into());
        let authz_context = Arc::new(authz_context);

        let visible = authz_context
            .visible_collections(&mut conn, &role)
            .await
            .unwrap();
        assert_eq!(visible.direct().len(), 1);
        assert!(visible.direct().contains(&collection));
        assert_eq!(visible.indirect().len(), 1);
        assert!(visible.indirect().contains(&inter_collection));
    }
}
