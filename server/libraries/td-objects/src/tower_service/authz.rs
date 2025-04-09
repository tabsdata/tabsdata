//
// Copyright 2025 Tabs Data Inc.
//

//! Services Authorization layers.
//!
//!
//! Assuming `PermissionsStore` implements [`AuthzContext`].
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
//! ```ignore
//!   ...
//!   layer(AuthzOn<SystemOrUserId>::set).
//!   layer(Authz<SysAdmin>::check).
//!   ...
//! ```
//!
//! User permission check:
//!
//! ```ignore
//!   ...
//!   layer(from_fn(extract_id::<User, UserId>)).
//!   layer(AuthzOn<SystemOrUserId>::set).
//!   layer(Authz<Requester>::check).
//!   ...
//! ```
//!
//! System or User permission check:
//!
//! ```ignore
//!   ...
//!   layer(from_fn(extract_id::<User, UserId>)).
//!   layer(AuthzOn<SystemOrUserId>::set).
//!   layer(Authz<Requester, SecAdmin>::check).
//!   ...
//! ```
//!
//! Collections permissions check:
//!
//! ```ignore
//!   ...
//!   .layer(from_fn(extract_name::<ListRequest<FunctionParam>, FunctionParam, CollectionName>))
//!   .layer(from_fn(find_by_name::<CollectionName, Collection>))
//!   .layer(from_fn(extract_id::<Collection, CollectionId>))
//!   .layer(AuthzOn<CollectionId>::set).
//!   .layer(Authz<SysAdmin, CollAdmin>::check).
//!   ...
//! ```

use crate::crudl::RequestContext;
use crate::types::basic::{CollectionId, RoleId, UserId};
use async_trait::async_trait;
use sqlx::SqliteConnection;
use std::any::type_name;
use std::collections::HashSet;
use std::fmt::{Debug, Display, Formatter};
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;
use td_error::TdError;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection, SrvCtx};

#[async_trait]
pub trait AuthzContext {
    async fn role_permissions(
        &self,
        conn: &mut SqliteConnection,
        role: &RoleId,
    ) -> Result<Option<Arc<Vec<Permission>>>, TdError>;

    async fn refresh(&self, _conn: &mut SqliteConnection) -> Result<(), TdError> {
        Ok(())
    }
}

#[td_error::td_error]
pub enum AuthzError {
    #[error("Unauthorized for '{0}'")]
    UnAuthorized(String) = 4000,

    #[error("Invalid authorization scope, '{0}' cannot be on {1}")]
    InvalidAuthzScope(String, String) = 5000,

    #[error("The entity returned by a '{0}::any_of()' cannot be `{1}`")]
    AuthEntityCannotBeAll(String, String) = 5001,
}

/// Enum that denotes an Entity to check authorization on.
#[derive(Debug, Clone, PartialEq, Eq, Hash, strum_macros::IntoStaticStr)]
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
            Self::On(e) => write!(f, "On({:?})", e),
            Self::All => write!(f, "All"),
        }
    }
}

/// Enum that denotes the scope for permissions.
#[derive(Debug, Clone, PartialEq, Eq, Hash, strum_macros::IntoStaticStr)]
pub enum AuthzScope {
    /// A system permission.
    System,
    /// A collection permission.
    Collection(AuthzEntity<CollectionId>),
    /// A user permission.
    User(AuthzEntity<UserId>),
    /// A system or user permission.
    SystemUser(AuthzEntity<UserId>),
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
            Self::Collection(c) => write!(f, "Collection({:?})", c),
            Self::User(u) => write!(f, "User({:?})", u),
            Self::SystemUser(u) => write!(f, "SystemUser({:?})", u),
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
        Ok(AuthzScope::Collection(AuthzEntity::On(
            collection_id.deref().clone(),
        )))
    }
}

pub struct SystemOrUserId {
    #[allow(dead_code)]
    instance_blocker: (),
}

impl AuthzOn<SystemOrUserId> {
    /// Set the Authorization scope to [`AuthzScope::User`] for a [`UserId`] in the service context.
    pub async fn set(Input(user_id): Input<UserId>) -> Result<AuthzScope, TdError> {
        Ok(AuthzScope::User(AuthzEntity::On(user_id.deref().clone())))
    }
}

/// Enum with all defined permissions.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Permission {
    /// Requester permission (this permission does not go into Permissions Table).
    User(AuthzEntity<UserId>),
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
    /// Accessing private and public tables (schema and data) in a collection.
    CollectionReadAll(AuthzEntity<CollectionId>), //read (public & private) tables
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
            AuthzScope::SystemUser(authz_on) => {
                Ok(Some(HashSet::from([Permission::User(authz_on.clone())])))
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
        AuthzScope::SystemUser(authz_on) => Err(AuthzError::InvalidAuthzScope(
            type_name::<R>().to_owned(),
            AuthzScope::User(authz_on.clone()).to_str().to_string(),
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
        collection_any_of::<Self>(scope, |collection_id| {
            HashSet::from([Permission::CollectionAdmin(AuthzEntity::On(
                collection_id.clone(),
            ))])
        })
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
            HashSet::from([Permission::CollectionDev(AuthzEntity::On(
                collection_id.clone(),
            ))])
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
            HashSet::from([Permission::CollectionExec(AuthzEntity::On(
                collection_id.clone(),
            ))])
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
            HashSet::from([Permission::CollectionRead(AuthzEntity::On(
                collection_id.clone(),
            ))])
        })
    }
}

/// Collection read public and private permission.
#[derive(Debug)]
pub struct CollReadAll {
    #[allow(dead_code)]
    instance_blocker: (),
}

impl AuthzRequirements for CollReadAll {
    fn any_of(scope: &AuthzScope) -> Result<Option<HashSet<Permission>>, TdError> {
        collection_any_of::<Self>(scope, |collection_id| {
            HashSet::from([Permission::CollectionReadAll(AuthzEntity::On(
                collection_id.clone(),
            ))])
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
            Permission::CollectionReadAll(AuthzEntity::On(_)) => {
                with_wildcards.insert(Permission::CollectionReadAll(AuthzEntity::All));
            }
            _ => {}
        }
    }
    with_wildcards
}

/// Service Authorization enforcer.
pub struct Authz<
    AC: AuthzContext,
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
        AC: AuthzContext,
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
    /// The role permissions are from the [`AuthzContext`] in the service context.
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
            }
            let user_id: UserId = request_context.user_id().as_str().try_into()?;
            if required_permissions.contains(&Permission::User(AuthzEntity::On(user_id))) {
                return Ok(());
            }
            Err(AuthzError::UnAuthorized(scope.to_string()))?
        }
    }
}

#[cfg(test)]
mod test {
    use crate::crudl::RequestContext;
    use crate::tower_service::authz::{
        AuthzContext, AuthzEntity, AuthzError, AuthzRequirements, AuthzScope, CollAdmin, CollDev,
        CollExec, CollRead, CollReadAll, NoPermissions, Permission, Requester, SecAdmin, SysAdmin,
    };
    use crate::types::basic::{CollectionId, RoleId, UserId};
    use async_trait::async_trait;
    use lazy_static::lazy_static;
    use sqlx::SqliteConnection;
    use std::collections::HashMap;
    use std::marker::PhantomData;
    use std::sync::Arc;
    use td_common::id;
    use td_error::TdError;
    use td_tower::extractors::{Connection, ConnectionType, Input, SrvCtx};

    fn sys_admin_role() -> &'static RoleId {
        lazy_static! {
            static ref ROLE_ID: RoleId = id::id().into();
        }
        &ROLE_ID
    }

    fn sec_admin_role() -> &'static RoleId {
        lazy_static! {
            static ref ROLE_ID: RoleId = id::id().into();
        }
        &ROLE_ID
    }

    fn user_role() -> &'static RoleId {
        lazy_static! {
            static ref ROLE_ID: RoleId = id::id().into();
        }
        &ROLE_ID
    }

    #[derive(Debug)]
    struct AuthzContextForTest {
        role_permissions_map: HashMap<RoleId, Arc<Vec<Permission>>>,
    }

    impl AuthzContextForTest {
        pub fn add(
            mut self,
            role: impl Into<RoleId>,
            permissions: impl Into<Vec<Permission>>,
        ) -> Self {
            self.role_permissions_map
                .insert(role.into(), Arc::new(permissions.into()));
            self
        }

        pub fn remove(mut self, role: &RoleId) -> Self {
            self.role_permissions_map.remove(role);
            self
        }

        pub fn default() -> Self {
            Self {
                role_permissions_map: HashMap::new(),
            }
            .add(
                sys_admin_role(),
                [
                    Permission::SysAdmin,
                    Permission::SecAdmin,
                    Permission::CollectionAdmin(AuthzEntity::All),
                    Permission::CollectionDev(AuthzEntity::All),
                    Permission::CollectionExec(AuthzEntity::All),
                    Permission::CollectionRead(AuthzEntity::All),
                    Permission::CollectionReadAll(AuthzEntity::All),
                ],
            )
            .add(
                sec_admin_role(),
                [
                    Permission::SecAdmin,
                    Permission::CollectionAdmin(AuthzEntity::All),
                ],
            )
            .add(
                user_role(),
                [
                    Permission::CollectionDev(AuthzEntity::All),
                    Permission::CollectionExec(AuthzEntity::All),
                    Permission::CollectionRead(AuthzEntity::All),
                    Permission::CollectionReadAll(AuthzEntity::All),
                ],
            )
        }
    }

    #[async_trait]
    impl AuthzContext for AuthzContextForTest {
        async fn role_permissions(
            &self,
            _conn: &mut SqliteConnection,
            role: &RoleId,
        ) -> Result<Option<Arc<Vec<Permission>>>, TdError> {
            Ok(self.role_permissions_map.get(role).map(Arc::clone))
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
                panic!("Check failed with {:?}", err);
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
                panic!(
                    "Check passed, it should have failed with {:?}",
                    expected_err
                );
            }
            Err(err) => {
                let authz_err: &AuthzError = err.domain_err();
                assert_eq!(
                    std::mem::discriminant(&expected_err),
                    std::mem::discriminant(authz_err),
                    "Expected {}, Got {}",
                    expected_err,
                    authz_err
                );
            }
        }
    }

    #[tokio::test]
    async fn test_default_roles_and_permissions() {
        let authz_context = AuthzContextForTest::default();

        let sys_admin_context =
            RequestContext::with(id::id(), &sys_admin_role().to_string(), false).await;
        let sec_admin_context =
            RequestContext::with(id::id(), &sec_admin_role().to_string(), false).await;
        let user_context = RequestContext::with(id::id(), &user_role().to_string(), false).await;

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
        assert_ok(
            &authz_context,
            &request_context,
            scope,
            Authz::<CollReadAll>::new(),
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
            AuthzError::UnAuthorized("".to_string()),
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
            AuthzError::UnAuthorized("".to_string()),
        )
        .await;
        assert_error(
            &authz_context,
            &request_context,
            scope,
            Authz::<CollExec>::new(),
            AuthzError::UnAuthorized("".to_string()),
        )
        .await;
        assert_error(
            &authz_context,
            &request_context,
            scope,
            Authz::<CollRead>::new(),
            AuthzError::UnAuthorized("".to_string()),
        )
        .await;
        assert_error(
            &authz_context,
            &request_context,
            scope,
            Authz::<CollReadAll>::new(),
            AuthzError::UnAuthorized("".to_string()),
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
            AuthzError::UnAuthorized("".to_string()),
        )
        .await;
        assert_error(
            &authz_context,
            &request_context,
            scope,
            Authz::<SecAdmin>::new(),
            AuthzError::UnAuthorized("".to_string()),
        )
        .await;
        let scope = &collection_scope;
        assert_error(
            &authz_context,
            &request_context,
            scope,
            Authz::<CollAdmin>::new(),
            AuthzError::UnAuthorized("".to_string()),
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
        assert_ok(
            &authz_context,
            &request_context,
            scope,
            Authz::<CollReadAll>::new(),
        )
        .await;
    }

    #[tokio::test]
    async fn test_multiple_any_of_all_avail() {
        let authz_context = AuthzContextForTest::default();

        let request_context =
            Arc::new(RequestContext::with(id::id(), &sys_admin_role().to_string(), false).await);

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

        let request_context =
            Arc::new(RequestContext::with(id::id(), &sec_admin_role().to_string(), false).await);

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

        let request_context =
            Arc::new(RequestContext::with(id::id(), &user_role().to_string(), false).await);

        let scope = Arc::new(AuthzScope::System);

        let authz_context = Arc::new(authz_context);

        // sys_admin role
        assert_error(
            &authz_context,
            &request_context,
            &scope,
            Authz::<SysAdmin, SecAdmin>::new(),
            AuthzError::UnAuthorized("".to_string()),
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
            .remove(user_role())
            .add(
                &all_collections,
                [Permission::CollectionRead(AuthzEntity::All)],
            )
            .add(
                &one_collection,
                [Permission::CollectionRead(AuthzEntity::On(
                    collection0.clone(),
                ))],
            );
        let authz_context = Arc::new(authz_context);

        // role with permission granted on all collections
        let request_context =
            Arc::new(RequestContext::with(id::id(), &all_collections.to_string(), false).await);
        let scope = Arc::new(AuthzScope::Collection(AuthzEntity::On(collection0.clone())));
        assert_ok(
            &authz_context,
            &request_context,
            &scope,
            Authz::<CollRead>::new(),
        )
        .await;
        let scope = Arc::new(AuthzScope::Collection(AuthzEntity::On(collection1.clone())));
        assert_ok(
            &authz_context,
            &request_context,
            &scope,
            Authz::<CollRead>::new(),
        )
        .await;

        // role with permission granted on one collection
        let request_context =
            Arc::new(RequestContext::with(id::id(), &one_collection.to_string(), false).await);
        let scope = Arc::new(AuthzScope::Collection(AuthzEntity::On(collection0.clone())));
        assert_ok(
            &authz_context,
            &request_context,
            &scope,
            Authz::<CollRead>::new(),
        )
        .await;
        let scope = Arc::new(AuthzScope::Collection(AuthzEntity::On(collection1.clone())));
        assert_error(
            &authz_context,
            &request_context,
            &scope,
            Authz::<CollRead>::new(),
            AuthzError::UnAuthorized("".to_string()),
        )
        .await;
    }

    #[tokio::test]
    async fn test_any_of_generics_1_to_7() {
        let collection = CollectionId::default();
        let role = RoleId::default();

        let authz_context = AuthzContextForTest::default()
            .remove(user_role())
            .add(&role, [Permission::CollectionRead(AuthzEntity::All)]);
        let authz_context = Arc::new(authz_context);

        // positive
        let request_context =
            Arc::new(RequestContext::with(id::id(), &role.to_string(), false).await);
        let scope = Arc::new(AuthzScope::Collection(AuthzEntity::On(collection.clone())));
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

        let request_context =
            Arc::new(RequestContext::with(id::id(), &role.to_string(), false).await);
        let scope = Arc::new(AuthzScope::Collection(AuthzEntity::On(collection.clone())));
        assert_error(
            &authz_context,
            &request_context,
            &scope,
            Authz::<CollExec>::new(),
            AuthzError::UnAuthorized("".to_string()),
        )
        .await;
        assert_error(
            &authz_context,
            &request_context,
            &scope,
            Authz::<NoPermissions, CollExec>::new(),
            AuthzError::UnAuthorized("".to_string()),
        )
        .await;
        assert_error(
            &authz_context,
            &request_context,
            &scope,
            Authz::<NoPermissions, NoPermissions, CollExec>::new(),
            AuthzError::UnAuthorized("".to_string()),
        )
        .await;
        assert_error(
            &authz_context,
            &request_context,
            &scope,
            Authz::<NoPermissions, NoPermissions, NoPermissions, CollExec>::new(),
            AuthzError::UnAuthorized("".to_string()),
        )
        .await;
        assert_error(
            &authz_context,
            &request_context,
            &scope,
            Authz::<NoPermissions, NoPermissions, NoPermissions, NoPermissions, CollExec>::new(),
            AuthzError::UnAuthorized("".to_string()),
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
            AuthzError::UnAuthorized("".to_string()),
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
            AuthzError::UnAuthorized("".to_string()),
        )
        .await;
    }

    #[tokio::test]
    async fn test_collection_system_permission_on_collection_scope() {
        let collection = CollectionId::default();
        let role = RoleId::default();

        let authz_context = AuthzContextForTest::default().add(
            &role,
            [
                Permission::SecAdmin,
                Permission::CollectionRead(AuthzEntity::All),
            ],
        );
        let authz_context = Arc::new(authz_context);

        let request_context =
            Arc::new(RequestContext::with(id::id(), &role.to_string(), false).await);
        let scope = Arc::new(AuthzScope::Collection(AuthzEntity::On(collection.clone())));
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
        let authz_context = AuthzContextForTest::default().add(
            &role,
            [
                Permission::SecAdmin,
                Permission::CollectionRead(AuthzEntity::All),
            ],
        );
        let authz_context = Arc::new(authz_context);

        let request_context =
            Arc::new(RequestContext::with(id::id(), &role.to_string(), false).await);
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
        let authz_context = AuthzContextForTest::default().add(
            &role,
            [
                Permission::SecAdmin,
                Permission::CollectionRead(AuthzEntity::All),
            ],
        );
        let authz_context = Arc::new(authz_context);

        let request_context =
            Arc::new(RequestContext::with(id::id(), &role.to_string(), false).await);
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
    async fn test_user_scope_as_user() {
        let user = UserId::default();
        let role = RoleId::default();

        let authz_context = AuthzContextForTest::default().add(&role, []);
        let authz_context = Arc::new(authz_context);

        let request_context =
            Arc::new(RequestContext::with(user.to_string(), &role.to_string(), false).await);

        let scope = Arc::new(AuthzScope::User(AuthzEntity::On(user.clone())));

        assert_ok(
            &authz_context,
            &request_context,
            &scope,
            Authz::<Requester>::new(),
        )
        .await;
    }

    #[tokio::test]
    async fn test_user_scope_as_user_error() {
        let user = UserId::default();
        let role = RoleId::default();

        let authz_context = AuthzContextForTest::default().add(&role, []);
        let authz_context = Arc::new(authz_context);

        // different user
        let request_context =
            Arc::new(RequestContext::with(id::id().to_string(), &role.to_string(), false).await);

        let scope = Arc::new(AuthzScope::User(AuthzEntity::On(user.clone())));

        assert_error(
            &authz_context,
            &request_context,
            &scope,
            Authz::<Requester>::new(),
            AuthzError::UnAuthorized("".to_string()),
        )
        .await;

        let authz_context =
            AuthzContextForTest::default().add(&role, [Permission::SecAdmin, Permission::SysAdmin]);
        let authz_context = Arc::new(authz_context);

        // different user
        let request_context =
            Arc::new(RequestContext::with(id::id().to_string(), &role.to_string(), false).await);

        let scope = Arc::new(AuthzScope::User(AuthzEntity::On(user.clone())));

        assert_error(
            &authz_context,
            &request_context,
            &scope,
            Authz::<Requester>::new(),
            AuthzError::UnAuthorized("".to_string()),
        )
        .await;
    }

    #[tokio::test]
    async fn test_system_user_scope_as_system() {
        let user = UserId::default();
        let role = RoleId::default();

        let authz_context = AuthzContextForTest::default().add(&role, [Permission::SecAdmin]);
        let authz_context = Arc::new(authz_context);

        let request_context =
            Arc::new(RequestContext::with(id::id().to_string(), &role.to_string(), false).await);

        let scope = Arc::new(AuthzScope::SystemUser(AuthzEntity::On(user.clone())));

        assert_ok(
            &authz_context,
            &request_context,
            &scope,
            Authz::<Requester, SecAdmin>::new(),
        )
        .await;
    }
}
