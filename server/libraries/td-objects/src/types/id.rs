//
// Copyright 2025 Tabs Data Inc.
//

use td_common::id::{ID_LENGTH, Id};
use td_security::{
    ID_ALL_ENTITIES, ID_ROLE_SEC_ADMIN, ID_ROLE_SYS_ADMIN, ID_ROLE_USER, ID_USER_ADMIN,
};

#[td_type::typed(id)]
pub struct AccessTokenId;

impl AccessTokenId {
    pub fn log(&self) -> String {
        // half the token as trace
        let s = self.to_string();
        let mid = ID_LENGTH / 2;
        s[mid..].to_string()
    }
}

#[td_type::typed(id)]
pub struct BundleId;

#[td_type::typed(id, try_from = EntityId, try_from = FromCollectionId, try_from = ToCollectionId)]
pub struct CollectionId;

impl CollectionId {
    pub fn all_collections() -> Self {
        Self(Id::_new(ID_ALL_ENTITIES))
    }

    pub fn is_all_collections(&self) -> bool {
        *self.0 == ID_ALL_ENTITIES
    }
}

#[td_type::typed(id)]
pub struct DependencyId;

#[td_type::typed(id)]
pub struct DependencyVersionId;

#[td_type::typed(id, try_from = CollectionId)]
pub struct EntityId;

impl EntityId {
    pub fn all_entities() -> Self {
        Self(Id::_new(ID_ALL_ENTITIES))
    }

    pub fn is_all_entities(&self) -> bool {
        *self.0 == ID_ALL_ENTITIES
    }
}

#[td_type::typed(id)]
pub struct ExecutionId;

#[td_type::typed(id, try_from = CollectionId)]
pub struct FromCollectionId;

#[td_type::typed(id)]
pub struct FunctionId;

#[td_type::typed(id)]
pub struct FunctionRunId;

#[td_type::typed(id)]
pub struct FunctionVersionId;

#[td_type::typed(id)]
pub struct InterCollectionPermissionId;

#[td_type::typed(id)]
pub struct PermissionId;

#[td_type::typed(id)]
pub struct RefreshTokenId;

#[td_type::typed(id)]
pub struct RequirementId;

#[td_type::typed(id)]
pub struct RoleId;

impl RoleId {
    pub fn sys_admin() -> Self {
        Self(Id::_new(ID_ROLE_SYS_ADMIN))
    }

    pub fn sec_admin() -> Self {
        Self(Id::_new(ID_ROLE_SEC_ADMIN))
    }

    pub fn user() -> Self {
        Self(Id::_new(ID_ROLE_USER))
    }
}

#[td_type::typed(id)]
pub struct SessionId;

#[td_type::typed(id)]
pub struct TableDataId;

#[td_type::typed(id)]
pub struct TableDataVersionId;

#[td_type::typed(id)]
pub struct TableId;

#[td_type::typed(id)]
pub struct TableVersionId;

#[td_type::typed(id, try_from = CollectionId)]
pub struct ToCollectionId;

#[td_type::typed(id)]
pub struct TransactionId;

#[td_type::typed(id)]
pub struct TriggerId;

#[td_type::typed(id)]
pub struct TriggerVersionId;

#[td_type::typed(id)]
pub struct UserId;

impl UserId {
    pub fn admin() -> Self {
        Self(Id::_new(ID_USER_ADMIN))
    }
}

#[td_type::typed(id)]
pub struct UserRoleId;

#[td_type::typed(id)]
pub struct WorkerId;
