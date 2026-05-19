//
// Copyright 2025 Tabs Data Inc.
//

use td_common::id::{ID_LENGTH, Id};
use td_security::{
    ID_ALL_ENTITIES, ID_ROLE_SEC_ADMIN, ID_ROLE_SYS_ADMIN, ID_ROLE_USER, ID_USER_ADMIN,
};

#[ta_objects::typed(id)]
pub struct AccessTokenId;

impl AccessTokenId {
    pub fn log(&self) -> String {
        // half the token as trace
        let s = self.to_string();
        let mid = ID_LENGTH / 2;
        s[mid..].to_string()
    }
}

#[ta_objects::typed(id)]
pub struct BundleId;

#[ta_objects::typed(id, try_from = EntityId, try_from = FromCollectionId, try_from = ToCollectionId)]
pub struct CollectionId;

impl CollectionId {
    pub fn all_collections() -> Self {
        Self(Id::_new(ID_ALL_ENTITIES))
    }

    pub fn is_all_collections(&self) -> bool {
        *self.0 == ID_ALL_ENTITIES
    }
}

#[ta_objects::typed(id)]
pub struct DependencyId;

#[ta_objects::typed(id)]
pub struct DependencyVersionId;

#[ta_objects::typed(id, try_from = CollectionId)]
pub struct EntityId;

impl EntityId {
    pub fn all_entities() -> Self {
        Self(Id::_new(ID_ALL_ENTITIES))
    }

    pub fn is_all_entities(&self) -> bool {
        *self.0 == ID_ALL_ENTITIES
    }
}

#[ta_objects::typed(idlogic(key = EP))]
pub struct ExecutionId;

#[ta_objects::typed(id, try_from = CollectionId)]
pub struct FromCollectionId;

#[ta_objects::typed(id)]
pub struct FunctionId;

#[ta_objects::typed(idlogic(key = FV))]
pub struct FunctionVersionId;

#[ta_objects::typed(id)]
pub struct InterCollectionPermissionId;

#[ta_objects::typed(idlogic(key = LFR))]
pub struct LogicalFunctionRunId;

#[ta_objects::typed(id)]
pub struct LogicalFunctionRunInputId;

#[ta_objects::typed(id)]
pub struct LogicalFunctionRunPointerId;

#[ta_objects::typed(idlogic(key = LDV))]
pub struct LogicalTableDataVersionId;

#[ta_objects::typed(id)]
pub struct LogicalTableDataVersionPointerId;

#[ta_objects::typed(id)]
pub struct PermissionId;

#[ta_objects::typed(idlogic(key = PFR))]
pub struct PhysicalFunctionRunId;

#[ta_objects::typed(idlogic(key = PDV))]
pub struct PhysicalTableDataVersionId;

#[ta_objects::typed(id)]
pub struct RefreshTokenId;

#[ta_objects::typed(id)]
pub struct RequirementId;

#[ta_objects::typed(id)]
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

#[ta_objects::typed(id)]
pub struct TableId;

#[ta_objects::typed(id)]
pub struct TableVersionId;

#[ta_objects::typed(id, try_from = CollectionId)]
pub struct ToCollectionId;

#[ta_objects::typed(idlogic(key = TX))]
pub struct TransactionId;

#[ta_objects::typed(id)]
pub struct TriggerId;

#[ta_objects::typed(id)]
pub struct TriggerVersionId;

#[ta_objects::typed(id)]
pub struct UserId;

impl UserId {
    pub fn admin() -> Self {
        Self(Id::_new(ID_USER_ADMIN))
    }
}

#[ta_objects::typed(id)]
pub struct UserRoleId;

#[ta_objects::typed(id)]
pub struct WorkerId;
