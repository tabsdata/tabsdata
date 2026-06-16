//
// Copyright 2025 Tabs Data Inc.
//

use td_common::id::{ID_LENGTH, Id};
use td_security::{
    ID_ALL_ENTITIES, ID_GROUP_DEFAULT, ID_GROUP_DESTINATION, ID_GROUP_SOURCE, ID_PROJECT_SANDBOX,
    ID_ROLE_DEFAULT, ID_ROLE_PROJECT_CREATOR, ID_ROLE_SYS_ADMIN, ID_SYSTEM, ID_USER_ADMIN,
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

#[ta_objects::typed(id, try_from = EntityId)]
pub struct ProjectId;

impl ProjectId {
    /// The seeded `sandbox` project — the only project the bootstrap creates;
    /// all others come from the projects CRUD API.
    #[cfg(feature = "td-test")]
    pub fn sandbox() -> Self {
        Self(Id::_new(ID_PROJECT_SANDBOX))
    }

    pub fn is_sandbox(&self) -> bool {
        *self.0 == ID_PROJECT_SANDBOX
    }
}

#[ta_objects::typed(id)]
pub struct DependencyId;

#[ta_objects::typed(id)]
pub struct DependencyVersionId;

#[ta_objects::typed(id, try_from = CollectionId, try_from = ProjectId)]
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

/// Identity of a function's stage/checkpoint directory. Stable across function
/// versions (carried forward on update) so a function's accumulating state
/// persists; regenerated only on `--reset-stage` to start fresh.
#[ta_objects::typed(id)]
pub struct StageId;

#[ta_objects::typed(idlogic(key = FV))]
pub struct FunctionVersionId;

#[ta_objects::typed(id)]
pub struct GroupId;

impl GroupId {
    // Group ids are unique per project (composite PK), and the seeded system
    // groups carry these well-known ids in EVERY project — so the helpers are
    // valid universally, unlike ordinary ids.

    /// The seeded `default` group.
    pub fn default_group() -> Self {
        Self(Id::_new(ID_GROUP_DEFAULT))
    }

    /// The seeded `source` group — collections in it are `Source` flavour.
    pub fn source() -> Self {
        Self(Id::_new(ID_GROUP_SOURCE))
    }

    /// The seeded `destination` group — collections in it are `Destination` flavour.
    pub fn destination() -> Self {
        Self(Id::_new(ID_GROUP_DESTINATION))
    }
}

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

    pub fn default_role() -> Self {
        Self(Id::_new(ID_ROLE_DEFAULT))
    }

    pub fn project_creator() -> Self {
        Self(Id::_new(ID_ROLE_PROJECT_CREATOR))
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

    pub fn system() -> Self {
        Self(Id::_new(ID_SYSTEM))
    }
}

#[ta_objects::typed(id)]
pub struct UserRoleId;

#[ta_objects::typed(id)]
pub struct WorkerId;
