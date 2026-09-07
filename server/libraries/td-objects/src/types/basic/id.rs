//
// Copyright 2025 Tabsdata Inc.
//

use ta_objects::{Typed, TypedOpenApi, TypedSerde, TypedSqlx};
use td_common::id::{ID_LENGTH, Id};
use td_common::logic_id::IdL;
use td_security::{
    ID_ALL_ENTITIES, ID_GROUP_DEFAULT, ID_GROUP_DESTINATION, ID_GROUP_SOURCE, ID_ROLE_DEFAULT,
    ID_ROLE_PROJECT_CREATOR, ID_ROLE_SYS_ADMIN, ID_SYSTEM, ID_USER_ADMIN,
};

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct AccessTokenId(Id);

impl AccessTokenId {
    pub fn log(&self) -> String {
        // half the token as trace
        let s = self.to_string();
        let mid = ID_LENGTH / 2;
        s[mid..].to_string()
    }
}

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct BundleId(Id);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
#[typed(try_from = EntityId, try_from = FromCollectionId, try_from = ToCollectionId)]
pub struct CollectionId(Id);

impl CollectionId {
    pub fn all_collections() -> Self {
        Self(Id::_new(ID_ALL_ENTITIES))
    }

    pub fn is_all_collections(&self) -> bool {
        *self.0 == ID_ALL_ENTITIES
    }
}

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct ConnectionId(Id);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
#[typed(try_from = EntityId)]
pub struct ProjectId(Id);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct DependencyId(Id);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct DependencyVersionId(Id);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
#[typed(try_from = CollectionId, try_from = ProjectId)]
pub struct EntityId(Id);

impl EntityId {
    pub fn all_entities() -> Self {
        Self(Id::_new(ID_ALL_ENTITIES))
    }

    pub fn is_all_entities(&self) -> bool {
        *self.0 == ID_ALL_ENTITIES
    }
}

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct ExecutionId(IdL<EP>);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
#[typed(try_from = CollectionId)]
pub struct FromCollectionId(Id);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct FunctionId(Id);

/// Identity of a function's stage/checkpoint directory. Stable across function
/// versions (carried forward on update) so a function's accumulating state
/// persists; regenerated only on `--reset-stage` to start fresh.
#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct StageId(Id);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct FunctionVersionId(IdL<FV>);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct GroupId(Id);

impl GroupId {
    // Group ids are unique per project (composite PK), and the seeded system
    // groups carry these well-known ids in EVERY project — so the helpers are
    // valid universally, unlike ordinary ids.

    /// The seeded `default` group.
    pub fn default_group() -> Self {
        Self(Id::_new(ID_GROUP_DEFAULT))
    }

    /// The seeded `sources` group — collections in it are `Source` flavour.
    pub fn source() -> Self {
        Self(Id::_new(ID_GROUP_SOURCE))
    }

    /// The seeded `destinations` group — collections in it are `Destination` flavour.
    pub fn destination() -> Self {
        Self(Id::_new(ID_GROUP_DESTINATION))
    }
}

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct LogicalFunctionRunId(IdL<FR>);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct LogicalFunctionRunInputId(Id);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct LogicalTableDataVersionId(IdL<DV>);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct PermissionId(Id);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct PhysicalFunctionRunId(IdL<PFR>);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct PhysicalTableDataVersionId(IdL<PDV>);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct RefreshTokenId(Id);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct RequirementId(Id);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct RoleId(Id);

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

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct TableId(Id);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct TableVersionId(Id);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
#[typed(try_from = CollectionId)]
pub struct ToCollectionId(Id);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct TransactionId(IdL<TX>);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct TriggerId(Id);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct TriggerVersionId(Id);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct UserId(Id);

impl UserId {
    pub fn admin() -> Self {
        Self(Id::_new(ID_USER_ADMIN))
    }

    pub fn system() -> Self {
        Self(Id::_new(ID_SYSTEM))
    }
}

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct UserRoleId(Id);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct WorkerId(IdL<WKR>);
