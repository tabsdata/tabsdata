//
// Copyright 2025 Tabs Data Inc.
//

use crate::types::SqlEntity;
use crate::types::id::{
    CollectionId, ExecutionId, FunctionId, InterCollectionPermissionId, PermissionId, RoleId,
    TableId, TransactionId, UserId, WorkerId,
};
use crate::types::string::{CollectionName, FunctionName, RoleName, TableName, UserName};
use std::fmt::Debug;

// Trait definitions and implementations
pub trait IdOrName: SqlEntity {
    type Id: SqlEntity;
    fn id(&self) -> Option<&Self::Id>;
    fn from_id(id: impl Into<Self::Id>) -> Self;

    type Name: SqlEntity;
    fn name(&self) -> Option<&Self::Name>;
    fn from_name(name: impl Into<Self::Name>) -> Self;
}

// Object type definitions

#[td_type::typed(id_name(id = CollectionId, name = CollectionName))]
pub struct CollectionIdName;

// ExecutionName is not UNIQUE nor NOT NULL. We cannot use it as a primary key to lookup.
#[td_type::typed(id_name(id = ExecutionId))]
pub struct ExecutionIdName;

#[td_type::typed(id_name(id = FunctionId, name = FunctionName))]
pub struct FunctionIdName;

#[td_type::typed(id_name(id = InterCollectionPermissionId))]
pub struct InterCollectionPermissionIdName;

#[td_type::typed(id_name(id = PermissionId))]
pub struct PermissionIdName;

#[td_type::typed(id_name(id = RoleId, name = RoleName))]
pub struct RoleIdName;

#[td_type::typed(id_name(id = TableId, name = TableName))]
pub struct TableIdName;

#[td_type::typed(id_name(id = TransactionId))]
pub struct TransactionIdName;

#[td_type::typed(id_name(id = UserId, name = UserName))]
pub struct UserIdName;

#[td_type::typed(id_name(id = WorkerId))]
pub struct WorkerIdName;
