//
// Copyright 2025 Tabs Data Inc.
//

use crate::types::basic::{
    CollectionId, CollectionName, ExecutionId, FunctionId, FunctionName,
    InterCollectionPermissionId, PermissionId, RoleId, RoleName, TableId, TableName, TransactionId,
    UserId, UserName, WorkerId,
};

#[ta_objects::typed_tag]
pub enum CollectionIdName {
    #[tag("~")]
    Id(CollectionId),
    #[tag("")]
    Name(CollectionName),
}

// ExecutionName is not UNIQUE nor NOT NULL. We cannot use it as a primary key to lookup.
#[ta_objects::typed_tag]
pub enum ExecutionIdName {
    #[tag("~")]
    Id(ExecutionId),
    #[tag("")]
    Name(ExecutionId),
}

#[ta_objects::typed_tag]
pub enum FunctionIdName {
    #[tag("~")]
    Id(FunctionId),
    #[tag("")]
    Name(FunctionName),
}

#[ta_objects::typed_tag]
pub enum InterCollectionPermissionIdName {
    #[tag("~")]
    Id(InterCollectionPermissionId),
    #[tag("")]
    Name(InterCollectionPermissionId),
}

#[ta_objects::typed_tag]
pub enum PermissionIdName {
    #[tag("~")]
    Id(PermissionId),
    #[tag("")]
    Name(PermissionId),
}

#[ta_objects::typed_tag]
pub enum RoleIdName {
    #[tag("~")]
    Id(RoleId),
    #[tag("")]
    Name(RoleName),
}

#[ta_objects::typed_tag]
pub enum TableIdName {
    #[tag("~")]
    Id(TableId),
    #[tag("")]
    Name(TableName),
}

#[ta_objects::typed_tag]
pub enum TransactionIdName {
    #[tag("~")]
    Id(TransactionId),
    #[tag("")]
    Name(TransactionId),
}

#[ta_objects::typed_tag]
pub enum UserIdName {
    #[tag("~")]
    Id(UserId),
    #[tag("")]
    Name(UserName),
}

#[ta_objects::typed_tag]
pub enum WorkerIdName {
    #[tag("~")]
    Id(WorkerId),
    #[tag("")]
    Name(WorkerId),
}
