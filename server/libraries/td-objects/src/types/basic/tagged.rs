//
// Copyright 2025 Tabsdata Inc.
//

use ta_objects::{TypedOpenApi, TypedSerde, TypedTag};

use crate::types::basic::{
    CollectionId, CollectionName, FunctionId, FunctionName, GroupId, GroupName, ProjectId,
    ProjectName, RoleId, RoleName, TableId, TableName, UserId, UserName,
};

#[derive(TypedTag, TypedSerde, TypedOpenApi)]
pub enum CollectionIdName {
    #[tag("~")]
    Id(CollectionId),
    #[tag("")]
    Name(CollectionName),
}

#[derive(TypedTag, TypedSerde, TypedOpenApi)]
pub enum ProjectIdName {
    #[tag("~")]
    Id(ProjectId),
    #[tag("")]
    Name(ProjectName),
}

#[derive(TypedTag, TypedSerde, TypedOpenApi)]
pub enum FunctionIdName {
    #[tag("~")]
    Id(FunctionId),
    #[tag("")]
    Name(FunctionName),
}

#[derive(TypedTag, TypedSerde, TypedOpenApi)]
pub enum GroupIdName {
    #[tag("~")]
    Id(GroupId),
    #[tag("")]
    Name(GroupName),
}

#[derive(TypedTag, TypedSerde, TypedOpenApi)]
pub enum RoleIdName {
    #[tag("~")]
    Id(RoleId),
    #[tag("")]
    Name(RoleName),
}

#[derive(TypedTag, TypedSerde, TypedOpenApi)]
pub enum TableIdName {
    #[tag("~")]
    Id(TableId),
    #[tag("")]
    Name(TableName),
}

#[derive(TypedTag, TypedSerde, TypedOpenApi)]
pub enum UserIdName {
    #[tag("~")]
    Id(UserId),
    #[tag("")]
    Name(UserName),
}
