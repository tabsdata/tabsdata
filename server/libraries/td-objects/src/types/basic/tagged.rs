//
// Copyright 2025 Tabsdata Inc.
//

use crate::types::basic::{
    CollectionId, CollectionName, FunctionId, FunctionName, GroupId, GroupName, ProjectId,
    ProjectName, RoleId, RoleName, TableId, TableName, UserId, UserName,
};

#[ta_objects::typed_tag]
pub enum CollectionIdName {
    #[tag("~")]
    Id(CollectionId),
    #[tag("")]
    Name(CollectionName),
}

#[ta_objects::typed_tag]
pub enum ProjectIdName {
    #[tag("~")]
    Id(ProjectId),
    #[tag("")]
    Name(ProjectName),
}

#[ta_objects::typed_tag]
pub enum FunctionIdName {
    #[tag("~")]
    Id(FunctionId),
    #[tag("")]
    Name(FunctionName),
}

#[ta_objects::typed_tag]
pub enum GroupIdName {
    #[tag("~")]
    Id(GroupId),
    #[tag("")]
    Name(GroupName),
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
pub enum UserIdName {
    #[tag("~")]
    Id(UserId),
    #[tag("")]
    Name(UserName),
}
