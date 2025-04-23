//
// Copyright 2025 Tabs Data Inc.
//

use crate::types::basic::{
    AtTime, CollectionId, CollectionName, DependencyId, DependencyPos, DependencyStatus,
    DependencyVersionId, FunctionId, FunctionName, FunctionVersionId, TableId, TableName,
    TableStatus, TableVersionId, TableVersions, TriggerId, TriggerStatus, TriggerVersionId, UserId,
    UserName,
};
use crate::types::dependency::DependencyVersionDBWithNames;
use crate::types::execution::{FunctionVersionNode, TableVersionNode};
use crate::types::table::TableVersionDBWithNames;
use crate::types::trigger::TriggerVersionDBWithNames;
use lazy_static::lazy_static;
use std::ops::Deref;
use td_error::TdError;

lazy_static! {
    static ref COLLECTION_ID: CollectionId = CollectionId::default();
    static ref COLLECTION_NAME: CollectionName = CollectionName::try_from("test").unwrap();
    static ref FUNCTION_VERSION_ID: FunctionVersionId = FunctionVersionId::default();
    static ref TABLE_ID: TableId = TableId::default();
    static ref TABLE_VERSION_ID: TableVersionId = TableVersionId::default();
}

pub fn function_node(
    name: impl TryInto<FunctionName, Error = impl Into<TdError>>,
) -> FunctionVersionNode {
    FunctionVersionNode::builder()
        .collection_id(COLLECTION_ID.deref())
        .collection(COLLECTION_NAME.deref())
        .function_version_id(FUNCTION_VERSION_ID.deref())
        .name(name.try_into().map_err(Into::into).unwrap())
        .build()
        .unwrap()
}

pub fn table_node(name: impl TryInto<TableName, Error = impl Into<TdError>>) -> TableVersionNode {
    TableVersionNode::builder()
        .collection_id(COLLECTION_ID.deref())
        .collection(COLLECTION_NAME.deref())
        .function_version_id(FUNCTION_VERSION_ID.deref())
        .table_id(TABLE_ID.deref())
        .table_version_id(TABLE_VERSION_ID.deref())
        .name(name.try_into().map_err(Into::into).unwrap())
        .build()
        .unwrap()
}

pub async fn table(
    function: impl TryInto<FunctionName, Error = impl Into<TdError>>,
    table: impl TryInto<TableName, Error = impl Into<TdError>>,
) -> TableVersionDBWithNames {
    TableVersionDBWithNames::builder()
        .id(TABLE_VERSION_ID.deref())
        .collection_id(COLLECTION_ID.deref())
        .table_id(TABLE_ID.deref())
        .name(table.try_into().map_err(Into::into).unwrap())
        .function_version_id(FUNCTION_VERSION_ID.deref())
        .function_param_pos(None)
        .defined_on(AtTime::now().await)
        .defined_by_id(UserId::default())
        .status(TableStatus::Active)
        .collection(COLLECTION_NAME.deref())
        .function(function.try_into().map_err(Into::into).unwrap())
        .defined_by(UserName::try_from("joaquin").unwrap())
        .build()
        .unwrap()
}

pub async fn dependency(
    table: impl TryInto<TableName, Error = impl Into<TdError>>,
    function: impl TryInto<FunctionName, Error = impl Into<TdError>>,
) -> DependencyVersionDBWithNames {
    DependencyVersionDBWithNames::builder()
        .id(DependencyVersionId::default())
        .collection_id(COLLECTION_ID.deref())
        .dependency_id(DependencyId::default())
        .function_id(FunctionId::default())
        .function_version_id(FUNCTION_VERSION_ID.deref())
        .table_collection_id(COLLECTION_ID.deref())
        .table_function_version_id(FUNCTION_VERSION_ID.deref())
        .table_id(TABLE_ID.deref())
        .table_version_id(TABLE_VERSION_ID.deref())
        .table_name(table.try_into().map_err(Into::into).unwrap())
        .table_versions(TableVersions::try_from("HEAD").unwrap())
        .dep_pos(DependencyPos::default())
        .status(DependencyStatus::Active)
        .defined_on(AtTime::now().await)
        .defined_by_id(UserId::default())
        .collection(COLLECTION_NAME.deref())
        .function(function.try_into().map_err(Into::into).unwrap())
        .table_collection(COLLECTION_NAME.deref())
        .defined_by(UserName::try_from("joaquin").unwrap())
        .build()
        .unwrap()
}

pub async fn trigger(
    table: impl TryInto<TableName, Error = impl Into<TdError>>,
    function: impl TryInto<FunctionName, Error = impl Into<TdError>>,
) -> TriggerVersionDBWithNames {
    TriggerVersionDBWithNames::builder()
        .id(TriggerVersionId::default())
        .collection_id(COLLECTION_ID.deref())
        .trigger_id(TriggerId::default())
        .function_id(FunctionId::default())
        .function_version_id(FUNCTION_VERSION_ID.deref())
        .trigger_by_collection_id(COLLECTION_ID.deref())
        .trigger_by_function_id(FunctionId::default())
        .trigger_by_function_version_id(FUNCTION_VERSION_ID.deref())
        .trigger_by_table_id(TABLE_ID.deref())
        .trigger_by_table_version_id(TABLE_VERSION_ID.deref())
        .status(TriggerStatus::Active)
        .defined_on(AtTime::now().await)
        .defined_by_id(UserId::default())
        .collection(COLLECTION_NAME.deref())
        .function(function.try_into().map_err(Into::into).unwrap())
        .trigger_by_collection(COLLECTION_NAME.deref())
        .trigger_by_table_name(table.try_into().map_err(Into::into).unwrap())
        .trigger_by_function(FunctionName::try_from("test").unwrap())
        .defined_by(UserName::try_from("joaquin").unwrap())
        .build()
        .unwrap()
}
