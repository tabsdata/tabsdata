//
// Copyright 2025 Tabsdata Inc.
//

use ta_objects::{Typed, TypedOpenApi, TypedSerde, TypedSqlx};
#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
#[typed(default = false)]
pub struct AllowDisabled(bool);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct DataChanged(bool);

impl From<Option<HasData>> for DataChanged {
    fn from(has_data: Option<HasData>) -> Self {
        if let Some(has_data) = has_data {
            DataChanged(*has_data)
        } else {
            DataChanged(false)
        }
    }
}

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
#[typed(default = false)]
pub struct CollectionWhenDeleted(bool);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
#[typed(default = false)]
pub struct ProjectWhenDeleted(bool);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Hash)]
#[typed(default = true)]
pub struct Enabled(bool);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
#[typed(default = false)]
pub struct Fixed(bool);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct FixedRole(bool);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
#[typed(default = false)]
pub struct FunctionOnly(bool);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Hash)]
#[typed(default = false)]
pub struct HasData(bool);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
pub struct IsEmptyVersions(bool);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
#[typed(default = false)]
pub struct OverrideDisabledTrigger(bool);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
pub struct Partitioned(bool);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
#[typed(default = false)]
pub struct PasswordMustChange(bool);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Hash)]
pub struct Private(bool);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
#[typed(default = false)]
pub struct ResetState(bool);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
#[typed(default = false)]
pub struct ResetStage(bool);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
#[typed(default = false)]
pub struct ReuseFrozen(bool);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Hash)]
#[typed(default = false)]
pub struct SchemaChanged(bool);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct SchemaDataChanged(bool);

impl From<Option<SchemaChanged>> for SchemaDataChanged {
    fn from(schema_changed: Option<SchemaChanged>) -> Self {
        if let Some(schema_changed) = schema_changed {
            SchemaDataChanged(*schema_changed)
        } else {
            SchemaDataChanged(false)
        }
    }
}

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SelfDependency(bool);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct SkipIfRunning(bool);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct SysAdmin(bool);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct System(bool);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
#[typed(default = false)]
pub struct UncommittedStatus(bool);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
#[typed(default = true)]
pub struct UserEnabled(bool);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
#[typed(default = false)]
pub struct UserSystem(bool);
