//
// Copyright 2025 Tabsdata Inc.
//

use ta_objects::{Typed, TypedOpenApi, TypedSerde, TypedSqlx};
#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
#[typed(default = false)]
pub struct AllowDisabled(bool);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
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

/// `/metrics` fact marks. `CurrentAttempt` is false on an attempt a replay
/// superseded; the `EdgeFirst*` pair marks one row per attempt and edge in
/// each dimension, so runs per edge is a count instead of a `COUNT(DISTINCT)`.
#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
#[typed(default = true)]
pub struct CurrentAttempt(bool);

/// Count the attempts a later attempt superseded too.
#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
#[typed(default = false)]
pub struct IncludeReplaced(bool);

/// Count the versions of private tables too.
#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
#[typed(default = false)]
pub struct IncludePrivate(bool);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
#[typed(default = false)]
pub struct RunHasInputs(bool);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
#[typed(default = false)]
pub struct RunHasOutputs(bool);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
#[typed(default = false)]
pub struct EdgeFirst(bool);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
#[typed(default = false)]
pub struct EdgeFirstGroup(bool);

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

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
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

/// Written and removed only by the server, never through the token API.
#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
#[typed(default = false)]
pub struct TokenSystem(bool);
