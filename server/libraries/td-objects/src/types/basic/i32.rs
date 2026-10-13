//
// Copyright 2025 Tabsdata Inc.
//

use ta_objects::{Typed, TypedOpenApi, TypedSerde, TypedSqlx};
#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[typed(default = 0)]
pub struct DependencyPos(i32);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
#[typed(min = 0, default = 0)]
pub struct Depth(i32);

/// Width of a `/metrics` window in units.
#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
#[typed(min = 1, default = 1)]
pub struct WindowSpan(i32);

impl Depth {
    pub const ZERO: Self = Self(0);
}

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct StatusCount(i32);

/// Times recovery re-dispatched a run on its own since it was planned or a
/// user last recovered or replayed it.
#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
#[typed(min = 0, default = 0)]
pub struct RecoveryCount(i32);

/// Failures in a row of a long-running worker, which space its restarts.
#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
#[typed(min = 0, default = 0)]
pub struct WorkerFailures(i32);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TableFunctionParamPos(i32);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[typed(default = 0)]
pub struct InputIdx(i32);

impl InputIdx {
    /// Input slot that resolved to no data version.
    pub const NO_DATA: Self = Self(-1);
}

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Hash)]
#[typed(default = 0)]
pub struct VersionPos(i32);

impl VersionPos {
    pub const SINGLE_VERSION: Self = Self(-1);
    pub const NO_VERSIONS: Self = Self(-2);
}

/// `(user_id, slot)` is unique, so these bounds cap an account at two tokens.
#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[typed(min = 1, max = 2)]
pub struct TokenSlot(i32);
