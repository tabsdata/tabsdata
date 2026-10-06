//
// Copyright 2025 Tabsdata Inc.
//

use ta_objects::{Typed, TypedOpenApi, TypedSerde, TypedSqlx};
#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct AccessTokenExpiration(i64);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Hash)]
pub struct ColumnCount(i64);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Hash)]
pub struct RowCount(i64);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Hash)]
pub struct ByteCount(i64);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Hash)]
pub struct TableVersionCount(i64);

/// Distinct tables behind a set of versions.
#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Hash)]
pub struct TableCount(i64);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Hash)]
pub struct FunctionRunCount(i64);

/// The attempt's ordinal within its run: 0 for the first, one more per replay.
#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Hash)]
#[typed(default = 0)]
pub struct ReplayOrdinal(i64);

/// UTC epoch ms, floored to a bucket boundary (hour / day / week / month).
#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct EpochMs(i64);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
#[typed(min = 0, default = 0)]
pub struct SampleOffset(i64);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
#[typed(min = 0, max = SampleLen::MAX, default = 100)]
pub struct SampleLen(i64);

impl SampleLen {
    pub const MAX: i64 = 1000;
}

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
#[typed(default = 60)]
pub struct TimeoutThresholdMin(i64);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
#[typed(default = 10000)]
pub struct SizeThreshold(i64);

/// Days, or [`ExpiresInDays::NEVER`]; whether never is permitted is decided
/// against the configured ceiling, not here.
#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
#[typed(min = -1)]
pub struct ExpiresInDays(i64);

impl ExpiresInDays {
    pub const NEVER: Self = Self(-1);
}
