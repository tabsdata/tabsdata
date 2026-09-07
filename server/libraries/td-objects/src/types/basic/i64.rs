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

/// UTC epoch ms, floored to a bucket boundary (hour / day / week / month).
/// Used by the operational-metrics endpoints to drive `GROUP BY` directly
/// off indexed columns — no `date_trunc` at query time.
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
