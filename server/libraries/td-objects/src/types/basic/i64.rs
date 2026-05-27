//
// Copyright 2025 Tabs Data Inc.
//

#[ta_objects::typed(i64)]
pub struct AccessTokenExpiration;

#[ta_objects::typed(i64)]
#[derive(Eq, PartialEq, Hash)]
pub struct ColumnCount;

#[ta_objects::typed(i64)]
#[derive(Eq, PartialEq, Hash)]
pub struct RowCount;

#[ta_objects::typed(i64)]
#[derive(Eq, PartialEq, Hash)]
pub struct ByteCount;

#[ta_objects::typed(i64)]
#[derive(Eq, PartialEq, Hash)]
pub struct TableVersionCount;

/// UTC epoch ms, floored to a bucket boundary (hour / day / week / month).
/// Used by the operational-metrics endpoints to drive `GROUP BY` directly
/// off indexed columns — no `date_trunc` at query time.
#[ta_objects::typed(i64)]
#[derive(Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct EpochMs;

#[ta_objects::typed(i64(min = 0, default = 0))]
pub struct SampleOffset;

#[ta_objects::typed(i64(min = 0, max = SampleLen::MAX, default = 100))]
pub struct SampleLen;

impl SampleLen {
    pub const MAX: i64 = 1000;
}

#[ta_objects::typed(i64(default = 60))]
pub struct TimeoutThresholdMin;

#[ta_objects::typed(i64(default = 10000))]
pub struct SizeThreshold;
