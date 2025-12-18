//
// Copyright 2025 Tabs Data Inc.
//

#[td_type::typed(i64)]
pub struct AccessTokenExpiration;

#[td_type::typed(i64)]
pub struct ColumnCount;

#[td_type::typed(i64)]
pub struct RowCount;

#[td_type::typed(i64(min = 0, default = 0))]
pub struct SampleOffset;

#[td_type::typed(i64(min = 0, max = SampleLen::MAX, default = 100))]
pub struct SampleLen;

impl SampleLen {
    pub const MAX: i64 = 1000;
}

#[td_type::typed(i64(default = 60))]
pub struct TimeoutThresholdMin;

#[td_type::typed(i64(default = 10000))]
pub struct SizeThreshold;
