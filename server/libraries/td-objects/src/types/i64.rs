//
// Copyright 2025 Tabs Data Inc.
//

use crate::types::timestamp::AtTime;

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

#[td_type::typed(i64(default = default_triggered_on()))]
pub struct TriggeredOnMillis;

fn default_triggered_on() -> i64 {
    AtTime::default().timestamp_millis()
}
