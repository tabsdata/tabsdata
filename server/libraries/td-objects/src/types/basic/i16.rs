//
// Copyright 2025 Tabsdata Inc.
//

#[ta_objects::typed(i16)]
pub struct ExecutionLimit;

#[ta_objects::typed(i16)]
pub struct ExecutionTry;

#[ta_objects::typed(i16(min = 1))]
pub struct LogsCastNumber;
