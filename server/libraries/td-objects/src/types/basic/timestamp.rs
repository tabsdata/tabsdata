//
// Copyright 2025 Tabs Data Inc.
//

#[td_type::typed(timestamp, try_from = TriggeredOn)]
pub struct AtTime;

#[td_type::typed(timestamp, try_from = AtTime)]
pub struct PasswordChangeTime;

#[td_type::typed(timestamp, try_from = AtTime)]
pub struct TriggeredOn;
