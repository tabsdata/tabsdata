//
// Copyright 2025 Tabs Data Inc.
//

#[ta_objects::typed(timestamp, try_from = TriggeredOn)]
#[derive(Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct AtTime;

#[ta_objects::typed(timestamp, try_from = AtTime)]
#[derive(Eq, PartialEq)]
pub struct PasswordChangeTime;

#[ta_objects::typed(timestamp, try_from = AtTime)]
#[derive(Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TriggeredOn;
