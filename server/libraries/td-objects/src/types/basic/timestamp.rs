//
// Copyright 2025 Tabsdata Inc.
//

use ta_objects::{Typed, TypedOpenApi, TypedSerde, TypedSqlx};
use td_common::datetime::Timestamp;
#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[typed(try_from = TriggeredOn)]
pub struct AtTime(Timestamp);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
#[typed(try_from = AtTime)]
pub struct PasswordChangeTime(Timestamp);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[typed(try_from = AtTime)]
pub struct TriggeredOn(Timestamp);
