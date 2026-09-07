//
// Copyright 2025 Tabsdata Inc.
//

use ta_objects::{Typed, TypedOpenApi, TypedSerde, TypedSqlx};
#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct ExecutionLimit(i16);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct ExecutionTry(i16);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
#[typed(min = 1)]
pub struct LogsCastNumber(i16);
