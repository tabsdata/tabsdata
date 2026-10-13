//
// Copyright 2026 Tabsdata Inc.
//

//! Column brands: same-typed columns distinguished by row role
//! (`Created<UserId>` vs `Modified<UserId>`). A closed vocabulary, not an
//! ad-hoc tagging mechanism.

use ta_objects::{Typed, TypedOpenApi, TypedSerde, TypedSqlx};

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct Created<T>(T);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct Modified<T>(T);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct Added<T>(T);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct Started<T>(T);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct Ended<T>(T);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct Dispatched<T>(T);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct Expires<T>(T);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct Retry<T>(T);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct DestinationWrite<T>(T);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct StatusChange<T>(T);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct EndedHour<T>(T);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct EndedDay<T>(T);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct EndedWeek<T>(T);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct EndedMonth<T>(T);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct TriggerBy<T>(T);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct OfTable<T>(T);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct Parent<T>(T);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct Patch<T>(T);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct WithData<T>(T);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct WhenDeleted<T>(T);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct OfExecution<T>(T);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct OfFunction<T>(T);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct LastUsed<T>(T);
