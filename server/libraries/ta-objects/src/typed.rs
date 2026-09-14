//
// Copyright 2026 Tabsdata Inc.
//

//! Support for `#[derive(Typed)]` newtypes and their facet derives: the
//! [`Typed`] trait is the construction gate, [`TypedInner`] abstracts the
//! wrapped primitive, [`TypedError`] is the shared validation error, and
//! the `check_*` helpers implement the `#[typed(...)]` constraints.
//!
//! # The gate invariant
//!
//! Every way a newtype value comes into existence routes through
//! [`Typed::try_new`]:
//!
//! | path                          | route                                  |
//! |-------------------------------|----------------------------------------|
//! | `FromStr`                     | [`from_str_typed`] → `try_new`         |
//! | `TryFrom<String/&str/Inner>`, `try_from =` siblings | `try_new`        |
//! | serde deserialize (`TypedSerde`) | [`deserialize_typed`] → `try_new`   |
//! | sqlx decode (`TypedSqlx`)     | [`decode_typed`] → `try_new`           |
//! | `Default`                     | `try_new(...).expect(...)`; emitted only where it can be valid |
//! | `Typed::into_inner`           | the single door OUT (moving, not a bypass) |
//!
//! The only bypass is a literal `Self(...)` in the declaring module
//! (system constants). Constrained types therefore always hold valid
//! values — including rows read back from the database.

use std::fmt::Display;
use std::ops::Deref;
use td_common::datetime::{IntervalEnd, Timestamp};
use td_common::id::Id;
use td_common::logic_id::{IdL, KeyId};
use td_error::{TdError, td_error};
use utoipa::PartialSchema;
use utoipa::openapi::RefOr;
use utoipa::openapi::schema::Schema;

/// The single construction gate for `#[derive(Typed)]` newtypes: every
/// construction path (FromStr, the TryFrom family, `try_from =` siblings,
/// serde deserialize via `TypedSerde`, sqlx decode via `TypedSqlx`,
/// Default) routes through [`Typed::try_new`], so a held value always
/// satisfies its `#[typed(...)]` constraints. The only bypass is a literal
/// `Self(...)` in the declaring module (system constants). Deriving a stock
/// `Deserialize`/`Decode` instead of the facet derives would skip the gate —
/// don't.
pub trait Typed: Sized {
    type Inner;
    fn try_new(val: impl Into<Self::Inner>) -> Result<Self, TdError>;
    /// The single door out: destructure into the wrapped value (moving, no
    /// clone). Sound — a held value is valid, and extraction is not
    /// construction.
    fn into_inner(self) -> Self::Inner;
}

/// Wraps the same primitive as `O` (every newtype derefs to its inner): an
/// instant of one newtype positioned against a column declared as another.
pub trait SameInner<O: Deref>: crate::SeaValue {}

impl<V, O> SameInner<O> for V
where
    V: Deref + crate::SeaValue,
    O: Deref<Target = V::Target>,
{
}

/// A primitive that a `#[derive(Typed)]` newtype can wrap.
pub trait TypedInner: Clone + Display {
    fn sea_value(&self) -> sea_query::Value;
    fn null_sea_value() -> sea_query::Value;
    fn parse_str(s: &str) -> Result<Self, TdError>;
    fn schema() -> RefOr<Schema>;
}

pub fn from_str_typed<T: Typed>(s: &str) -> Result<T, TdError>
where
    T::Inner: TypedInner,
{
    T::try_new(T::Inner::parse_str(s)?)
}

pub fn deserialize_typed<'de, T: Typed, D: serde::Deserializer<'de>>(d: D) -> Result<T, D::Error>
where
    T::Inner: serde::Deserialize<'de>,
{
    let inner = <T::Inner as serde::Deserialize>::deserialize(d)?;
    T::try_new(inner).map_err(serde::de::Error::custom)
}

pub fn decode_typed<'r, DB: sqlx::Database, T: Typed>(
    value: <DB as sqlx::Database>::ValueRef<'r>,
) -> Result<T, sqlx::error::BoxDynError>
where
    T::Inner: sqlx::Decode<'r, DB>,
{
    let inner = <T::Inner as sqlx::Decode<'r, DB>>::decode(value)?;
    T::try_new(inner).map_err(Into::into)
}

#[td_error]
pub enum TypedError {
    #[error("{0}: '{1}' must have length {2}")]
    Len(&'static str, String, usize) = 0,
    #[error("{0}: '{1}' shorter than min_len {2}")]
    MinLen(&'static str, String, usize) = 1,
    #[error("{0}: '{1}' longer than max_len {2}")]
    MaxLen(&'static str, String, usize) = 2,
    #[error("{0}: '{1}' does not match regex '{2}'")]
    Regex(&'static str, String, String) = 3,
    #[error("{0}: '{1}' < min '{2}'")]
    Min(&'static str, String, String) = 4,
    #[error("{0}: '{1}' > max '{2}'")]
    Max(&'static str, String, String) = 5,
    #[error("{0}: parse error: {1}")]
    Parse(&'static str, String) = 6,
    #[error("{0}: could not parse '{1}', expected: {2}")]
    Expected(&'static str, String, &'static str) = 7,
    #[error("{0}: unknown variant '{1}'")]
    UnknownVariant(&'static str, String) = 8,
    #[error("{0}: no tag matches '{1}'")]
    NoTagMatch(&'static str, String) = 9,
}

// Length constraints count characters — matching the emitted OpenAPI
// `minLength`/`maxLength` and PostgreSQL `varchar(n)` — not bytes.
pub fn check_len(type_name: &'static str, val: &str, len: usize) -> Result<(), TdError> {
    if val.chars().count() != len {
        Err(TypedError::Len(type_name, val.to_string(), len))?;
    }
    Ok(())
}

pub fn check_min_len(type_name: &'static str, val: &str, min_len: usize) -> Result<(), TdError> {
    if val.chars().count() < min_len {
        Err(TypedError::MinLen(type_name, val.to_string(), min_len))?;
    }
    Ok(())
}

pub fn check_max_len(type_name: &'static str, val: &str, max_len: usize) -> Result<(), TdError> {
    if val.chars().count() > max_len {
        Err(TypedError::MaxLen(type_name, val.to_string(), max_len))?;
    }
    Ok(())
}

pub fn apply_parser(
    parser: impl FnOnce(String) -> Result<String, TdError>,
    val: String,
) -> Result<String, TdError> {
    parser(val)
}

pub fn check_min<T: PartialOrd + Display>(
    type_name: &'static str,
    val: &T,
    min: T,
) -> Result<(), TdError> {
    if *val < min {
        Err(TypedError::Min(type_name, val.to_string(), min.to_string()))?;
    }
    Ok(())
}

pub fn check_max<T: PartialOrd + Display>(
    type_name: &'static str,
    val: &T,
    max: T,
) -> Result<(), TdError> {
    if *val > max {
        Err(TypedError::Max(type_name, val.to_string(), max.to_string()))?;
    }
    Ok(())
}

impl TypedInner for String {
    fn sea_value(&self) -> sea_query::Value {
        sea_query::Value::String(Some(self.clone()))
    }
    fn null_sea_value() -> sea_query::Value {
        sea_query::Value::String(None)
    }
    fn parse_str(s: &str) -> Result<Self, TdError> {
        Ok(s.to_string())
    }
    fn schema() -> RefOr<Schema> {
        <String as PartialSchema>::schema()
    }
}

macro_rules! numeric_typed_inner {
    ($ty:ty, $variant:ident) => {
        impl TypedInner for $ty {
            fn sea_value(&self) -> sea_query::Value {
                sea_query::Value::$variant(Some(*self))
            }
            fn null_sea_value() -> sea_query::Value {
                sea_query::Value::$variant(None)
            }
            fn parse_str(s: &str) -> Result<Self, TdError> {
                s.parse().map_err(|e: <$ty as std::str::FromStr>::Err| {
                    TypedError::Parse(stringify!($ty), e.to_string()).into()
                })
            }
            fn schema() -> RefOr<Schema> {
                <$ty as PartialSchema>::schema()
            }
        }
    };
}

numeric_typed_inner!(i16, SmallInt);
numeric_typed_inner!(i32, Int);
numeric_typed_inner!(i64, BigInt);
numeric_typed_inner!(f32, Float);
numeric_typed_inner!(f64, Double);

impl TypedInner for bool {
    fn sea_value(&self) -> sea_query::Value {
        sea_query::Value::Bool(Some(*self))
    }
    fn null_sea_value() -> sea_query::Value {
        sea_query::Value::Bool(None)
    }
    fn parse_str(s: &str) -> Result<Self, TdError> {
        s.parse()
            .map_err(|e: std::str::ParseBoolError| TypedError::Parse("bool", e.to_string()).into())
    }
    fn schema() -> RefOr<Schema> {
        <bool as PartialSchema>::schema()
    }
}

impl TypedInner for Id {
    fn sea_value(&self) -> sea_query::Value {
        sea_query::Value::String(Some(self.to_string()))
    }
    fn null_sea_value() -> sea_query::Value {
        sea_query::Value::String(None)
    }
    fn parse_str(s: &str) -> Result<Self, TdError> {
        Ok(Id::try_from(s)?)
    }
    fn schema() -> RefOr<Schema> {
        utoipa::openapi::ObjectBuilder::new()
            .schema_type(utoipa::openapi::schema::SchemaType::new(
                utoipa::openapi::schema::Type::String,
            ))
            .min_length(Some(td_common::id::ID_LENGTH))
            .max_length(Some(td_common::id::ID_LENGTH))
            .into()
    }
}

impl<K: KeyId> TypedInner for IdL<K> {
    fn sea_value(&self) -> sea_query::Value {
        sea_query::Value::BigInt(Some(self.as_i64()))
    }
    fn null_sea_value() -> sea_query::Value {
        sea_query::Value::BigInt(None)
    }
    fn parse_str(s: &str) -> Result<Self, TdError> {
        Ok(IdL::parse(s)?)
    }
    fn schema() -> RefOr<Schema> {
        <String as PartialSchema>::schema()
    }
}

impl TypedInner for Timestamp {
    fn sea_value(&self) -> sea_query::Value {
        sea_query::Value::ChronoDateTimeUtc(Some(**self))
    }
    fn null_sea_value() -> sea_query::Value {
        sea_query::Value::ChronoDateTimeUtc(None)
    }
    fn parse_str(s: &str) -> Result<Self, TdError> {
        Ok(s.parse::<Timestamp>()?)
    }
    fn schema() -> RefOr<Schema> {
        <i64 as PartialSchema>::schema()
    }
}

impl TypedInner for IntervalEnd {
    // Infinity has no sea-query value (it binds NULL here), which is why the
    // derive emits no condition leaf for interval-end wrappers: the
    // open-interval predicate is the 'infinity' literal, never a bound value.
    fn sea_value(&self) -> sea_query::Value {
        sea_query::Value::ChronoDateTimeUtc(self.at().map(|t| **t))
    }
    fn null_sea_value() -> sea_query::Value {
        sea_query::Value::ChronoDateTimeUtc(None)
    }
    fn parse_str(s: &str) -> Result<Self, TdError> {
        Ok(s.parse::<IntervalEnd>()?)
    }
    fn schema() -> RefOr<Schema> {
        utoipa::openapi::ObjectBuilder::new()
            .schema_type(utoipa::openapi::schema::SchemaType::from_iter([
                utoipa::openapi::schema::Type::Integer,
                utoipa::openapi::schema::Type::Null,
            ]))
            .format(Some(utoipa::openapi::schema::SchemaFormat::KnownFormat(
                utoipa::openapi::schema::KnownFormat::Int64,
            )))
            .into()
    }
}
