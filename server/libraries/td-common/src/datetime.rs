//
// Copyright 2025 Tabsdata Inc.
//

use crate::time::UniqueUtc;
use chrono::{DateTime, Utc};
use std::fmt::Display;
use std::ops::Deref;
use std::str::FromStr;
use td_error::td_error;

#[td_error]
pub enum TimestampError {
    #[error("Invalid timestamp: {0}")]
    InvalidTimestampMillis(i64) = 0,
    #[error("Invalid timestamp: {0}")]
    InvalidTimestamp(String) = 1,
    #[error("Invalid timestamp (microseconds since the PostgreSQL epoch): {0}")]
    InvalidTimestampMicros(i64) = 2,
}

/// Timestamp implementation with serde/deser using milliseconds.
/// Sqlx encoding still preserves original Datetime encoding.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, sqlx::Type)]
#[sqlx(transparent)]
pub struct Timestamp(DateTime<Utc>);

impl Default for Timestamp {
    fn default() -> Self {
        Timestamp(UniqueUtc::now_millis())
    }
}

impl Deref for Timestamp {
    type Target = DateTime<Utc>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<DateTime<Utc>> for Timestamp {
    fn from(dt: DateTime<Utc>) -> Self {
        Timestamp(dt)
    }
}

impl TryFrom<i64> for Timestamp {
    type Error = TimestampError;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        Ok(Self(
            DateTime::<Utc>::from_timestamp_millis(value)
                .ok_or(TimestampError::InvalidTimestampMillis(value))?,
        ))
    }
}

impl Display for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.timestamp_millis())
    }
}

impl FromStr for Timestamp {
    type Err = TimestampError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let date = if let Ok(dt) = s.parse::<i64>() {
            match DateTime::from_timestamp_millis(dt) {
                Some(dt) => Ok(dt),
                None => Err(TimestampError::InvalidTimestampMillis(dt)),
            }
        } else {
            s.parse::<DateTime<Utc>>()
                .map_err(|_| TimestampError::InvalidTimestamp(s.to_string()))
        }?;

        Ok(Timestamp(date))
    }
}

/// End of a validity interval: a point in time, or open (`infinity`).
/// Hand-rolled codec: chrono cannot represent `timestamptz` `infinity`.
/// Serde is epoch-millis with `null` for the open interval.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Default)]
pub enum IntervalEnd {
    At(Timestamp),
    #[default]
    Infinity,
}

// Binary timestamptz is microseconds since 2000-01-01Z; i64::MAX = infinity.
const PG_EPOCH_MICROS: i64 = 946_684_800_000_000;

impl IntervalEnd {
    /// No client-side value type carries `infinity`, so it travels as this
    /// literal.
    pub const SQL_INFINITY: &str = "'infinity'::timestamptz";

    pub fn at(&self) -> Option<&Timestamp> {
        match self {
            IntervalEnd::At(t) => Some(t),
            IntervalEnd::Infinity => None,
        }
    }

    pub fn is_infinity(&self) -> bool {
        matches!(self, IntervalEnd::Infinity)
    }
}

impl From<Timestamp> for IntervalEnd {
    fn from(t: Timestamp) -> Self {
        IntervalEnd::At(t)
    }
}

impl Display for IntervalEnd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IntervalEnd::At(t) => write!(f, "{t}"),
            IntervalEnd::Infinity => write!(f, "infinity"),
        }
    }
}

impl FromStr for IntervalEnd {
    type Err = TimestampError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "infinity" {
            Ok(IntervalEnd::Infinity)
        } else {
            Ok(IntervalEnd::At(s.parse()?))
        }
    }
}

impl serde::Serialize for IntervalEnd {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            IntervalEnd::At(t) => s.serialize_some(t),
            IntervalEnd::Infinity => s.serialize_none(),
        }
    }
}

impl<'de> serde::Deserialize<'de> for IntervalEnd {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Ok(match Option::<Timestamp>::deserialize(d)? {
            Some(t) => IntervalEnd::At(t),
            None => IntervalEnd::Infinity,
        })
    }
}

impl sqlx::Type<sqlx::Postgres> for IntervalEnd {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <DateTime<Utc> as sqlx::Type<sqlx::Postgres>>::type_info()
    }
    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        <DateTime<Utc> as sqlx::Type<sqlx::Postgres>>::compatible(ty)
    }
}

impl sqlx::postgres::PgHasArrayType for IntervalEnd {
    fn array_type_info() -> sqlx::postgres::PgTypeInfo {
        <DateTime<Utc> as sqlx::postgres::PgHasArrayType>::array_type_info()
    }
}

impl<'r> sqlx::Decode<'r, sqlx::Postgres> for IntervalEnd {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        match value.format() {
            sqlx::postgres::PgValueFormat::Binary => {
                let micros = i64::from_be_bytes(value.as_bytes()?.try_into()?);
                if micros == i64::MAX {
                    return Ok(IntervalEnd::Infinity);
                }
                let dt = micros
                    .checked_add(PG_EPOCH_MICROS)
                    .and_then(DateTime::<Utc>::from_timestamp_micros)
                    .ok_or(TimestampError::InvalidTimestampMicros(micros))?;
                Ok(IntervalEnd::At(Timestamp::from(dt)))
            }
            sqlx::postgres::PgValueFormat::Text => {
                let s = value.as_str()?;
                if s == "infinity" {
                    return Ok(IntervalEnd::Infinity);
                }
                let dt = s
                    .parse::<DateTime<Utc>>()
                    .or_else(|_| {
                        DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f%#z")
                            .map(|d| d.with_timezone(&Utc))
                    })
                    .map_err(|_| TimestampError::InvalidTimestamp(s.to_string()))?;
                Ok(IntervalEnd::At(Timestamp::from(dt)))
            }
        }
    }
}

impl<'q> sqlx::Encode<'q, sqlx::Postgres> for IntervalEnd {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        match self {
            IntervalEnd::At(t) => {
                <DateTime<Utc> as sqlx::Encode<'q, sqlx::Postgres>>::encode_by_ref(t, buf)
            }
            IntervalEnd::Infinity => {
                buf.extend_from_slice(&i64::MAX.to_be_bytes());
                Ok(sqlx::encode::IsNull::No)
            }
        }
    }
}

impl<'de> serde::Deserialize<'de> for Timestamp {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = i64::deserialize(d)?;
        let s = DateTime::from_timestamp_millis(s)
            .ok_or_else(|| serde::de::Error::custom("Invalid timestamp"))?;
        Ok(Timestamp(s))
    }
}

impl serde::Serialize for Timestamp {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        self.timestamp_millis().serialize(s)
    }
}
