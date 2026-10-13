//
// Copyright 2026 Tabsdata Inc.
//
//! This module provides an ID implementation with human friendly representation
//! '<KEY><YYYYMMDD>-<COUNTER>'. The `<KEY>` prefix is always lowercase.
//!
//! The module also includes an `IdLGenerator` for creating new [`IdL`] identifiers in a
//! thread-safe manner, ensuring uniqueness and chronological ordering.
//!
//! The DB representation of a logical ID is the `i64` whose digits are
//! '<YYYYMMDD><COUNTER>', with `<COUNTER>` being a 10 digit zero padded number. Numeric
//! order is therefore chronological order.
//!
//! The JSON (REST API) generated representation is the '<KEY><YYYYMMDD>-<COUNTER>' string
//! without the `<COUNTER>` padded, but it accepts both padded and unpadded.
//!
//! Key features:
//! - **Type-safe ID kinds**: `KeyId` trait allows distinguishing different logical IDs at
//!   compile time.
//! - **Structured logical IDs**: Composed of date, key, and counter for better organization.
//! - **ID Generation**: `IdLGenerator` for creating new, unique logical IDs.
//! - **Serialization/Deserialization**: Supports `serde` for JSON (string) and `sqlx` for
//!   database (`i64`) interaction.

use chrono::{Datelike, NaiveDate, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sqlx::encode::IsNull;
use sqlx::error::BoxDynError;
use sqlx::postgres::{PgHasArrayType, PgTypeInfo};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use td_error::{TdError, td_error};

const MAX_KEY_LEN: usize = 3;
const DATE_LEN: usize = 8;

const SEPARATOR: char = '-';
const ID_PADDING: usize = 10;
const MAX_IDL_LEN: usize = MAX_KEY_LEN + DATE_LEN + 1 + ID_PADDING;

/// Scale separating the date digits from the counter digits in the `i64` DB form.
const COUNTER_SCALE: i64 = 10_i64.pow(ID_PADDING as u32);

/// Errors that can occur when working with logical IDs.
#[td_error]
pub enum IdLError {
    /// The key provided for a `KeyId` is invalid.
    #[error("Invalid KeyId key, it must be 1 to 3 lowercase characters: {0}")]
    InvalidKey(String) = 0,
    /// The given string cannot be parsed into a logical ID.
    #[error("Invalid ID, not a '{0}' logical ID: {1}")]
    CannotParse(String, String) = 1,
    /// The given database value does not decode into a logical ID.
    #[error("Invalid '{0}' logical ID database value: {1}")]
    CannotDecode(String, i64) = 2,
}

/// A trait for defining a key that identifies a logical ID type.
///
/// This trait is used to provide a short, unique string identifier for different kinds of
/// logical IDs. The key must be between 1 and 3 lowercase alphabetic characters.
pub trait KeyId: Clone {
    /// Returns the key for the logical ID.
    ///
    /// The key must be 1 to 3 lowercase alphabetic characters.
    fn key() -> &'static str;
}

/// A logical identifier `<KEY><YYYYMMDD>-<COUNTER>`.
///
/// The type parameter `K` must implement the `KeyId` trait, which provides a unique key for
/// each logical ID type.
///
/// When serializing/deserializing, an UNPADDED ID string is used.
/// When encoding/decoding with sqlx, the `<YYYYMMDD><COUNTER>` `i64` is used.
/// Type K enforces validation during Deserialization.
#[derive(Debug, Clone)]
pub struct IdL<K: KeyId> {
    date: NaiveDate,
    counter: usize,
    _marker: PhantomData<K>,
}

impl<K: KeyId> IdL<K> {
    /// Returns the date part of the ID.
    pub fn date(&self) -> NaiveDate {
        self.date
    }

    /// Returns the counter part of the ID.
    pub fn counter(&self) -> usize {
        self.counter
    }

    /// Returns the unpadded string representation of the ID, the REST form.
    pub fn unpadded_str(&self) -> String {
        format!(
            "{}{}{SEPARATOR}{}",
            K::key(),
            self.date.format("%Y%m%d"),
            self.counter
        )
    }

    /// Returns the padded string representation of the ID, ordering as a string.
    pub fn padded_str(&self) -> String {
        format!(
            "{}{}{SEPARATOR}{:0width$}",
            K::key(),
            self.date.format("%Y%m%d"),
            self.counter,
            width = ID_PADDING
        )
    }

    /// Returns the `<YYYYMMDD><COUNTER>` `i64`, the at-rest form.
    pub fn as_i64(&self) -> i64 {
        date_digits(&self.date) * COUNTER_SCALE + self.counter as i64
    }

    /// Parses the `<YYYYMMDD><COUNTER>` `i64` at-rest form into an `IdL<K>`.
    pub fn from_i64(value: i64) -> Result<Self, IdLError> {
        let error = || IdLError::CannotDecode(K::key().to_string(), value);
        if value < 0 {
            return Err(error());
        }
        let date = date_from_digits(value / COUNTER_SCALE).ok_or_else(error)?;
        Ok(Self::new(date, (value % COUNTER_SCALE) as usize))
    }

    /// Parses a string into an `IdL<K>`.
    ///
    /// The string must be a `<KEY><YYYYMMDD>-<COUNTER>` logical ID, with the counter either
    /// padded or unpadded.
    pub fn parse(s: &str) -> Result<Self, IdLError> {
        let key = K::key();
        let error = || IdLError::CannotParse(key.to_string(), s.to_string());

        if s.len() < key.len() + DATE_LEN + 1 || s.len() > MAX_IDL_LEN {
            return Err(error());
        }

        let key_str = s.get(0..key.len()).ok_or_else(error)?;
        let date_str = s.get(key.len()..key.len() + DATE_LEN).ok_or_else(error)?;
        let separator = s
            .get(key.len() + DATE_LEN..key.len() + DATE_LEN + 1)
            .ok_or_else(error)?;
        let counter_str = s.get(key.len() + DATE_LEN + 1..).ok_or_else(error)?;

        if !key_str.eq_ignore_ascii_case(key) || separator != SEPARATOR.to_string() {
            return Err(error());
        }

        let date = NaiveDate::parse_from_str(date_str, "%Y%m%d").map_err(|_| error())?;
        let counter = usize::from_str(counter_str).map_err(|_| error())?;

        Ok(Self::new(date, counter))
    }

    /// Creates a new logical ID from a date and a counter.
    pub fn new(date: NaiveDate, counter: usize) -> Self {
        debug_assert!(
            (counter as i64) < COUNTER_SCALE,
            "counter overflows the {ID_PADDING} digit counter: {counter}"
        );
        Self {
            date,
            counter,
            _marker: PhantomData,
        }
    }
}

/// `YYYYMMDD` as an integer.
fn date_digits(date: &NaiveDate) -> i64 {
    date.year() as i64 * 10_000 + date.month() as i64 * 100 + date.day() as i64
}

/// The inverse of [`date_digits`], `None` when the digits are not a valid date.
fn date_from_digits(digits: i64) -> Option<NaiveDate> {
    let year = i32::try_from(digits / 10_000).ok()?;
    let month = u32::try_from(digits / 100 % 100).ok()?;
    let day = u32::try_from(digits % 100).ok()?;
    NaiveDate::from_ymd_opt(year, month, day)
}

impl<K: KeyId> PartialEq for IdL<K> {
    fn eq(&self, other: &Self) -> bool {
        (self.date, self.counter).eq(&(other.date, other.counter))
    }
}

impl<K: KeyId> Eq for IdL<K> {}

#[allow(clippy::non_canonical_partial_ord_impl)]
impl<K: KeyId> PartialOrd for IdL<K> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        (self.date, self.counter).partial_cmp(&(other.date, other.counter))
    }
}

impl<K: KeyId> Ord for IdL<K> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.date, self.counter).cmp(&(other.date, other.counter))
    }
}

impl<K: KeyId> Hash for IdL<K> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (self.date, self.counter).hash(state)
    }
}

impl<K: KeyId> fmt::Display for IdL<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.padded_str())
    }
}

impl<K: KeyId> TryFrom<&String> for IdL<K> {
    type Error = TdError;

    fn try_from(value: &String) -> Result<Self, Self::Error> {
        Ok(Self::parse(value)?)
    }
}

impl<K: KeyId> From<&IdL<K>> for IdL<K> {
    fn from(value: &IdL<K>) -> Self {
        value.clone()
    }
}

// --- IdL: Serialize / Deserialize (Unpadded string) ---

impl<K: KeyId> Serialize for IdL<K> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.unpadded_str())
    }
}

impl<'de, K: KeyId> Deserialize<'de> for IdL<K> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(&value).map_err(serde::de::Error::custom)
    }
}

// --- IdL: Encode / Decode (i64) ---
impl<DB: sqlx::Database, K: KeyId> sqlx::Type<DB> for IdL<K>
where
    i64: sqlx::Type<DB>,
{
    fn type_info() -> <DB as sqlx::Database>::TypeInfo {
        <i64 as sqlx::Type<DB>>::type_info()
    }

    fn compatible(ty: &<DB as sqlx::Database>::TypeInfo) -> bool {
        <i64 as sqlx::Type<DB>>::compatible(ty)
    }
}

impl<'q, DB: sqlx::Database, K: KeyId> sqlx::Encode<'q, DB> for IdL<K>
where
    i64: sqlx::Encode<'q, DB>,
{
    fn encode_by_ref(
        &self,
        buf: &mut <DB as sqlx::Database>::ArgumentBuffer,
    ) -> Result<IsNull, BoxDynError> {
        <i64 as sqlx::Encode<'q, DB>>::encode_by_ref(&self.as_i64(), buf)
    }
}

impl<'r, DB: sqlx::Database, K: KeyId> sqlx::Decode<'r, DB> for IdL<K>
where
    i64: sqlx::Decode<'r, DB>,
{
    fn decode(
        value: <DB as sqlx::Database>::ValueRef<'r>,
    ) -> Result<Self, Box<dyn std::error::Error + 'static + Send + Sync>> {
        let value = <i64 as sqlx::Decode<'r, DB>>::decode(value)?;
        Ok(Self::from_i64(value)?)
    }
}

impl<K: KeyId> PgHasArrayType for IdL<K> {
    fn array_type_info() -> PgTypeInfo {
        <i64 as PgHasArrayType>::array_type_info()
    }
    fn array_compatible(ty: &PgTypeInfo) -> bool {
        <i64 as PgHasArrayType>::array_compatible(ty)
    }
}

/// Renders a logical ID in the padded, string form.
///
/// Serde writes the unpadded form, which is the REST representation meant for
/// human consumption. Documents whose IDs are read back as plain strings —
/// the worker request YAMLs — need the padded form instead, so that ordering
/// the string orders the ID. Use it through the [`padded`] and
/// [`padded_option`] serde adapters.
pub trait PaddedIdL {
    fn padded(&self) -> String;
}

impl<K: KeyId> PaddedIdL for IdL<K> {
    fn padded(&self) -> String {
        self.padded_str()
    }
}

/// `#[serde(serialize_with = "td_common::logic_id::padded")]` for a logical ID.
///
/// Serialization only: [`IdL::parse`] already reads both forms back.
pub fn padded<T, S>(value: &T, serializer: S) -> Result<S::Ok, S::Error>
where
    T: PaddedIdL,
    S: Serializer,
{
    serializer.serialize_str(&value.padded())
}

/// [`padded`] for an `Option<logical ID>`.
pub fn padded_option<T, S>(value: &Option<T>, serializer: S) -> Result<S::Ok, S::Error>
where
    T: PaddedIdL,
    S: Serializer,
{
    match value {
        Some(value) => serializer.serialize_some(&value.padded()),
        None => serializer.serialize_none(),
    }
}

/// The internal state of the `IdLGenerator`.
#[derive(Debug)]
struct GeneratorState {
    /// The date of the last generated ID.
    last_date: Option<NaiveDate>,
    /// The counter of the last generated ID.
    counter: usize,
}

/// A thread-safe generator for creating new `IdL<K>` identifiers.
///
/// The generator ensures that each new ID is unique by maintaining an internal counter
/// that is reset daily. It can be initialized with the latest known ID to continue
/// the sequence from that point.
#[derive(Debug, Clone)]
pub struct IdLGenerator<K: KeyId> {
    state: Arc<Mutex<GeneratorState>>,
    _marker: PhantomData<K>,
}

impl<K: KeyId> IdLGenerator<K> {
    /// Creates a new `IdLGenerator`.
    ///
    /// The generator can be initialized with an optional `latest` ID. If `latest` is provided,
    /// the generator will start creating new IDs with a counter value greater than that of `latest`.
    /// If `latest` is `None`, the counter starts at 1.
    ///
    /// # Errors
    ///
    /// Returns an `IdLError::InvalidKey` if the key from `K::key()` is invalid.
    pub fn new(latest: &Option<IdL<K>>) -> Result<Self, IdLError> {
        let key = K::key();

        if key.is_empty()
            || key.chars().count() > MAX_KEY_LEN
            || !key.chars().all(|c| c.is_ascii_lowercase())
        {
            return Err(IdLError::InvalidKey(key.to_string()));
        }

        let (date, counter) = match latest {
            Some(latest) => (Some(latest.date()), latest.counter()),
            None => (None, 0),
        };

        Ok(IdLGenerator {
            state: Arc::new(Mutex::new(GeneratorState {
                last_date: date,
                counter,
            })),
            _marker: PhantomData,
        })
    }
}

impl<K: KeyId> IdLGenerator<K> {
    /// Returns the next date and sequence number. A date before the latest
    /// one issued (a clock stepped back, or a restart on a host behind)
    /// stays on the latest: ids only ever grow, so none is issued twice.
    fn next_sequence(&self, current_date: NaiveDate) -> (NaiveDate, usize) {
        let mut state = self.state.lock().unwrap();

        match state.last_date {
            Some(last) if current_date > last => {
                state.counter = 1;
                state.last_date = Some(current_date);
            }
            Some(_) => {
                state.counter += 1;
            }
            None => {
                state.counter = 1;
                state.last_date = Some(current_date);
            }
        }

        (state.last_date.expect("set above"), state.counter)
    }

    /// Generates a new `IdL<K>` for a specific date.
    fn _generate(&self, current_date: NaiveDate) -> IdL<K> {
        let (date, counter) = self.next_sequence(current_date);
        IdL::new(date, counter)
    }

    /// Generates a new `IdL<K>` using the current UTC date.
    pub fn generate(&self) -> IdL<K> {
        self._generate(Utc::now().date_naive())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id;
    use chrono::{Days, NaiveDate};

    #[derive(Debug, Clone)]
    struct TestKey;
    impl KeyId for TestKey {
        fn key() -> &'static str {
            "tst"
        }
    }

    type TestId = IdL<TestKey>;

    #[test]
    fn test_parse_rejects_legacy_id() {
        let id_str = id::id().to_string();
        let err = TestId::parse(&id_str).unwrap_err();
        assert!(matches!(err, IdLError::CannotParse(_, _)));
    }

    #[test]
    fn test_parse_logic_id_unpadded() {
        let id_str = "TST20240101-123";
        let id = TestId::parse(id_str).unwrap();
        assert_eq!(id.date(), NaiveDate::from_ymd_opt(2024, 1, 1).unwrap());
        assert_eq!(id.counter(), 123);
        assert_eq!(id.unpadded_str(), "tst20240101-123");
        assert_eq!(id.padded_str(), "tst20240101-0000000123");
    }

    #[test]
    fn test_parse_logic_id_padded() {
        let id_str = "TST20240101-0000000123";
        let id = TestId::parse(id_str).unwrap();
        assert_eq!(id.date(), NaiveDate::from_ymd_opt(2024, 1, 1).unwrap());
        assert_eq!(id.counter(), 123);
        assert_eq!(id.unpadded_str(), "tst20240101-123");
        assert_eq!(id.padded_str(), "tst20240101-0000000123");
    }

    #[test]
    fn test_parse_invalid_short_id() {
        let id_str = "20240101";
        let err = TestId::parse(id_str).unwrap_err();
        assert!(matches!(err, IdLError::CannotParse(_, _)));
    }

    #[test]
    fn test_parse_invalid_date() {
        let id_str = "TST20241301-123"; // Invalid month
        let err = TestId::parse(id_str).unwrap_err();
        assert!(matches!(err, IdLError::CannotParse(_, _)));
    }

    #[test]
    fn test_parse_invalid_key() {
        let id_str = "inv20240101-123";
        let err = TestId::parse(id_str).unwrap_err();
        assert!(matches!(err, IdLError::CannotParse(_, _)));
    }

    #[test]
    fn test_parse_invalid_separator() {
        let id_str = "tst20240101x123";
        let err = TestId::parse(id_str).unwrap_err();
        assert!(matches!(err, IdLError::CannotParse(_, _)));
    }

    #[test]
    fn test_parse_invalid_counter() {
        let id_str = "tst20240101-abc";
        let err = TestId::parse(id_str).unwrap_err();
        assert!(matches!(err, IdLError::CannotParse(_, _)));
    }

    #[test]
    fn test_parse_multibyte_char_does_not_panic() {
        let id_str = "tst2024010\u{20ac}1";
        let err = TestId::parse(id_str).unwrap_err();
        assert!(matches!(err, IdLError::CannotParse(_, _)));
    }

    #[test]
    fn test_generator_new() {
        let generator = IdLGenerator::<TestKey>::new(&None).unwrap();
        let id = generator.generate();
        assert_eq!(id.counter(), 1);
    }

    #[test]
    fn test_generator_new_with_same_date() {
        let date = Utc::now().date_naive();
        let latest_id = TestId::new(date, 10);
        let generator = IdLGenerator::<TestKey>::new(&Some(latest_id)).unwrap();
        // using _generate() to ensure we are in the same date
        let id = generator._generate(date);
        assert_eq!(id.date(), date);
        assert_eq!(id.counter(), 11);
    }

    #[test]
    fn test_generator_new_with_newer_date() {
        let generator_date = Utc::now().date_naive();
        let latest_id = TestId::new(generator_date, 10);
        let generator = IdLGenerator::<TestKey>::new(&Some(latest_id)).unwrap();
        let current_date = generator_date.checked_add_days(Days::new(1)).unwrap();
        // using _generate() to ensure we are in the same date
        let id = generator._generate(current_date);
        assert_eq!(id.date(), current_date);
        assert_eq!(id.counter(), 1);
    }

    #[test]
    fn test_generator_generate_multiple() {
        let generator = IdLGenerator::<TestKey>::new(&None).unwrap();
        // using _generate() to ensure we are in the same date
        let date = Utc::now().date_naive();
        let id1 = generator._generate(date);
        let id2 = generator._generate(date);
        assert_eq!(id1.counter(), 1);
        assert_eq!(id2.counter(), 2);
        assert!(id1 < id2);
    }

    /// Ids from D1 were issued, then one from D2, and the date then
    /// goes back to D1 (a clock step back, or a restart onto a host whose
    /// clock is behind, resuming from the latest persisted id). No id may be
    /// issued twice, in the same process or after the restart.
    #[test]
    fn test_generator_does_not_reissue_ids_when_the_date_goes_back() {
        let d1 = NaiveDate::from_ymd_opt(2026, 10, 4).unwrap();
        let d2 = d1.checked_add_days(Days::new(1)).unwrap();
        let generator = IdLGenerator::<TestKey>::new(&None).unwrap();
        let issued: Vec<TestId> = vec![
            generator._generate(d1),
            generator._generate(d1),
            generator._generate(d1),
            generator._generate(d2),
        ];
        let latest = issued.last().cloned();

        let stepped_back = generator._generate(d1);
        let restarted = IdLGenerator::<TestKey>::new(&latest).unwrap()._generate(d1);
        for (when, id) in [
            ("after a step back", stepped_back),
            ("after a restart", restarted),
        ] {
            assert!(
                !issued.contains(&id),
                "{when} the generator reissued {id}, already issued in {issued:?}"
            );
        }
    }

    #[test]
    fn test_generator_invalid_key() {
        #[derive(Debug, Clone)]
        struct BadKey;
        impl KeyId for BadKey {
            fn key() -> &'static str {
                "TOOLONG"
            }
        }
        let err = IdLGenerator::<BadKey>::new(&None).unwrap_err();
        assert!(matches!(err, IdLError::InvalidKey(_)));
    }

    #[test]
    fn test_serialization_is_unpadded() {
        let id = logic(123);
        assert_eq!(serde_json::to_string(&id).unwrap(), r#""tst20240101-123""#);
    }

    #[test]
    fn test_deserialization_unpadded() {
        let json = r#""TST20240101-123""#;
        let id: TestId = serde_json::from_str(json).unwrap();
        assert_eq!(id.counter(), 123);
        assert_eq!(id.padded_str(), "tst20240101-0000000123");
    }

    #[test]
    fn test_deserialization_padded() {
        let json = r#""TST20240101-0000000123""#;
        let id: TestId = serde_json::from_str(json).unwrap();
        assert_eq!(id.counter(), 123);
        assert_eq!(id.padded_str(), "tst20240101-0000000123");
    }

    #[test]
    fn test_i64_digits_are_the_date_and_the_padded_counter() {
        let id = logic(123);
        assert_eq!(id.as_i64().to_string(), "202401010000000123");
    }

    #[test]
    fn test_i64_round_trip() {
        let id = logic(123);
        assert_eq!(TestId::from_i64(id.as_i64()).unwrap(), id);
    }

    #[test]
    fn test_i64_rejects_invalid_values() {
        // Negative, no date digits, and a 13th month.
        for value in [-1, 0, 202413010000000001] {
            let err = TestId::from_i64(value).unwrap_err();
            assert!(matches!(err, IdLError::CannotDecode(_, _)), "{value}");
        }
    }

    #[test]
    fn test_i64_orders_chronologically() {
        let ids = [
            TestId::new(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(), 9),
            TestId::new(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(), 10),
            TestId::new(NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(), 1),
            TestId::new(NaiveDate::from_ymd_opt(2024, 2, 1).unwrap(), 1),
        ];
        let mut sorted = ids.clone();
        sorted.sort();
        assert_eq!(ids, sorted);

        let values: Vec<i64> = ids.iter().map(TestId::as_i64).collect();
        let mut values_sorted = values.clone();
        values_sorted.sort();
        assert_eq!(values, values_sorted);
    }

    #[derive(Serialize)]
    struct PaddedHolder {
        #[serde(serialize_with = "padded")]
        id: TestId,
        #[serde(serialize_with = "padded_option")]
        maybe: Option<TestId>,
    }

    fn logic(counter: usize) -> TestId {
        TestId::new(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(), counter)
    }

    #[test]
    fn test_padded_adapter_pads_where_plain_serde_does_not() {
        let holder = PaddedHolder {
            id: logic(123),
            maybe: Some(logic(7)),
        };
        assert_eq!(
            serde_json::to_string(&holder).unwrap(),
            r#"{"id":"tst20240101-0000000123","maybe":"tst20240101-0000000007"}"#
        );
        assert_eq!(
            serde_json::to_string(&holder.id).unwrap(),
            r#""tst20240101-123""#
        );
    }

    #[test]
    fn test_padded_option_adapter_keeps_none() {
        let holder = PaddedHolder {
            id: logic(1),
            maybe: None,
        };
        assert_eq!(
            serde_json::to_string(&holder).unwrap(),
            r#"{"id":"tst20240101-0000000001","maybe":null}"#
        );
    }

    #[test]
    fn test_padded_output_orders_as_a_string() {
        let counters = [1usize, 2, 9, 10, 100];

        let padded: Vec<String> = counters.iter().map(|c| logic(*c).padded()).collect();
        let mut padded_sorted = padded.clone();
        padded_sorted.sort();
        assert_eq!(padded, padded_sorted);

        // Which is exactly what the unpadded REST form cannot do.
        let unpadded: Vec<String> = counters.iter().map(|c| logic(*c).unpadded_str()).collect();
        let mut unpadded_sorted = unpadded.clone();
        unpadded_sorted.sort();
        assert_ne!(unpadded, unpadded_sorted);
    }
}
