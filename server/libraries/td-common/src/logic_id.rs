//
// Copyright 2026 Tabs Data Inc.
//

use crate::id;
use crate::id::Id;
use chrono::{NaiveDate, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use td_error::{td_error, TdError};

const DATE_LEN: usize = 8;
const MAX_KEY_LEN: usize = 3;
const ID_PADDING: usize = 10;
const MAX_IDL_LEN: usize = DATE_LEN + MAX_KEY_LEN + ID_PADDING;

#[td_error]
pub enum IdLError {
    #[error("Invalid KeyId key, it must be 1 to 3 uppercase characters: {0}")]
    InvalidKey(String) = 0,
    #[error("IdGenerator for '{0}' cannot be initialized with a Legacy Id: {1}")]
    CannotInitializeGenerator(String, String) = 1,
    #[error("Invalid ID, not Legacy, not '{0}' logical: {1}")]
    CannotParse(String, String) = 2,
}

/// Key Identifying a logical ID.
pub trait KeyId: Clone {
    /// The Key must be 1 to 3 uppercase alphabetic characters.
    fn key() -> &'static str;
}

/// Holds the ID string.
/// When serializing/deserializing, an UNPADDED ID string is used.
/// When encoding/decoding with sqlx, the full PADDED ID string is used.
/// Type K enforces validation during Deserialization.
#[derive(Debug, Clone, Eq, Ord)]
pub enum IdL<K: KeyId> {
    Legacy(String, PhantomData<K>),
    Logic(String, PhantomData<K>),
}

impl<K: KeyId> IdL<K> {
    fn is_legacy(&self) -> bool {
        matches!(self, IdL::Legacy(_, _))
    }

    fn is_logic(&self) -> bool {
        matches!(self, IdL::Logic(_, _))
    }

    fn raw_str(&self) -> &str {
        match self {
            IdL::Legacy(str, _) => str,
            IdL::Logic(str, _) => str,
        }
    }

    fn date_str(&self) -> Option<&str> {
        match self {
            IdL::Logic(str, _) => Some(&str[0..DATE_LEN]),
            _ => None,
        }
    }

    pub fn date(&self) -> Option<NaiveDate> {
        self.date_str()
            .and_then(|s| NaiveDate::parse_from_str(s, "%Y%m%d").ok())
    }

    fn counter_str(&self) -> Option<&str> {
        match self {
            IdL::Logic(str, _) => Some(&str[DATE_LEN + K::key().len()..]),
            _ => None,
        }
    }

    pub fn counter(&self) -> Option<usize> {
        self.counter_str().and_then(|s| s.parse().ok())
    }

    pub fn unpadded_str(&self) -> String {
        match self {
            IdL::Legacy(s, _) => s.clone(),
            IdL::Logic(_, _) => {
                format!(
                    "{}{}{}",
                    self.date_str().unwrap(),
                    K::key(),
                    self.counter().unwrap()
                )
            }
        }
    }

    pub fn padded_str(&self) -> String {
        match self {
            IdL::Legacy(s, _) => s.clone(),
            IdL::Logic(s, _) => s.clone(),
        }
    }

    fn parse(s: &str) -> Result<Self, IdLError> {
        // Legacy IDs must be 26 chars, Logical Ids must be between 19 and 21 chars.
        if s.len() == id::ID_LENGTH
            && let Ok(_) = Id::try_from(s)
        {
            return Ok(IdL::Legacy(s.to_string(), PhantomData));
        }

        if s.len() < DATE_LEN + K::key().len() + 1 || s.len() > MAX_IDL_LEN {
            return Err(IdLError::CannotParse(K::key().to_string(), s.to_string()));
        }

        let date_str = &s[0..DATE_LEN];
        let key_str = &s[DATE_LEN..DATE_LEN + K::key().len()];
        let counter_str = &s[DATE_LEN + K::key().len()..];

        let date = NaiveDate::parse_from_str(date_str, "%Y%m%d")
            .map_err(|_| IdLError::CannotParse(K::key().to_string(), s.to_string()))?;

        if key_str != K::key() {
            return Err(IdLError::CannotParse(K::key().to_string(), s.to_string()));
        }

        let counter = usize::from_str(counter_str)
            .map_err(|_| IdLError::CannotParse(K::key().to_string(), s.to_string()))?;

        Ok(Self::new_logic(date, counter))
    }

    fn new_logic(date: NaiveDate, counter: usize) -> Self {
        let s = format!(
            "{}{}{:0width$}",
            date.format("%Y%m%d"),
            K::key(),
            counter,
            width = ID_PADDING
        );
        Self::Logic(s, PhantomData)
    }
}

impl<K: KeyId> PartialEq for IdL<K> {
    fn eq(&self, other: &Self) -> bool {
        self.raw_str().eq(other.raw_str())
    }
}

//impl<K: KeyId> Eq for IdL<K> {}

impl<K: KeyId> PartialOrd for IdL<K> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.raw_str().partial_cmp(other.raw_str())
    }
}

impl<K: KeyId> Hash for IdL<K> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.raw_str().hash(state)
    }
}

impl<K: KeyId> fmt::Display for IdL<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.raw_str())
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

// --- IdL: Serialize / Deserialize (Unpadded) ---

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

// --- IdL: Encode / Decode (Padded) ---
impl<DB: sqlx::Database, K: KeyId> sqlx::Type<DB> for IdL<K>
where
    String: sqlx::Type<DB>,
{
    fn type_info() -> <DB as sqlx::Database>::TypeInfo {
        <String as sqlx::Type<DB>>::type_info()
    }

    fn compatible(ty: &<DB as sqlx::Database>::TypeInfo) -> bool {
        <String as sqlx::Type<DB>>::compatible(ty)
    }
}

impl<'q, DB: sqlx::Database, K: KeyId> sqlx::Encode<'q, DB> for IdL<K>
where
    String: sqlx::Encode<'q, DB>,
{
    fn encode_by_ref(
        &self,
        buf: &mut <DB as sqlx::Database>::ArgumentBuffer<'q>,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        let value = self.padded_str();
        <String as sqlx::Encode<'q, DB>>::encode_by_ref(&value, buf)
    }

    fn produces(&self) -> Option<DB::TypeInfo> {
        <String as sqlx::Encode<'q, DB>>::produces(&self.to_string())
    }

    fn size_hint(&self) -> usize {
        <String as sqlx::Encode<'q, DB>>::size_hint(&self.to_string())
    }
}

impl<'r, DB: sqlx::Database, K: KeyId> sqlx::Decode<'r, DB> for IdL<K>
where
    String: sqlx::Decode<'r, DB>,
{
    fn decode(
        value: <DB as sqlx::Database>::ValueRef<'r>,
    ) -> Result<Self, Box<dyn std::error::Error + 'static + Send + Sync>> {
        let s = <String as sqlx::Decode<'r, DB>>::decode(value)?;
        Ok(Self::parse(&s)?)
    }
}

#[derive(Debug)]
struct GeneratorState {
    last_date: Option<NaiveDate>,
    counter: usize,
}

#[derive(Debug, Clone)]
pub struct IdLGenerator<K: KeyId> {
    state: Arc<Mutex<GeneratorState>>,
    _marker: PhantomData<K>,
}

impl<K: KeyId> IdLGenerator<K> {
    pub fn new(latest: &Option<IdL<K>>) -> Result<Self, IdLError> {
        let key = K::key();

        if key.is_empty()
            || key.chars().count() > MAX_KEY_LEN
            || !key.chars().all(|c| c.is_ascii_uppercase())
        {
            return Err(IdLError::InvalidKey(key.to_string()));
        }

        let (date, counter) = if let Some(latest) = latest {
            if latest.is_legacy() {
                return Err(IdLError::CannotInitializeGenerator(
                    K::key().to_string(),
                    latest.to_string(),
                ));
            }
            let last_date = latest.date();
            let counter = latest.counter().unwrap_or(0);
            (last_date, counter)
        } else {
            (None, 0)
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

        (current_date, state.counter)
    }

    fn _generate(&self, current_date: NaiveDate) -> IdL<K> {
        let (date, counter) = self.next_sequence(current_date);
        IdL::new_logic(date, counter)
    }

    pub fn generate(&self) -> IdL<K> {
        self._generate(Utc::now().date_naive())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Days, NaiveDate};

    #[derive(Debug, Clone)]
    struct TestKey;
    impl KeyId for TestKey {
        fn key() -> &'static str {
            "TST"
        }
    }

    type TestId = IdL<TestKey>;

    #[test]
    fn test_parse_legacy_id() {
        let id_str = id::id().to_string();
        let id = TestId::parse(&id_str).unwrap();
        assert!(id.is_legacy());
        assert_eq!(id.raw_str(), id_str);
    }

    #[test]
    fn test_parse_logic_id_unpadded() {
        let id_str = "20240101TST123";
        let id = TestId::parse(id_str).unwrap();
        assert!(id.is_logic());
        assert_eq!(
            id.date().unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()
        );
        assert_eq!(id.counter().unwrap(), 123);
        assert_eq!(id.unpadded_str(), "20240101TST123");
        assert_eq!(id.padded_str(), "20240101TST0000000123");
    }

    #[test]
    fn test_parse_logic_id_padded() {
        let id_str = "20240101TST0000000123";
        let id = TestId::parse(id_str).unwrap();
        assert!(id.is_logic());
        assert_eq!(
            id.date().unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()
        );
        assert_eq!(id.counter().unwrap(), 123);
        assert_eq!(id.unpadded_str(), "20240101TST123");
        assert_eq!(id.padded_str(), "20240101TST0000000123");
    }

    #[test]
    fn test_parse_invalid_short_id() {
        let id_str = "20240101";
        let err = TestId::parse(id_str).unwrap_err();
        assert!(matches!(err, IdLError::CannotParse(_, _)));
    }

    #[test]
    fn test_parse_invalid_date() {
        let id_str = "20241301TST123"; // Invalid month
        let err = TestId::parse(id_str).unwrap_err();
        assert!(matches!(err, IdLError::CannotParse(_, _)));
    }

    #[test]
    fn test_parse_invalid_key() {
        let id_str = "20240101INV123";
        let err = TestId::parse(id_str).unwrap_err();
        assert!(matches!(err, IdLError::CannotParse(_, _)));
    }

    #[test]
    fn test_parse_invalid_counter() {
        let id_str = "20240101TSTabc";
        let err = TestId::parse(id_str).unwrap_err();
        assert!(matches!(err, IdLError::CannotParse(_, _)));
    }

    #[test]
    fn test_generator_new() {
        let generator = IdLGenerator::<TestKey>::new(&None).unwrap();
        let id = generator.generate();
        assert!(id.is_logic());
        assert!(id.date().is_some());
        assert_eq!(id.counter(), Some(1));
    }

    #[test]
    fn test_generator_new_with_same_date() {
        let date = Utc::now().date_naive();
        let latest_id = TestId::new_logic(date, 10);
        let generator = IdLGenerator::<TestKey>::new(&Some(latest_id)).unwrap();
        // using _generate() to ensure we are in the same date
        let id = generator._generate(date);
        assert!(id.is_logic());
        assert_eq!(id.date(), Some(date));
        assert_eq!(id.counter(), Some(11));
    }

    #[test]
    fn test_generator_new_with_newer_date() {
        let generator_date = Utc::now().date_naive();
        let latest_id = TestId::new_logic(generator_date, 10);
        let generator = IdLGenerator::<TestKey>::new(&Some(latest_id)).unwrap();
        let current_date = generator_date.checked_add_days(Days::new(1)).unwrap();
        // using _generate() to ensure we are in the same date
        let id = generator._generate(current_date);
        assert!(id.is_logic());
        assert_eq!(id.date(), Some(current_date));
        assert_eq!(id.counter(), Some(1));
    }

    #[test]
    fn test_generator_new_with_legacy_fails() {
        let latest_id = TestId::Legacy(id::id().to_string(), PhantomData);
        let result = IdLGenerator::<TestKey>::new(&Some(latest_id));
        assert!(matches!(
            result,
            Err(IdLError::CannotInitializeGenerator(_, _))
        ));
    }

    #[test]
    fn test_generator_generate_multiple() {
        let generator = IdLGenerator::<TestKey>::new(&None).unwrap();
        // using _generate() to ensure we are in the same date
        let date = Utc::now().date_naive();
        let id1 = generator._generate(date);
        let id2 = generator._generate(date);
        assert_eq!(id1.date(), id2.date());
        assert_eq!(id1.counter().unwrap() + 1, id2.counter().unwrap());
    }

    #[test]
    fn test_new_logic_creates_logic_variant() {
        let id = TestId::new_logic(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(), 1);
        assert!(id.is_logic());
        assert_eq!(id.padded_str(), "20240101TST0000000001");
    }

    #[test]
    fn test_serialization() {
        let id = TestId::new_logic(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(), 123);
        let serialized = serde_json::to_string(&id).unwrap();
        assert_eq!(serialized, r#""20240101TST123""#);
    }

    #[test]
    fn test_deserialization_unpadded() {
        let json = r#""20240101TST123""#;
        let id: TestId = serde_json::from_str(json).unwrap();
        assert!(id.is_logic());
        assert_eq!(id.counter(), Some(123));
        assert_eq!(id.padded_str(), "20240101TST0000000123");
    }

    #[test]
    fn test_deserialization_padded() {
        let json = r#""20240101TST0000000123""#;
        let id: TestId = serde_json::from_str(json).unwrap();
        assert!(id.is_logic());
        assert_eq!(id.counter(), Some(123));
        assert_eq!(id.padded_str(), "20240101TST0000000123");
    }

    #[test]
    fn test_sqlx_encoding_logic_id() {
        // This is a simplified simulation of what sqlx does.
        // We are testing that encode calls padded_str and decode calls parse.
        let id = TestId::new_logic(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(), 123);
        let encoded_str = id.padded_str();
        assert_eq!(encoded_str, "20240101TST0000000123");

        let decoded_id = TestId::parse(&encoded_str).unwrap();
        assert_eq!(id, decoded_id);
    }

    #[test]
    fn test_sqlx_encoding_legacy_id() {
        // This is a simplified simulation of what sqlx does.
        let id_str = id::id().to_string();
        let id = TestId::parse(&id_str).unwrap();
        let encoded_str = id.padded_str();
        assert_eq!(encoded_str, id_str);

        let decoded_id = TestId::parse(&encoded_str).unwrap();
        assert_eq!(id, decoded_id);
    }
}
