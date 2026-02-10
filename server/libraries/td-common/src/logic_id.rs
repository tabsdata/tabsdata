//
// Copyright 2026 Tabs Data Inc.
//

use chrono::{NaiveDate, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use td_error::{TdError, td_error};

const DATE_LEN: usize = 8;
const ID_PADDING: usize = 10;

#[td_error]
pub enum IdLError {
    #[error("Invalid KeyId key, it must be 1 to 3 uppercase characters: {0}")]
    InvalidKey(String) = 0,
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
pub struct IdL<K: KeyId>(String, PhantomData<K>);

impl<K: KeyId> IdL<K> {
    fn date_str(&self) -> &str {
        &self.0[0..DATE_LEN]
    }

    pub fn date(&self) -> NaiveDate {
        NaiveDate::parse_from_str(self.date_str(), "%Y%m%d").unwrap()
    }

    fn counter_str(&self) -> &str {
        &self.0[DATE_LEN + K::key().len()..]
    }

    pub fn counter(&self) -> usize {
        self.counter_str().parse().unwrap_or(0)
    }

    pub fn unpadded(&self) -> Self {
        Self(
            format!(
                "{}{}{}",
                self.date().format("%Y%m%d"),
                K::key(),
                self.counter()
            ),
            PhantomData,
        )
    }

    pub fn padded(&self) -> Self {
        Self(
            format!(
                "{}{}{:0width$}",
                self.date().format("%Y%m%d"),
                K::key(),
                self.counter(),
                width = ID_PADDING
            ),
            PhantomData,
        )
    }
}

impl<K: KeyId> PartialEq for IdL<K> {
    fn eq(&self, other: &Self) -> bool {
        self.date_str() == other.date_str() && self.counter() == other.counter()
    }
}

//impl<K: KeyId> Eq for IdL<K> {}

impl<K: KeyId> PartialOrd for IdL<K> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.date_str().partial_cmp(other.date_str()) {
            Some(std::cmp::Ordering::Equal) => self.counter().partial_cmp(&other.counter()),
            ord => ord,
        }
    }
}

// impl<K: KeyId> Ord for IdL<K> {
//     fn cmp(&self, other: &Self) -> std::cmp::Ordering {
//         self.partial_cmp(&other).unwrap()
//     }
// }

impl<K: KeyId> Hash for IdL<K> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.date_str().hash(state);
        self.counter().hash(state);
    }
}

impl<K: KeyId> fmt::Display for IdL<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<K: KeyId> TryFrom<&String> for IdL<K> {
    type Error = TdError;

    fn try_from(value: &String) -> Result<Self, Self::Error> {
        Ok(IdL(value.to_string(), PhantomData))
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
        serializer.serialize_str(&self.0)
    }
}

impl<'de, K: KeyId> Deserialize<'de> for IdL<K> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;

        if s.len() < DATE_LEN + 1 {
            return Err(de::Error::custom("ID string too short"));
        }

        let remaining = &s[DATE_LEN..];
        let split_idx = remaining
            .find(|c: char| c.is_ascii_digit())
            .unwrap_or(remaining.len());
        let (extracted_key, _) = remaining.split_at(split_idx);

        let expected_key = K::key();
        if extracted_key != expected_key {
            return Err(de::Error::custom(format!(
                "Key mismatch. Expected ID for '{}', but found key '{}'",
                expected_key, extracted_key
            )));
        }

        Ok(IdL(s, PhantomData))
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
        let s = self.to_string();
        if s.len() < DATE_LEN + 1 {
            return <String as sqlx::Encode<'q, DB>>::encode_by_ref(&s, buf);
        }

        let date_part = &s[0..DATE_LEN];
        let remaining = &s[DATE_LEN..];

        let split_idx = remaining
            .find(|c: char| c.is_ascii_digit())
            .unwrap_or(remaining.len());

        let (key_part, counter_str) = remaining.split_at(split_idx);
        let counter_val: usize = counter_str.parse().unwrap_or(0);

        let padded_str = format!(
            "{}{}{:0width$}",
            date_part,
            key_part,
            counter_val,
            width = ID_PADDING
        );

        <String as sqlx::Encode<'q, DB>>::encode_by_ref(&padded_str, buf)
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
        let len = s.len();

        if len < DATE_LEN + ID_PADDING + 1 {
            return Ok(IdL(s, PhantomData));
        }

        let date_part = &s[0..DATE_LEN];
        let key_part = &s[DATE_LEN..len - ID_PADDING];
        let counter_part_padded = &s[len - ID_PADDING..];

        let counter_val: usize = counter_part_padded.parse().unwrap_or(0);
        let unpadded_str = format!("{}{}{}", date_part, key_part, counter_val);

        Ok(IdL(unpadded_str, PhantomData))
    }
}

#[derive(Debug)]
struct GeneratorState {
    key: &'static str,
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

        if key.is_empty() || key.chars().count() > 3 || !key.chars().all(|c| c.is_ascii_uppercase())
        {
            return Err(IdLError::InvalidKey(key.to_string()));
        }

        let last_date = latest.as_ref().map(|l| l.date());
        let counter = latest.as_ref().map(|l| l.counter()).unwrap_or(0);
        Ok(IdLGenerator {
            state: Arc::new(Mutex::new(GeneratorState {
                key,
                last_date,
                counter,
            })),
            _marker: PhantomData,
        })
    }
}

impl<K: KeyId> IdLGenerator<K> {
    fn next_sequence(&self, current_date: NaiveDate) -> (usize, &'static str) {
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

        (state.counter, state.key)
    }

    pub fn generate(&self) -> IdL<K> {
        let now = Utc::now();
        let current_date = now.date_naive();
        let (counter, key) = self.next_sequence(current_date);

        let padded_string = format!(
            "{}{}{:0width$}",
            current_date.format("%Y%m%d"),
            key,
            counter,
            width = ID_PADDING
        );

        IdL(padded_string, PhantomData)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Define Types
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TxnKey;
    impl KeyId for TxnKey {
        fn key() -> &'static str {
            "TXN"
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct UserKey;
    impl KeyId for UserKey {
        fn key() -> &'static str {
            "USR"
        }
    }

    #[test]
    fn test_idl_serialization() {
        let generator = IdLGenerator::<TxnKey>::new(&None).unwrap();
        let id: IdL<TxnKey> = generator.generate();

        let json = serde_json::to_string(&id).unwrap();
        let restored: IdL<TxnKey> = serde_json::from_str(&json).unwrap();
        assert_eq!(restored, id);

        let result: Result<IdL<UserKey>, _> = serde_json::from_str(&json);
        assert!(result.is_err());
        let err_msg = result.err().unwrap().to_string();
        assert!(err_msg.contains("Key mismatch"));
        assert!(err_msg.contains("Expected ID for 'USR'"));
        assert!(err_msg.contains("found key 'TXN'"));
    }

    #[test]
    fn test_dbidl_serialization() {
        let generator = IdLGenerator::<TxnKey>::new(&None).unwrap();
        let db_id: IdL<TxnKey> = generator.generate();

        let json = serde_json::to_string(&db_id).unwrap();
        let restored: IdL<TxnKey> = serde_json::from_str(&json).unwrap();
        assert_eq!(restored, db_id);

        let result: Result<IdL<UserKey>, _> = serde_json::from_str(&json);
        assert!(result.is_err());
        let err_msg = result.err().unwrap().to_string();
        assert!(err_msg.contains("Key mismatch"));
        assert!(err_msg.contains("Expected ID for 'USR'"));
        assert!(err_msg.contains("found key 'TXN'"));
    }

    #[test]
    fn test_conversion_type_safety() {
        let generator = IdLGenerator::<UserKey>::new(&None).unwrap();
        let _db_id: IdL<UserKey> = generator.generate();

        // let _id: IdL<UserKey> = (&db_id).into(); // OK

        // Uncommenting this causes compilation error:
        // let _bad: IdL<TxnKey> = db_id.into();
    }
}
