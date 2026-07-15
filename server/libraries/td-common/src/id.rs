//
// Copyright 2024 Tabs Data Inc.
//

use data_encoding::BASE32HEX_NOPAD;
use sqlx::error::BoxDynError;
use sqlx::postgres::{PgHasArrayType, PgTypeInfo};
use sqlx::{Database, Decode, Type};
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::ops::{Add, Deref};
use std::time::{Duration, SystemTime};
use td_error::td_error;
use td_security::DEFAULT_IDS;
use uuid::{Bytes, Uuid};

pub const ID_LENGTH: usize = 26;

#[td_error]
pub enum IdError {
    #[error("Invalid String representation, not BASE32HEX_NOPAD: {0}")]
    InvalidStringRepresentation(String) = 0,
    #[error("Invalid Base32Hex value, it should be 26 characters. It is: {0}")]
    InvalidBase32HexValue(usize) = 1,
}

/// A unique identifier encoding a timestamp.
///
/// It is a UUID v7 as a '[u8; 16]'.
///
/// The string representation is a 26 character base32hex string as it does not require URL encoding.
#[derive(Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct Id([u8; 16]);

impl Id {
    /// For td_objects::types::basic::UserId/RoleI duse ONLY.
    pub fn _new(bytes: [u8; 16]) -> Self {
        Id(bytes)
    }
}

impl Deref for Id {
    type Target = [u8; 16];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Debug for Id {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Id({self})")
    }
}

impl Default for Id {
    fn default() -> Self {
        id()
    }
}

/// Generates a new unique identifier.
pub fn id() -> Id {
    Id(*Uuid::now_v7().as_bytes())
}

/// Returns the time of the identifier.
pub fn id_time(id: &Id) -> SystemTime {
    let uuid = Uuid::from_bytes(Bytes::from(id.0));
    if uuid.get_version_num() == 7 {
        let seconds_nanos = uuid.get_timestamp().unwrap().to_unix();
        SystemTime::UNIX_EPOCH
            + Duration::from_millis(seconds_nanos.0 * 1000)
                .add(Duration::from_nanos(seconds_nanos.1 as u64))
    } else if DEFAULT_IDS.contains(&id.0) {
        // System epoch for default ids
        SystemTime::UNIX_EPOCH
    } else {
        panic!("Invalid Id {id}, not a UUID v7");
    }
}

impl TryFrom<&String> for Id {
    type Error = IdError;
    fn try_from(s: &String) -> Result<Self, Self::Error> {
        TryFrom::<&str>::try_from(s.as_str())
    }
}

impl TryFrom<&str> for Id {
    type Error = IdError;
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        if s.len() != ID_LENGTH {
            return Err(IdError::InvalidBase32HexValue(s.len()));
        }
        // The string representation is lowercase, but decoding accepts either case.
        let input = s.to_ascii_uppercase();
        let mut bytes = vec![0; 16];
        BASE32HEX_NOPAD
            .decode_mut(input.as_bytes(), &mut bytes)
            .map_err(|_| IdError::InvalidStringRepresentation(s.to_string()))?;
        Ok(Id(bytes.as_slice()[..16].try_into().unwrap()))
    }
}

impl Display for Id {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            BASE32HEX_NOPAD.encode(&self.0).to_ascii_lowercase()
        )
    }
}

impl From<Id> for String {
    fn from(id: Id) -> String {
        id.to_string()
    }
}

impl<'de> serde::Deserialize<'de> for Id {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Id::try_from(&s).map_err(serde::de::Error::custom)
    }
}

impl serde::Serialize for Id {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.to_string().serialize(serializer)
    }
}

impl<'r, DB: Database> Decode<'r, DB> for Id
where
    String: sqlx::Decode<'r, DB>,
{
    fn decode(
        value: <DB as Database>::ValueRef<'r>,
    ) -> Result<Self, Box<dyn Error + 'static + Send + Sync>> {
        let id = <String as Decode<'r, DB>>::decode(value)?;
        Id::try_from(&id).map_err(|e| Box::new(e) as BoxDynError)
    }
}

impl<DB: Database> Type<DB> for Id
where
    String: sqlx::Type<DB>,
{
    fn type_info() -> DB::TypeInfo {
        <String as Type<DB>>::type_info()
    }
    fn compatible(ty: &DB::TypeInfo) -> bool {
        <String as Type<DB>>::compatible(ty)
    }
}

impl PgHasArrayType for Id {
    fn array_type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("_text")
    }
    fn array_compatible(ty: &PgTypeInfo) -> bool {
        <String as PgHasArrayType>::array_compatible(ty)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;
    use std::time::{Duration, SystemTime};
    use td_security::DEFAULT_ENCODED_IDS;
    use uuid::{Bytes, Uuid};

    #[test]
    fn test_id() {
        let id = id();
        let now = SystemTime::now();

        let uuid = Uuid::from_bytes(Bytes::from(id.0));
        assert_eq!(uuid.get_version_num(), 7);

        let time = id_time(&id);
        assert!(now.duration_since(time).unwrap().as_secs() < 1);

        assert_eq!(format!("{id:#?}"), format!("Id({})", id.to_string()));
    }

    #[test]
    fn test_to_string_try_from_string() {
        let id = id();
        let s = id.to_string();
        assert_eq!(s.len(), ID_LENGTH);
        let id2 = Id::try_from(&s).unwrap();
        assert_eq!(id, id2);
    }

    #[test]
    fn test_try_from_string_invalid_string_representation() {
        let s = "012345678901234567890124@#";
        let id = Id::try_from(&s.to_string());
        assert!(matches!(id, Err(IdError::InvalidStringRepresentation(_))));
    }

    #[test]
    fn test_try_from_string_invalid_byte_sequence() {
        let s = "1";
        let id = Id::try_from(&s.to_string());
        assert!(matches!(id, Err(IdError::InvalidBase32HexValue(_))));
    }

    #[test]
    fn test_lexicographic_order() {
        let id1 = id();
        sleep(Duration::from_millis(1));
        let id2 = id();
        assert!(id1.to_string() < id2.to_string());
    }

    #[test]
    fn test_default_ids() {
        for (b, s) in DEFAULT_IDS.iter().zip(DEFAULT_ENCODED_IDS.iter()) {
            let encoded = BASE32HEX_NOPAD.encode(b).to_ascii_lowercase();
            assert_eq!(&encoded, s);

            let mut decoded = vec![0; 16];
            BASE32HEX_NOPAD
                .decode_mut(s.to_ascii_uppercase().as_bytes(), &mut decoded)
                .map_err(|_| IdError::InvalidStringRepresentation(s.to_string()))
                .unwrap();
            assert_eq!(&decoded, b);

            let id = Id::try_from(&s.to_string()).unwrap();
            assert_eq!(id, Id(*b));

            let id = Id(*b);
            let time = id_time(&id);
            assert_eq!(time, SystemTime::UNIX_EPOCH);
        }
    }
}
