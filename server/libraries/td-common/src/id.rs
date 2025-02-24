//
// Copyright 2024 Tabs Data Inc.
//
use crate as td_common;
use data_encoding::BASE32HEX_NOPAD;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::ops::Add;
use std::time::{Duration, SystemTime};
use td_error::td_error;
use uuid::{Bytes, Uuid};

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
#[derive(
    Debug, Copy, Clone, Serialize, Deserialize, PartialEq, PartialOrd, Eq, Ord, Hash, Default,
)]
pub struct Id([u8; 16]);

/// Generates a new unique identifier.
pub fn id() -> Id {
    Id(*Uuid::now_v7().as_bytes())
}

/// Returns the time of the identifier.
pub fn id_time(id: &Id) -> SystemTime {
    let uuid = Uuid::from_bytes(Bytes::from(id.0));
    if uuid.get_version_num() != 7 {
        panic!("Invalid Id {}, not a UUID v7", id);
    }
    let seconds_nanos = uuid.get_timestamp().unwrap().to_unix();
    SystemTime::UNIX_EPOCH
        + Duration::from_millis(seconds_nanos.0 * 1000)
            .add(Duration::from_nanos(seconds_nanos.1 as u64))
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
        let input = s.as_bytes();
        if input.len() != 26 {
            return Err(IdError::InvalidBase32HexValue(input.len()));
        }
        let mut bytes = vec![0; 16];
        BASE32HEX_NOPAD
            .decode_mut(s.as_bytes(), &mut bytes)
            .map_err(|_| IdError::InvalidStringRepresentation(s.to_string()))?;
        Ok(Id(bytes.as_slice()[..16].try_into().unwrap()))
    }
}

impl Display for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", BASE32HEX_NOPAD.encode(&self.0))
    }
}

impl From<Id> for String {
    fn from(id: Id) -> String {
        id.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;
    use std::time::{Duration, SystemTime};
    use uuid::{Bytes, Uuid};

    #[test]
    fn test_id() {
        let id = id();
        let now = SystemTime::now();

        let uuid = Uuid::from_bytes(Bytes::from(id.0));
        assert_eq!(uuid.get_version_num(), 7);

        let time = id_time(&id);
        assert!(now.duration_since(time).unwrap().as_secs() < 1);
    }

    #[test]
    fn test_to_string_try_from_string() {
        let id = id();
        let s = id.to_string();
        assert_eq!(s.len(), 26);
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
}
