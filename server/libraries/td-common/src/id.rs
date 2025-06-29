//
// Copyright 2024 Tabs Data Inc.
//

use data_encoding::BASE32HEX_NOPAD;
use serde::{Deserialize, Serialize};
use sqlx::encode::IsNull;
use sqlx::error::BoxDynError;
use sqlx::sqlite::{SqliteArgumentValue, SqliteTypeInfo, SqliteValueRef};
use sqlx::{Decode, Encode, Sqlite, Type};
use std::borrow::Cow;
use std::fmt::{Debug, Display, Formatter};
use std::ops::Add;
use std::time::{Duration, SystemTime};
use td_error::td_error;
use td_security::DEFAULT_IDS;
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
#[derive(Copy, Clone, Serialize, Deserialize, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct Id([u8; 16]);

impl Id {
    /// For td_objects::types::basic::UserId/RoleI duse ONLY.
    pub fn _new(bytes: [u8; 16]) -> Self {
        Id(bytes)
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
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", BASE32HEX_NOPAD.encode(&self.0))
    }
}

impl From<Id> for String {
    fn from(id: Id) -> String {
        id.to_string()
    }
}

impl Type<Sqlite> for Id {
    fn type_info() -> SqliteTypeInfo {
        <String as Type<Sqlite>>::type_info()
    }
}

impl Encode<'_, Sqlite> for Id {
    fn encode_by_ref(
        &self,
        args: &mut Vec<SqliteArgumentValue<'_>>,
    ) -> Result<IsNull, BoxDynError> {
        let id = self.to_string();
        args.push(SqliteArgumentValue::Text(Cow::Owned(id)));

        Ok(IsNull::No)
    }
}

impl<'r> Decode<'r, Sqlite> for Id {
    fn decode(value: SqliteValueRef<'r>) -> Result<Self, BoxDynError> {
        let id = <String as Decode<Sqlite>>::decode(value)?;
        Id::try_from(&id).map_err(|e| Box::new(e) as BoxDynError)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::SqliteConnection;
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

    #[tokio::test]
    async fn test_sqlx_encode_decode() {
        let id = id();
        let db = sqlx::sqlite::SqlitePoolOptions::new()
            .connect("sqlite::memory:")
            .await
            .unwrap();
        let mut conn = db.acquire().await.unwrap();
        let conn = &mut conn as &mut SqliteConnection;
        sqlx::query(
            r#"
            CREATE TABLE test (
                id TEXT PRIMARY KEY
            )
            "#,
        )
        .execute(&mut *conn)
        .await
        .unwrap();
        sqlx::query("INSERT INTO test (id) VALUES (?)")
            .bind(id)
            .execute(&mut *conn)
            .await
            .unwrap();

        #[derive(sqlx::FromRow)]
        struct Row {
            id: Id,
        }

        let got: Row = sqlx::query_as("SELECT id FROM test")
            .fetch_one(&mut *conn)
            .await
            .unwrap();

        assert_eq!(id, got.id);
    }

    #[test]
    fn test_default_ids() {
        for (b, s) in DEFAULT_IDS.iter().zip(DEFAULT_ENCODED_IDS.iter()) {
            let encoded = BASE32HEX_NOPAD.encode(b);
            assert_eq!(&encoded, s);

            let mut decoded = vec![0; 16];
            BASE32HEX_NOPAD
                .decode_mut(s.as_bytes(), &mut decoded)
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
