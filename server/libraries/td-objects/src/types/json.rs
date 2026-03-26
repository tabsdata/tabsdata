//
// Copyright 2026 Tabs Data Inc.
//

use sea_query::Value;
use serde::Serialize;
use std::any::TypeId;
use std::fmt::Debug;
use std::ops::Deref;
use ta_objects::{NullableSeaValue, SeaValue};
use tracing::error;

#[derive(Debug, Clone)]
pub struct Json<T>(pub T);

impl<T> Deref for Json<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: Serialize + Debug + Send + Sync + 'static> SeaValue for Json<T> {
    fn as_sea_value(&self) -> Value {
        serde_json::to_value(&self.0)
            .map(|v| Value::Json(Some(Box::new(v))))
            .unwrap_or({
                // this should never fail as per serde::to_value doc, but just in case
                error!("Error converting json value {:?} to a sea value", self);
                Value::Json(None)
            })
    }

    fn as_type_id(&self) -> TypeId {
        TypeId::of::<T>()
    }
}

impl<T: Serialize + Debug + Send + Sync + 'static> NullableSeaValue for Json<T> {
    fn null_sea_value() -> Value {
        Value::Json(None)
    }
}

impl<T: serde::Serialize> TryFrom<Json<T>> for Value {
    type Error = serde_json::Error;

    fn try_from(val: Json<T>) -> Result<Self, Self::Error> {
        Ok(Value::Json(Some(Box::new(serde_json::to_value(val.0)?))))
    }
}

impl<'r, DB: sqlx::Database, T: serde::de::DeserializeOwned> sqlx::Decode<'r, DB> for Json<T>
where
    sqlx::types::Json<T>: sqlx::Decode<'r, DB>,
{
    fn decode(value: DB::ValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let inner = sqlx::types::Json::<T>::decode(value)?;
        Ok(Json(inner.0))
    }
}

impl<DB: sqlx::Database, T: serde::de::DeserializeOwned> sqlx::Type<DB> for Json<T>
where
    sqlx::types::Json<T>: sqlx::Type<DB>,
{
    fn type_info() -> DB::TypeInfo {
        sqlx::types::Json::<T>::type_info()
    }
}
