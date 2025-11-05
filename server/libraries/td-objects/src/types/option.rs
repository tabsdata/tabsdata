//
// Copyright 2025 Tabs Data Inc.
//

use crate::types::SqlEntity;
use std::fmt::Display;

impl<T> SqlEntity for Option<T>
where
    T: SqlEntity + Display + 'static,
{
    fn push_bind<'a>(&'a self, builder: &mut sqlx::QueryBuilder<'a, sqlx::Sqlite>) {
        if let Some(value) = self {
            value.push_bind(builder);
        } else {
            builder.push_bind(None::<String>);
        }
    }

    fn push_bind_unseparated<'a>(
        &'a self,
        builder: &mut sqlx::query_builder::Separated<'_, 'a, sqlx::Sqlite, &str>,
    ) {
        if let Some(value) = self {
            value.push_bind_unseparated(builder);
        } else {
            builder.push_bind_unseparated(None::<String>);
        }
    }

    fn as_display(&self) -> String {
        if let Some(value) = self {
            value.to_string()
        } else {
            "".to_string()
        }
    }

    fn from_display(s: impl ToString) -> Result<Self, td_error::TdError> {
        let s = s.to_string();
        if s.is_empty() {
            Ok(None)
        } else {
            T::from_display(s).map(Some)
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
