//
// Copyright 2026 Tabsdata Inc.
//

/// A domain type usable in a query: a bound value, or an SQL literal for the
/// few with none (an interval end's infinity). The orphan rule rules out
/// blanket `From<MyType> for sea_query::Expr` impls, so call sites convert
/// explicitly.
pub trait SeaValue: std::fmt::Debug + Send + Sync {
    fn as_sea_expr(&self) -> sea_query::Expr;

    /// True for `Option<T>`: the column may hold SQL NULL.
    fn is_nullable(&self) -> bool {
        false
    }

    /// True when this value renders as SQL NULL.
    fn is_sql_null(&self) -> bool {
        false
    }

    /// For error messages: strings single-quoted, `None` as `"null"`.
    fn display_value(&self) -> String {
        use sea_query::{Expr, Value};
        let value = match self.as_sea_expr() {
            Expr::Value(value) => value,
            Expr::Custom(literal) => return literal.to_string(),
            _ => return "null".to_string(),
        };
        match value {
            Value::Bool(Some(b)) => b.to_string(),
            Value::TinyInt(Some(i)) => i.to_string(),
            Value::SmallInt(Some(i)) => i.to_string(),
            Value::Int(Some(i)) => i.to_string(),
            Value::BigInt(Some(i)) => i.to_string(),
            Value::TinyUnsigned(Some(i)) => i.to_string(),
            Value::SmallUnsigned(Some(i)) => i.to_string(),
            Value::Unsigned(Some(i)) => i.to_string(),
            Value::BigUnsigned(Some(i)) => i.to_string(),
            Value::Float(Some(f)) => f.to_string(),
            Value::Double(Some(f)) => f.to_string(),
            Value::String(Some(s)) => format!("'{s}'"),
            Value::Char(Some(c)) => format!("'{c}'"),
            _ => "null".to_string(),
        }
    }
}

/// Delegates through references.
impl<T: SeaValue> SeaValue for &T {
    fn as_sea_expr(&self) -> sea_query::Expr {
        (**self).as_sea_expr()
    }

    fn is_nullable(&self) -> bool {
        (**self).is_nullable()
    }

    fn is_sql_null(&self) -> bool {
        (**self).is_sql_null()
    }
}

/// Opt-in for types that may appear as `Option<T>` in conditions: supplies the
/// typed null value.
pub trait NullableSeaValue: SeaValue + Sized {
    fn null_sea_value() -> sea_query::Value;
}

// Enables the `Option<Option<T>>` tri-state: None skips the column,
// Some(None) sets NULL, Some(Some(v)) sets v.
impl<T: NullableSeaValue + 'static> NullableSeaValue for Option<T> {
    fn null_sea_value() -> sea_query::Value {
        T::null_sea_value()
    }
}

/// `Option<T>` is a value whenever `T` opts in to being nullable.
impl<T: NullableSeaValue + 'static> SeaValue for Option<T> {
    fn as_sea_expr(&self) -> sea_query::Expr {
        match self {
            Some(v) => v.as_sea_expr(),
            None => T::null_sea_value().into(),
        }
    }

    fn is_nullable(&self) -> bool {
        true
    }

    fn is_sql_null(&self) -> bool {
        self.is_none()
    }
}
