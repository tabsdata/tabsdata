//
// Copyright 2026 Tabs Data Inc.
//

/// Typed value trait. Call `.as_sea_value()` to get a `sea_query::Value`
/// for use in query builders.
///
/// Blanket `From<MyType> for sea_query::Value` impls are not possible due to
/// the orphan rule — use `.as_sea_value()` explicitly at call sites, or
/// collect with `.map(SeaValue::as_sea_value)`.
pub trait SeaValue: std::fmt::Debug + Send + Sync {
    fn as_sea_value(&self) -> sea_query::Value;

    /// Returns the `TypeId` of this value's type, or — for `typed_tag` enums —
    /// the `TypeId` of the active variant's inner type.  Used by `col_for_type`
    /// to resolve the right database column at runtime.
    fn as_type_id(&self) -> std::any::TypeId;

    /// Format this value for user display in error messages.
    ///
    /// Strings are single-quoted, `None` variants render as `"null"`.
    fn display_value(&self) -> String {
        use sea_query::Value;
        match self.as_sea_value() {
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

/// Delegate through references.
impl<T: SeaValue> SeaValue for &T {
    fn as_sea_value(&self) -> sea_query::Value {
        (**self).as_sea_value()
    }

    fn as_type_id(&self) -> std::any::TypeId {
        (**self).as_type_id()
    }
}

/// Opt-in trait for types that can appear as `Option<T>` in query conditions.
/// Provides the typed null `sea_query::Value` variant for that type.
pub trait NullableSeaValue: SeaValue + Sized {
    fn null_sea_value() -> sea_query::Value;
}

// Option<T> is SeaValue whenever T opts in to NullableSeaValue.
impl<T: NullableSeaValue + 'static> SeaValue for Option<T> {
    fn as_sea_value(&self) -> sea_query::Value {
        match self {
            Some(v) => v.as_sea_value(),
            None => T::null_sea_value(),
        }
    }

    fn as_type_id(&self) -> std::any::TypeId {
        std::any::TypeId::of::<Self>()
    }
}
