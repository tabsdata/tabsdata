//
// Copyright 2026 Tabsdata Inc.
//

use polars::prelude::*;

pub fn stored(dtype: &DataType) -> bool {
    match dtype {
        DataType::Boolean
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::UInt128
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Int128
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal(_, _)
        | DataType::String
        | DataType::Binary
        | DataType::Date
        | DataType::Datetime(_, _)
        | DataType::Duration(_)
        | DataType::Time
        | DataType::Null
        | DataType::Categorical(_, _)
        | DataType::Enum(_, _) => true,
        DataType::List(inner) | DataType::Array(inner, _) => stored(inner),
        DataType::Struct(fields) => fields.iter().all(|field| stored(field.dtype())),
        DataType::BinaryOffset | DataType::Unknown(_) => false,
    }
}

pub fn every_dtype_frame() -> DataFrame {
    let numbers = Series::new("".into(), [Some(1i64), None, Some(3)]);
    let strings = Series::new("".into(), [Some("a"), None, Some("c")]);
    let from = |values: &Series, name: &str, dtype: DataType| {
        values.cast(&dtype).unwrap().with_name(name.into())
    };
    let list = Series::new(
        "list".into(),
        [
            Some(Series::new("".into(), [1i64, 1])),
            None,
            Some(Series::new("".into(), [3i64, 3])),
        ],
    );
    let array = from(
        &list,
        "array",
        DataType::Array(Box::new(DataType::Int64), 2),
    );
    let structure = StructChunked::from_series(
        "struct".into(),
        numbers.len(),
        [
            numbers.clone().with_name("number".into()),
            strings.clone().with_name("text".into()),
        ]
        .iter(),
    )
    .unwrap()
    .into_series();
    let categories = FrozenCategories::new(["a", "c"]).unwrap();
    let microseconds = TimeUnit::Microseconds;
    let series = vec![
        from(&numbers, "i8", DataType::Int8),
        from(&numbers, "i16", DataType::Int16),
        from(&numbers, "i32", DataType::Int32),
        from(&numbers, "i64", DataType::Int64),
        from(&numbers, "i128", DataType::Int128),
        from(&numbers, "u8", DataType::UInt8),
        from(&numbers, "u16", DataType::UInt16),
        from(&numbers, "u32", DataType::UInt32),
        from(&numbers, "u64", DataType::UInt64),
        from(&numbers, "u128", DataType::UInt128),
        from(&numbers, "f16", DataType::Float16),
        from(&numbers, "f32", DataType::Float32),
        from(&numbers, "f64", DataType::Float64),
        from(&numbers, "decimal", DataType::Decimal(10, 2)),
        from(&numbers, "decimal_38", DataType::Decimal(38, 0)),
        from(&numbers, "date", DataType::Date),
        from(&numbers, "time", DataType::Time),
        from(
            &numbers,
            "datetime_ms",
            DataType::Datetime(TimeUnit::Milliseconds, None),
        ),
        from(
            &numbers,
            "datetime_us",
            DataType::Datetime(microseconds, None),
        ),
        from(
            &numbers,
            "datetime_ns",
            DataType::Datetime(TimeUnit::Nanoseconds, None),
        ),
        from(
            &numbers,
            "datetime_utc",
            DataType::Datetime(microseconds, Some(TimeZone::UTC)),
        ),
        from(
            &numbers,
            "duration_ms",
            DataType::Duration(TimeUnit::Milliseconds),
        ),
        from(&numbers, "duration_us", DataType::Duration(microseconds)),
        from(
            &numbers,
            "duration_ns",
            DataType::Duration(TimeUnit::Nanoseconds),
        ),
        Series::new("boolean".into(), [Some(true), None, Some(false)]),
        from(&strings, "string", DataType::String),
        from(&strings, "binary", DataType::Binary),
        from(
            &strings,
            "categorical",
            DataType::from_categories(Categories::global()),
        ),
        from(
            &strings,
            "enum",
            DataType::from_frozen_categories(categories),
        ),
        list,
        array,
        structure,
        Series::full_null("null".into(), numbers.len(), &DataType::Null),
    ];
    DataFrame::new_infer_height(series.into_iter().map(Series::into_column).collect()).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_every_dtype_frame_holds_only_stored_dtypes() {
        let frame = every_dtype_frame();
        assert_eq!(frame.height(), 3);
        for column in frame.columns() {
            assert!(stored(column.dtype()), "{}", column.name());
        }
    }

    #[test]
    fn test_unresolved_and_internal_dtypes_are_not_stored() {
        assert!(!stored(&DataType::Unknown(Default::default())));
        assert!(!stored(&DataType::BinaryOffset));
        assert!(!stored(&DataType::List(Box::new(DataType::BinaryOffset))));
    }
}
