//
// Copyright 2026 Tabsdata Inc.
//

use polars::prelude::*;
use pyo3_polars::export::polars_ffi::version_0::{export_series, import_series};

pub fn every_dtype_series() -> Vec<Series> {
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
    vec![
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
        from(&numbers, "date", DataType::Date),
        from(&numbers, "time", DataType::Time),
        from(&numbers, "datetime", DataType::Datetime(microseconds, None)),
        from(
            &numbers,
            "datetime_utc",
            DataType::Datetime(microseconds, Some(TimeZone::UTC)),
        ),
        from(&numbers, "duration", DataType::Duration(microseconds)),
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
    ]
}

pub fn through_plugin(series: &Series) -> Series {
    unsafe { import_series(export_series(series)) }.unwrap()
}

#[test]
fn test_every_dtype_reaches_a_plugin_as_it_was_sent() {
    for series in every_dtype_series() {
        let received = through_plugin(&series);
        assert_eq!(received.dtype(), series.dtype(), "{}", series.name());
        assert!(received.equals_missing(&series), "{}", series.name());
    }
}
