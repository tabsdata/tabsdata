//
// Copyright 2026 Tabsdata Inc.
//

use super::*;
use arrow::datatypes::{DataType, Field, Fields, IntervalUnit, UnionFields, UnionMode};
use std::sync::Arc;

fn field(data_type: DataType) -> Field {
    Field::new("v", data_type, true)
}

fn union(branches: Vec<DataType>) -> DataType {
    let fields: Vec<Field> = branches
        .into_iter()
        .enumerate()
        .map(|(position, data_type)| Field::new(format!("b{position}"), data_type, true))
        .collect();
    DataType::Union(UnionFields::from_fields(fields), UnionMode::Dense)
}

#[test]
fn a_union_counts_its_branches_other_than_null() {
    let reason = unheld(&field(union(vec![
        DataType::Null,
        DataType::Utf8,
        DataType::Int64,
    ])));
    assert_eq!(
        reason.as_deref(),
        Some("The field 'v' is a union of 2 types, which no single column type holds")
    );
}

#[test]
fn a_duration_and_a_wide_decimal_are_unheld() {
    let duration = unheld(&field(DataType::Interval(IntervalUnit::MonthDayNano)));
    assert!(duration.unwrap().contains("is an Avro duration"));
    let decimal = unheld(&field(DataType::Decimal256(40, 2)));
    assert!(
        decimal
            .unwrap()
            .contains("precision 40, above the 38 supported")
    );
}

#[test]
fn an_unheld_type_nested_in_a_record_a_list_or_a_map_is_found() {
    let duration = Field::new("d", DataType::Interval(IntervalUnit::MonthDayNano), true);
    let record = DataType::Struct(Fields::from(vec![duration.clone()]));
    assert!(unheld(&field(record)).unwrap().contains("'d'"));
    let list = DataType::List(Arc::new(duration.clone()));
    assert!(unheld(&field(list)).unwrap().contains("'d'"));
    let entries = Field::new(
        "entries",
        DataType::Struct(Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            duration,
        ])),
        false,
    );
    assert!(
        unheld(&field(DataType::Map(Arc::new(entries), false)))
            .unwrap()
            .contains("'d'")
    );
}

#[test]
fn held_types_pass() {
    for data_type in [
        DataType::Int64,
        DataType::Utf8,
        DataType::Decimal128(38, 2),
        DataType::FixedSizeBinary(16),
        DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
    ] {
        assert_eq!(unheld(&field(data_type)), None);
    }
}
