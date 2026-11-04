//
// Copyright 2026 Tabsdata Inc.
//

use arrow::datatypes::{DataType, Field};

const MAX_DECIMAL_PRECISION: u8 = 38;

pub(super) fn unheld(field: &Field) -> Option<String> {
    let name = field.name();
    match field.data_type() {
        DataType::Union(fields, _) => Some(format!(
            "The field '{name}' is a union of {} types, which no single column type holds",
            fields
                .iter()
                .filter(|(_, branch)| branch.data_type() != &DataType::Null)
                .count()
        )),
        DataType::Interval(_) => Some(format!(
            "The field '{name}' is an Avro duration, not supported"
        )),
        DataType::Decimal256(precision, _) => Some(format!(
            "The field '{name}' is a decimal of precision {precision}, above the \
             {MAX_DECIMAL_PRECISION} supported"
        )),
        DataType::Struct(fields) => fields.iter().find_map(|inner| unheld(inner)),
        DataType::List(inner) | DataType::LargeList(inner) | DataType::Map(inner, _) => {
            unheld(inner)
        }
        _ => None,
    }
}
