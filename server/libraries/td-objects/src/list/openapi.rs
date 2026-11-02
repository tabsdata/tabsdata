//
// Copyright 2026 Tabsdata Inc.
//

use ta_objects::{ListDto, ListRole};
use utoipa::openapi::RefOr;
use utoipa::openapi::path::{Operation, ParameterIn};

/// Lists in the description of a list endpoint's `order_by` and `filter`
/// query parameters the fields `D` accepts.
pub fn document_list_fields<D: ListDto>(operation: &mut Operation) {
    let Some(parameters) = operation.parameters.as_mut() else {
        return;
    };
    for parameter in parameters.iter_mut().filter_map(|p| match p {
        RefOr::T(p) if matches!(p.parameter_in, ParameterIn::Query) => Some(p),
        _ => None,
    }) {
        let text = match parameter.name.as_str() {
            "order_by" => format!(
                "`<FIELD>[+|-]`; order-by fields: {}",
                D::fields(ListRole::Order).join(", ")
            ),
            "filter" => format!(
                "`<FIELD><OPERATOR><VALUE>`; filter fields: {}; like (`:lk:`) filter fields: {}",
                D::fields(ListRole::Filter).join(", "),
                D::fields(ListRole::Like).join(", ")
            ),
            _ => continue,
        };
        parameter.description = Some(match parameter.description.take() {
            Some(existing) => format!("{existing}. {text}"),
            None => text,
        });
    }
}
