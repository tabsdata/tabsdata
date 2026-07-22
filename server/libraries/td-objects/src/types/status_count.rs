//
// Copyright 2025 Tabsdata Inc.
//

use crate::types::basic::{LogicalFunctionRunStatus, StatusCount};
use crate::types::json::Json;
use std::collections::HashMap;
use strum::IntoEnumIterator;

#[derive(utoipa::ToSchema, Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FunctionRunStatusCount(HashMap<LogicalFunctionRunStatus, StatusCount>);

impl FunctionRunStatusCount {
    fn new(map: HashMap<LogicalFunctionRunStatus, StatusCount>) -> Self {
        let mut map = map;
        // Enforce all statuses to be present in the map with count 0.
        for status in LogicalFunctionRunStatus::iter() {
            map.entry(status)
                .or_insert(StatusCount::try_from(0).unwrap());
        }
        Self(map)
    }
}

impl From<Json<HashMap<LogicalFunctionRunStatus, StatusCount>>> for FunctionRunStatusCount {
    fn from(value: Json<HashMap<LogicalFunctionRunStatus, StatusCount>>) -> Self {
        FunctionRunStatusCount::new(value.0)
    }
}
