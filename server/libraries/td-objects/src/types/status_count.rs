//
// Copyright 2025 Tabs Data Inc.
//

use crate::types::basic::{FunctionRunStatus, StatusCount};
use std::collections::HashMap;
use strum::IntoEnumIterator;

#[derive(utoipa::ToSchema, Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct FunctionRunStatusCount(HashMap<FunctionRunStatus, StatusCount>);

impl FunctionRunStatusCount {
    fn new(map: HashMap<FunctionRunStatus, StatusCount>) -> Self {
        let mut map = map;
        // Enforce all statuses to be present in the map with count 0.
        for status in FunctionRunStatus::iter() {
            map.entry(status)
                .or_insert(StatusCount::try_from(0).unwrap());
        }
        Self(map)
    }
}

impl From<sqlx::types::Json<HashMap<FunctionRunStatus, StatusCount>>> for FunctionRunStatusCount {
    fn from(value: sqlx::types::Json<HashMap<FunctionRunStatus, StatusCount>>) -> Self {
        FunctionRunStatusCount::new(value.0)
    }
}
