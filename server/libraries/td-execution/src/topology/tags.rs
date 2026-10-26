//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::HashMap;
use td_error::TdError;
use td_objects::dxo::topology::TopologyTag;
use td_objects::types::basic::{FunctionId, TableId, TagEntityType, TopologyTagKey, TopologyTags};

#[derive(Default)]
pub(crate) struct TagIndex {
    functions: HashMap<FunctionId, TopologyTags>,
    tables: HashMap<TableId, TopologyTags>,
    parents: HashMap<FunctionId, FunctionId>,
}

impl TagIndex {
    pub(crate) fn of(rows: Vec<TopologyTag>) -> Result<Self, TdError> {
        let mut tags = Self::default();
        for row in rows {
            match row.entity_type {
                TagEntityType::Function => {
                    let function = row.entity_id.id()?;
                    if row.key == TopologyTagKey::Parent {
                        tags.parents.insert(function, row.value.id()?);
                    }
                    tags.functions
                        .entry(function)
                        .or_default()
                        .insert(row.key, row.value);
                }
                TagEntityType::Table => {
                    let table = row.entity_id.id()?;
                    tags.tables
                        .entry(table)
                        .or_default()
                        .insert(row.key, row.value);
                }
                _ => {}
            }
        }
        Ok(tags)
    }

    pub(crate) fn function(&self, function: &FunctionId) -> TopologyTags {
        self.functions.get(function).cloned().unwrap_or_default()
    }

    pub(crate) fn table(&self, table: &TableId) -> TopologyTags {
        self.tables.get(table).cloned().unwrap_or_default()
    }

    pub(crate) fn parent(&self, function: &FunctionId) -> Option<&FunctionId> {
        self.parents.get(function)
    }
}
