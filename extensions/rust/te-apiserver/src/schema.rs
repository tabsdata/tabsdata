//
// Copyright 2026 Tabs Data Inc.
//

use td_database::config::{APISERVER_SCHEMA_KEY, SYSTEM_SCHEMA_KEY};
use td_database::error::DbError;
use td_database::schema::{ApiserverSchemaManager, SchemaManager, SystemSchemaManager};

pub struct SchemaManagerResolver;

impl SchemaManagerResolver {
    pub fn for_component(component: &str) -> Result<Box<dyn SchemaManager>, DbError> {
        match component {
            APISERVER_SCHEMA_KEY => Ok(Box::new(ApiserverSchemaManager)),
            SYSTEM_SCHEMA_KEY => Ok(Box::new(SystemSchemaManager)),
            _ => Err(DbError::UnknownSchemaEntry(component.to_owned())),
        }
    }
}
