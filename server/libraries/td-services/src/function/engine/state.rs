//
// Copyright 2026 Tabsdata Inc.
//

use td_database::access::SeesProjectRows;
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::dependency::DependencyDB;
use td_objects::dxo::table::TableDB;
use td_objects::dxo::trigger::TriggerDBWithNames;
use td_objects::types::basic::{AtTime, FunctionId};
use td_queries::Queries;

/// A function's open relations at one instant: every table version whatever
/// its status, every dependency and every trigger.
pub struct FunctionState {
    pub tables: Vec<TableDB>,
    pub dependencies: Vec<DependencyDB>,
    pub triggers: Vec<TriggerDBWithNames>,
}

impl FunctionState {
    /// A function that does not exist yet.
    pub fn empty() -> Self {
        Self {
            tables: Vec::new(),
            dependencies: Vec::new(),
            triggers: Vec::new(),
        }
    }

    pub async fn load<S: SeesProjectRows>(
        conn: &mut DomainConnection<Apiserver, S>,
        function_id: &FunctionId,
        at: &AtTime,
    ) -> Result<Self, TdError> {
        let tables: Vec<TableDB> = conn.as_of(at).fetch_all(function_id).await?;
        let dependencies: Vec<DependencyDB> = conn.as_of(at).fetch_all(function_id).await?;
        let triggers: Vec<TriggerDBWithNames> = conn.as_of(at).fetch_all(function_id).await?;
        Ok(Self {
            tables,
            dependencies,
            triggers,
        })
    }
}
