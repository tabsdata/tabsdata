//
// Copyright 2026 Tabsdata Inc.
//

use td_database::access::{AccessMode, SeesProjectRows};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::dependency::DependencyDB;
use td_objects::dxo::table::TableDB;
use td_objects::dxo::trigger::TriggerDBWithNames;
use td_objects::types::basic::FunctionId;
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

    pub async fn load<S: SeesProjectRows, M: AccessMode>(
        conn: &mut DomainConnection<Apiserver, S, M>,
        function_id: &FunctionId,
    ) -> Result<Self, TdError> {
        let tables: Vec<TableDB> = conn.scd2().fetch_all(function_id).await?;
        let dependencies: Vec<DependencyDB> = conn.scd2().fetch_all(function_id).await?;
        let triggers: Vec<TriggerDBWithNames> = conn.scd2().fetch_all(function_id).await?;
        Ok(Self {
            tables,
            dependencies,
            triggers,
        })
    }
}
