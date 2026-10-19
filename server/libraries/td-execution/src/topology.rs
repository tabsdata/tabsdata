//
// Copyright 2026 Tabsdata Inc.
//

use crate::graph::{TdGraph, TdGraphBuilder};
use std::collections::HashMap;
use td_database::access::{AccessMode, InProject};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::dependency::DependencyDBWithNames;
use td_objects::dxo::function::FunctionDBWithNames;
use td_objects::dxo::table::TableDBWithNames;
use td_objects::dxo::topology::ProjectTopologyDB;
use td_objects::dxo::trigger::TriggerDBWithNames;
use td_queries::Queries;

/// A project's whole graph at one topology version.
pub struct ProjectTopology {
    pub version: ProjectTopologyDB,
    pub graph: TdGraph,
}

impl ProjectTopology {
    pub async fn of<M: AccessMode>(
        conn: &mut DomainConnection<Apiserver, InProject, M>,
        version: ProjectTopologyDB,
    ) -> Result<Self, TdError> {
        let functions: Vec<FunctionDBWithNames> = conn
            .scd2()
            .as_of(&version.interval.valid_from)
            .fetch_all(())
            .await?;
        let tables: Vec<TableDBWithNames> = conn
            .scd2()
            .as_of(&version.interval.valid_from)
            .fetch_all(())
            .await?;
        let dependencies: Vec<DependencyDBWithNames> = conn
            .scd2()
            .as_of(&version.interval.valid_from)
            .fetch_all(())
            .await?;
        let triggers: Vec<TriggerDBWithNames> = conn
            .scd2()
            .as_of(&version.interval.valid_from)
            .fetch_all(())
            .await?;

        let functions: HashMap<_, _> = functions
            .into_iter()
            .map(|f| (f.base.base.function_id, f))
            .collect();
        // A frozen table outlives the function that wrote it: no output edge.
        let outputs: Vec<TableDBWithNames> = tables
            .iter()
            .filter(|t| functions.contains_key(&t.base.base.function_id))
            .cloned()
            .collect();
        let tables: HashMap<_, _> = tables
            .into_iter()
            .map(|t| (t.base.base.table_id, t))
            .collect();
        let graph = TdGraphBuilder::new(&tables, &functions)
            .with_triggers(&triggers)?
            .with_dependencies(&dependencies)?
            .with_outputs(&outputs)?
            .build();
        Ok(Self { version, graph })
    }
}
