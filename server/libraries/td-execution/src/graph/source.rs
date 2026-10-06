//
// Copyright 2026 Tabsdata Inc.
//

use crate::graph::FunctionOrTable;
use crate::graph::build::GraphBuilder;
use ta_execution::GraphSource;
use td_database::access::{AccessMode, SeesProjectRows};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::trigger::TriggerDBWithNames;
use td_objects::types::basic::{AtTime, FunctionId};
use td_objects::types::recursion::RecursionOptions;

pub struct ConnGraphSource<'a, S: SeesProjectRows, M: AccessMode> {
    pub conn: &'a mut DomainConnection<Apiserver, S, M>,
    pub builder: &'a GraphBuilder,
    pub at_time: Option<&'a AtTime>,
}

impl<S: SeesProjectRows, M: AccessMode> GraphSource for ConnGraphSource<'_, S, M> {
    async fn walk(
        &mut self,
        start: &FunctionId,
        options: &RecursionOptions,
    ) -> Result<Vec<TriggerDBWithNames>, TdError> {
        self.builder
            .find_recursive_trigger_graph(
                self.conn,
                self.at_time,
                &FunctionOrTable::from(start),
                options,
            )
            .await
    }

    async fn children_triggers(
        &mut self,
        start: &FunctionId,
        triggers: &[TriggerDBWithNames],
        options: &RecursionOptions,
    ) -> Result<Vec<TriggerDBWithNames>, TdError> {
        self.builder
            .find_children_triggers(self.conn, self.at_time, start, triggers, options)
            .await
    }
}
