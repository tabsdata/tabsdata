//
// Copyright 2026 Tabsdata Inc.
//

pub(crate) mod plan;
pub(crate) mod run;

use std::collections::HashSet;
use std::sync::{Mutex, MutexGuard};

use serde_yaml::Value;
use td_bridge::messaging::OrchestratorMessagePayload;
use td_objects::types::basic::WorkerId;
use tokio::sync::Notify;

use crate::runtime::gateway::messaging::reconciler::plan::{Key, State, Submission};

pub(crate) type Payload = OrchestratorMessagePayload<Value>;

#[derive(Default)]
pub(crate) struct Reconciler {
    state: Mutex<State<Payload>>,
    wake: Notify,
}

impl Reconciler {
    pub(crate) fn submit(&self, key: Key, submission: Submission<Payload>) {
        self.state().submit(key, submission);
        self.wake.notify_one();
    }

    pub(crate) fn withdraw(&self, key: Key, worker_id: &WorkerId, name: &str) {
        self.state().withdraw(key, worker_id, name);
        self.wake.notify_one();
    }

    pub(crate) fn queued_runs(&self) -> HashSet<WorkerId> {
        self.state().queued_runs()
    }

    fn state(&self) -> MutexGuard<'_, State<Payload>> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}
