//
// Copyright 2026 Tabsdata Inc.
//

//! In-memory queue for tests: records every `put` regardless of payload kind
//! and replays them via `locked_messages`. Unlike the file queue it accepts
//! all message kinds (including the kubernetes function request).

use super::{
    OrchestratorMessage, OrchestratorMessagePayload, OrchestratorMessageQueue, QueueError,
};
use async_trait::async_trait;
use serde_yaml::Value;
use std::path::PathBuf;
use std::sync::Mutex;

#[derive(Default)]
pub struct InMemoryOrchestratorMessageQueue {
    messages: Mutex<Vec<OrchestratorMessage<Value>>>,
}

#[async_trait]
impl OrchestratorMessageQueue for InMemoryOrchestratorMessageQueue {
    async fn put(
        &self,
        id: String,
        payload: OrchestratorMessagePayload<Value>,
    ) -> Result<OrchestratorMessage<Value>, QueueError> {
        let message = OrchestratorMessage::new(id.clone(), id, PathBuf::new(), payload);
        self.messages.lock().unwrap().push(message.clone());
        Ok(message)
    }

    async fn commit(&self, _message: &OrchestratorMessage<Value>) -> Result<(), QueueError> {
        Ok(())
    }

    async fn rollback(&self, _message: &OrchestratorMessage<Value>) -> Result<(), QueueError> {
        Ok(())
    }

    async fn locked_messages(&self) -> Vec<OrchestratorMessage<Value>> {
        self.messages.lock().unwrap().clone()
    }
}
