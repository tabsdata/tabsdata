//
// Copyright 2026 Tabs Data Inc.
//

use serde_yaml::Value;
use td_common::server::{Callback, MessageAction, RequestMessagePayload, WorkerClass};

use crate::base::error::kubernetes::KubeMessagingError;
use crate::runtime::gateway::messaging::Decorator::Publisher;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Decorator {
    Publisher,
    Transformer,
    Subscriber,
}

#[derive(Debug, Clone)]
pub enum FunctionMessage {
    Start(FunctionStart),
    Stop(FunctionStop),
    Info(FunctionInfo),
}

#[derive(Debug, Clone)]
pub struct FunctionStart {
    pub worker: String,
    pub decorator: Decorator,
    pub runid: String,
    pub resolver: Callback,
    pub responder: Callback,
    pub collection: String,
    pub function: String,
}

#[derive(Debug, Clone)]
pub struct FunctionStop;

#[derive(Debug, Clone)]
pub struct FunctionInfo;

impl FunctionMessage {
    pub fn from_payload(
        payload: &RequestMessagePayload<Value>,
    ) -> Result<Self, KubeMessagingError> {
        match payload.action {
            MessageAction::Start => {
                let responder = payload.callback.clone().ok_or_else(|| {
                    KubeMessagingError::StartMissingCallback {
                        worker: payload.worker.clone(),
                        runid: payload.run_id.clone(),
                    }
                })?;
                Ok(FunctionMessage::Start(FunctionStart {
                    worker: payload.worker.clone(),
                    decorator: Publisher,
                    runid: payload.run_id.clone(),
                    resolver: responder.clone(),
                    responder: responder.clone(),
                    collection: "".to_string(),
                    function: "".to_string(),
                }))
            }
            MessageAction::Stop => Ok(FunctionMessage::Stop(FunctionStop {
            })),
            MessageAction::Notify => Ok(FunctionMessage::Info(FunctionInfo {
            })),
        }
    }
}
