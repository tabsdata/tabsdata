//
// Copyright 2026 Tabsdata Inc.
//

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use serde_yaml::Value;
use td_bridge::messaging::{
    CronRequest, FunctionRequest, OrchestratorMessage, OrchestratorMessagePayload,
    OrchestratorMessageQueue, QueueError, StageTriggerRequest,
};
use td_bridge::observer::WorkloadKind;
use td_common::worker::{Decorator, MessageAction};
use td_objects::types::basic::WorkerId;
use tracing::error;

use crate::base::naming::{logical_id_text, to_fn_pod_name, worker_name_prefix, worker_pod_stem};
use crate::runtime::gateway::messaging::messenger::factory::WorkloadFactory;
use crate::runtime::gateway::messaging::messenger::kit::{
    FunctionRunKit, build_cron_run_kit, build_function_run_kit, build_stream_run_kit,
};
use crate::runtime::gateway::messaging::messenger::template::{PodTemplate, PodTemplateIndex};
use crate::runtime::gateway::messaging::reconciler::Reconciler;
use crate::runtime::gateway::messaging::reconciler::plan::{Key, Submission};

fn unsupported(id: &str, action: &str) -> QueueError {
    error!(id = %id, action = %action, "Action not yet supported in Kubernetes messaging layer.");
    QueueError::BackendError {
        cause: format!("Action '{action}' not yet supported in Kubernetes messaging layer."),
    }
}

pub(super) fn unexpected_kind(id: &str, kind: &str) -> QueueError {
    error!(id = %id, kind = %kind, "Template kind not routable by the Kubernetes messaging layer.");
    QueueError::BackendError {
        cause: format!(
            "Template kind '{kind}' is applied by the endpoint phase, not routed as a worker."
        ),
    }
}

fn unexpected(id: &str, kind: &str, payload: &OrchestratorMessagePayload<Value>) -> QueueError {
    error!(id = %id, kind = %kind, "Payload kind not expected in Kubernetes messaging layer.");
    QueueError::BackendError {
        cause: format!(
            "Payload kind '{kind}' message payloads are not expected in Kubernetes messaging layer: {payload:?}"
        ),
    }
}

pub(super) struct PayloadRouting {
    pub(super) template_key: String,
    pub(super) prefix: String,
    pub(super) action: MessageAction,
}

pub(super) fn derive_routing(
    id: &str,
    payload: &OrchestratorMessagePayload<Value>,
) -> Result<PayloadRouting, QueueError> {
    match payload {
        OrchestratorMessagePayload::OrchestratorFunctionRequestMessagePayload(request) => {
            match request {
                FunctionRequest::Start(start) => Ok(PayloadRouting {
                    template_key: format!(
                        "{}-{}",
                        worker_pod_stem(&start.worker),
                        start.decorator.as_ref()
                    ),
                    prefix: worker_name_prefix(&start.worker, Some(&start.decorator)).to_owned(),
                    action: MessageAction::Start,
                }),
                FunctionRequest::Stop(stop) => Ok(PayloadRouting {
                    template_key: format!(
                        "{}-{}",
                        worker_pod_stem(&stop.worker),
                        stop.decorator.as_ref()
                    ),
                    prefix: worker_name_prefix(&stop.worker, Some(&stop.decorator)).to_owned(),
                    action: MessageAction::Stop,
                }),
                FunctionRequest::Notify => Err(unsupported(id, "Notify")),
            }
        }
        OrchestratorMessagePayload::OrchestratorStageTriggerRequestMessagePayload(request) => {
            match request {
                StageTriggerRequest::Start(start) => Ok(PayloadRouting {
                    template_key: format!(
                        "{}-{}",
                        worker_pod_stem(&start.worker),
                        Decorator::Publisher.as_ref()
                    ),
                    prefix: worker_name_prefix(&start.worker, None).to_owned(),
                    action: MessageAction::Start,
                }),
                StageTriggerRequest::Stop(stop) => Ok(PayloadRouting {
                    template_key: format!(
                        "{}-{}",
                        worker_pod_stem(&stop.worker),
                        Decorator::Publisher.as_ref()
                    ),
                    prefix: worker_name_prefix(&stop.worker, None).to_owned(),
                    action: MessageAction::Stop,
                }),
                StageTriggerRequest::Notify => Err(unsupported(id, "Notify")),
            }
        }
        OrchestratorMessagePayload::OrchestratorCronRequestMessagePayload(request) => match request
        {
            CronRequest::Start(start) => Ok(PayloadRouting {
                template_key: format!(
                    "{}-{}",
                    worker_pod_stem(&start.worker),
                    Decorator::Publisher.as_ref()
                ),
                prefix: worker_name_prefix(&start.worker, None).to_owned(),
                action: MessageAction::Start,
            }),
            CronRequest::Stop(stop) => Ok(PayloadRouting {
                template_key: format!(
                    "{}-{}",
                    worker_pod_stem(&stop.worker),
                    Decorator::Publisher.as_ref()
                ),
                prefix: worker_name_prefix(&stop.worker, None).to_owned(),
                action: MessageAction::Stop,
            }),
            CronRequest::Notify => Err(unsupported(id, "Notify")),
        },
        OrchestratorMessagePayload::OrchestratorRequestMessagePayload(request) => {
            match request.action {
                MessageAction::Start => Err(unsupported(id, "Start")),
                MessageAction::Stop => Err(unsupported(id, "Stop")),
                MessageAction::Notify => Err(unsupported(id, "Notify")),
            }
        }
        OrchestratorMessagePayload::OrchestratorResponseMessagePayload(_) => {
            Err(unexpected(id, "Response", payload))
        }
        OrchestratorMessagePayload::OrchestratorExceptionMessagePayload(_) => {
            Err(unexpected(id, "Exception", payload))
        }
    }
}

pub(super) fn lookup_template<'a>(
    templates: &'a PodTemplateIndex,
    id: &str,
    template_key: &str,
) -> Result<&'a PodTemplate, QueueError> {
    templates.lookup(template_key).map_err(|error| {
        error!(
            id = %id,
            template_key = %template_key,
            error = %error,
            "Template not found."
        );
        QueueError::BackendError {
            cause: error.to_string(),
        }
    })
}

pub(super) fn kit_for_payload(
    pod_name: &str,
    namespace: &str,
    payload: &OrchestratorMessagePayload<Value>,
) -> Result<Option<FunctionRunKit>, QueueError> {
    match payload {
        OrchestratorMessagePayload::OrchestratorFunctionRequestMessagePayload(
            FunctionRequest::Start(start),
        ) => {
            let kit = build_function_run_kit(pod_name, namespace, start).map_err(|error| {
                QueueError::BackendError {
                    cause: error.to_string(),
                }
            })?;
            Ok(Some(kit))
        }
        OrchestratorMessagePayload::OrchestratorStageTriggerRequestMessagePayload(
            StageTriggerRequest::Start(start),
        ) => {
            let kit = build_stream_run_kit(pod_name, namespace, start).map_err(|error| {
                QueueError::BackendError {
                    cause: error.to_string(),
                }
            })?;
            Ok(Some(kit))
        }
        OrchestratorMessagePayload::OrchestratorCronRequestMessagePayload(CronRequest::Start(
            start,
        )) => {
            let kit = build_cron_run_kit(pod_name, namespace, start).map_err(|error| {
                QueueError::BackendError {
                    cause: error.to_string(),
                }
            })?;
            Ok(Some(kit))
        }
        _ => Ok(None),
    }
}

pub(super) fn derive_kube_name(worker_id: &WorkerId, prefix: &str) -> Result<String, QueueError> {
    to_fn_pod_name(prefix, worker_id).map_err(|error| {
        error!(
            id = %logical_id_text(worker_id),
            prefix = %prefix,
            error = %error,
            "Failed to derive kube name from worker and id."
        );
        QueueError::BackendError {
            cause: error.to_string(),
        }
    })
}

pub(super) fn parse_worker_id(id: &str) -> Result<WorkerId, QueueError> {
    WorkerId::try_from(id).map_err(|error| {
        error!(id = %id, error = %error, "Message id is not a worker id.");
        QueueError::BackendError {
            cause: error.to_string(),
        }
    })
}

fn derive_key(id: &str, payload: &OrchestratorMessagePayload<Value>) -> Result<Key, QueueError> {
    let (function_id, kind) = match payload {
        OrchestratorMessagePayload::OrchestratorFunctionRequestMessagePayload(
            FunctionRequest::Start(start),
        ) => (start.function_id.to_string(), WorkloadKind::Function),
        OrchestratorMessagePayload::OrchestratorFunctionRequestMessagePayload(
            FunctionRequest::Stop(stop),
        ) => (stop.function_id.to_string(), WorkloadKind::Function),
        OrchestratorMessagePayload::OrchestratorStageTriggerRequestMessagePayload(
            StageTriggerRequest::Start(start),
        ) => (start.function_id.to_string(), WorkloadKind::StageTrigger),
        OrchestratorMessagePayload::OrchestratorStageTriggerRequestMessagePayload(
            StageTriggerRequest::Stop(stop),
        ) => (stop.function_id.to_string(), WorkloadKind::StageTrigger),
        OrchestratorMessagePayload::OrchestratorCronRequestMessagePayload(CronRequest::Start(
            start,
        )) => (start.function_id.to_string(), WorkloadKind::CronTrigger),
        OrchestratorMessagePayload::OrchestratorCronRequestMessagePayload(CronRequest::Stop(
            stop,
        )) => (stop.function_id.to_string(), WorkloadKind::CronTrigger),
        _ => return Err(unexpected(id, "untracked", payload)),
    };
    Ok(Key { function_id, kind })
}

pub struct KubernetesOrchestratorMessageQueue {
    factory: Arc<WorkloadFactory>,
    reconciler: Arc<Reconciler>,
}

impl KubernetesOrchestratorMessageQueue {
    pub(crate) fn new(factory: Arc<WorkloadFactory>, reconciler: Arc<Reconciler>) -> Self {
        Self {
            factory,
            reconciler,
        }
    }

    fn locate(
        &self,
        id: &str,
        payload: &OrchestratorMessagePayload<Value>,
    ) -> Result<(PayloadRouting, WorkerId, String, Key), QueueError> {
        let worker_id = parse_worker_id(id)?;
        let routing = derive_routing(id, payload)?;
        lookup_template(self.factory.templates(), id, &routing.template_key)?;
        let name = derive_kube_name(&worker_id, &routing.prefix)?;
        let key = derive_key(id, payload)?;
        Ok((routing, worker_id, name, key))
    }
}

#[async_trait]
impl OrchestratorMessageQueue for KubernetesOrchestratorMessageQueue {
    async fn put(
        &self,
        id: String,
        payload: OrchestratorMessagePayload<Value>,
    ) -> Result<OrchestratorMessage<Value>, QueueError> {
        let (routing, worker_id, name, key) = self.locate(&id, &payload)?;
        match routing.action {
            MessageAction::Start => {
                kit_for_payload(&name, self.factory.namespace(), &payload)?;
                self.reconciler.submit(
                    key,
                    Submission {
                        worker_id,
                        name,
                        payload: payload.clone(),
                        attempts: 0,
                    },
                );
            }
            MessageAction::Stop => self.reconciler.withdraw(key, &worker_id, &name),
            MessageAction::Notify => return Err(unsupported(&id, "Notify")),
        }
        Ok(OrchestratorMessage::new(
            id.clone(),
            id,
            PathBuf::new(),
            payload,
        ))
    }

    async fn commit(&self, _message: &OrchestratorMessage<Value>) -> Result<(), QueueError> {
        Ok(())
    }

    async fn rollback(&self, message: &OrchestratorMessage<Value>) -> Result<(), QueueError> {
        let (_, worker_id, name, key) = self.locate(&message.id, &message.payload)?;
        self.reconciler.withdraw(key, &worker_id, &name);
        Ok(())
    }

    async fn locked_messages(&self) -> Vec<OrchestratorMessage<Value>> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests;
