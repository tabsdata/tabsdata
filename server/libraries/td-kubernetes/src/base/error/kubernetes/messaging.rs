//
// Copyright 2026 Tabs Data Inc.
//

use td_error::td_error;

#[td_error]
pub enum KubeMessagingError {
    #[error("No pod template found for worker '{worker}'")]
    TemplateNotFound { worker: String } = 1000,
    #[error("Start message for worker '{worker}' run '{runid}' is missing the responder callback")]
    StartMissingCallback { worker: String, runid: String } = 1001,
    #[error("Pod template for worker '{worker}' has unsupported kind '{kind}'")]
    UnsupportedTemplateKind { worker: String, kind: String } = 5000,
}
