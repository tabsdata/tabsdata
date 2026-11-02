//
// Copyright 2026 Tabsdata Inc.
//

//! Where a worker's logs are read from once its pod is gone: the cloud's own
//! log store, picked from what the apiserver pod runs as.

use std::sync::Arc;

use td_bridge::worker::WorkerLogStream;

use crate::cloudwatch::CloudWatchLogReader;

/// What a reader needs to find one worker's archived logs.
#[derive(Clone, Copy, Debug)]
pub struct ArchiveRequest<'a> {
    pub worker_id: &'a str,
    /// The `tabsdata.com/log-id` label the worker's pods carry: the name of
    /// its workload.
    pub log_id: &'a str,
}

pub trait ArchiveReader: Send + Sync {
    fn stream(&self, request: ArchiveRequest<'_>) -> WorkerLogStream;
}

#[derive(Clone)]
pub struct LogArchive {
    reader: Arc<dyn ArchiveReader>,
}

impl LogArchive {
    pub fn new(reader: impl ArchiveReader + 'static) -> Self {
        Self {
            reader: Arc::new(reader),
        }
    }

    pub async fn from_environment(namespace: &str) -> Option<Self> {
        if CloudWatchLogReader::assumes_role() {
            return CloudWatchLogReader::from_environment(namespace)
                .await
                .map(Self::new);
        }
        None
    }

    pub fn stream(&self, request: ArchiveRequest<'_>) -> WorkerLogStream {
        self.reader.stream(request)
    }
}
