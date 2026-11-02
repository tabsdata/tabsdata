//
// Copyright 2026 Tabsdata Inc.
//

//! Reads a worker's archived logs: what the cluster's log store kept
//! after the pod was removed. Live pod logs are read from the Kubernetes API by
//! `td-kubernetes`, which falls back to this crate once the pod is gone.
//!
//! Every store is an [`ArchiveReader`]; [`LogArchive`] picks the one for the
//! cluster the apiserver runs in, and is all a caller needs.

mod archive;
mod cloudlogging;
mod cloudwatch;
mod env;
mod kminus;
mod loganalytics;
mod render;

pub use archive::{ArchiveReader, ArchiveRequest, LogArchive};
pub use env::{
    ENV_TD_WORKER_LOGS_BUCKET, ENV_TD_WORKER_LOGS_LOCATION, ENV_TD_WORKER_LOGS_WORKSPACE,
};
pub use render::container_header;
