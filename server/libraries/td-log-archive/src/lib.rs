//
// Copyright 2026 Tabsdata Inc.
//

//! Reads a worker's archived logs: what the cluster's cloud log store kept
//! after the pod was removed. Live pod logs are read from the Kubernetes API by
//! `td-kubernetes`, which falls back to this crate once the pod is gone.
//!
//! Every store is an [`ArchiveReader`]; [`LogArchive`] picks the one for the
//! cluster the apiserver runs in, and is all a caller needs.

mod archive;
mod cloudwatch;
mod render;

pub use archive::{ArchiveReader, ArchiveRequest, LogArchive};
pub use render::container_header;
