//
// Copyright 2026 Tabsdata Inc.
//

//! Aggregated outcome of a single physical function run.
//!
//! Computed once from the worker callback at terminalization, then
//! flattened into `UpdatePhysicalFunctionRunDB` (whose columns are the
//! SQL representation). Wrapping the three values in one struct keeps
//! "no output yet" (None) distinct from "output, but data_bytes omitted
//! by an older worker yaml" (Some(RunMetrics { data_bytes: None, .. })).

use crate::types::basic::{ByteCount, RowCount, TableVersionCount};

#[ta_objects::Dto]
#[derive(Eq, PartialEq, Hash)]
pub struct RunMetrics {
    /// Distinct `table_version_id`s this attempt produced.
    pub table_versions: TableVersionCount,
    /// Total rows this attempt wrote across all produced table versions.
    pub rows: RowCount,
    /// Parquet file bytes this attempt wrote on disk, summed across
    /// all produced table versions. `None` when the worker yaml
    /// omits `data_bytes` on every written table (older protocol).
    pub data_bytes: Option<ByteCount>,
}
