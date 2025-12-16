//
// Copyright 2025 Tabs Data Inc.
//

// Uncomment if this crate ends needing to use polars extensions.
/*
#[cfg(not(feature = "without-polars-alloc"))]
use pyo3_polars::PolarsAllocator;
 */
use pyo3_stub_gen::define_stub_info_gatherer;

pub mod foundation;

define_stub_info_gatherer!(stub_info);
