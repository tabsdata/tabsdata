//
// Copyright 2025 Tabs Data Inc.
//

#[cfg(not(feature = "without-polars-alloc"))]
use pyo3_polars::PolarsAllocator;
use pyo3_stub_gen::define_stub_info_gatherer;

pub mod tableframe;

#[cfg(not(feature = "without-polars-alloc"))]
#[global_allocator]
static ALLOC: PolarsAllocator = PolarsAllocator::new();

define_stub_info_gatherer!(stub_info);
