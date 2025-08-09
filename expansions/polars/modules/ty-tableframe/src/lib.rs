//
// Copyright 2025 Tabs Data Inc.
//

use pyo3_polars::PolarsAllocator;

pub mod tableframe;

#[global_allocator]
static ALLOC: PolarsAllocator = PolarsAllocator::new();
