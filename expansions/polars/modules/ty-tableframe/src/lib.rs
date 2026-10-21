//
// Copyright 2025 Tabsdata Inc.
//

#[cfg(not(feature = "without-polars-alloc"))]
use pyo3_polars::PolarsAllocator;
use pyo3_stub_gen::StubInfo;
use std::path::Path;

pub mod tableframe;

#[cfg(not(feature = "without-polars-alloc"))]
#[global_allocator]
static ALLOC: PolarsAllocator = PolarsAllocator::new();

pub fn stub_info() -> pyo3_stub_gen::Result<StubInfo> {
    let manifest_dir: &Path = env!("CARGO_MANIFEST_DIR").as_ref();
    StubInfo::from_pyproject_toml(manifest_dir.join("pyproject.toml"))
}
