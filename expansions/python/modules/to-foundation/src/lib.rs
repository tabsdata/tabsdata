//
// Copyright 2025 Tabsdata Inc.
//

// Uncomment if this crate ends needing to use polars extensions.
/*
#[cfg(not(feature = "without-polars-alloc"))]
use pyo3_polars::PolarsAllocator;
 */
use pyo3_stub_gen::StubInfo;
use std::path::Path;

pub mod foundation;

pub fn stub_info() -> pyo3_stub_gen::Result<StubInfo> {
    let manifest_dir: &Path = env!("CARGO_MANIFEST_DIR").as_ref();
    let mut stub_info = StubInfo::from_pyproject_toml(manifest_dir.join("pyproject.toml"))?;
    let root = stub_info.default_module_name.clone();
    let prefix = format!("{root}.");
    stub_info
        .modules
        .retain(|name, _| name == &root || name.starts_with(&prefix));
    Ok(stub_info)
}
