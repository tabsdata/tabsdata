//
// Copyright 2025 Tabs Data Inc.
//

use pyo3_stub_gen::Result;

fn main() -> Result<()> {
    let stub = ty_tableframe::stub_info()?;
    stub.generate()?;
    Ok(())
}
